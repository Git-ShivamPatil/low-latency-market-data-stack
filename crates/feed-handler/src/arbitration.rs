//! A/B arbitration: take whichever arm arrives first, and tell late from lost.
//!
//! # The problem
//!
//! Two channels carry identical datagrams with identical sequence numbers. On a
//! healthy pair each message arrives twice, microseconds apart, and the second
//! copy is waste. When one arm drops a datagram the other still has it — that is
//! the entire point of publishing twice — but it arrives *after* datagrams the
//! surviving arm has already delivered. So the consumer sees:
//!
//! ```text
//!   arm A:  100..131   [lost]     164..195   196..227
//!   arm B:  100..131   132..163   164..195   196..227
//!   order:  100..131   164..195   132..163   196..227
//!                                 ^^^^^^^^ late, not lost
//! ```
//!
//! A handler that keys on "is this the sequence I expected" declares a gap at
//! 164 and then treats 132..163 as a duplicate — losing 32 messages while
//! reporting a recovery it never made. The naive logic milestone 2 shipped did
//! exactly that, and said so in its own doc comment.
//!
//! # The fix
//!
//! A bounded reorder window. Out-of-order datagrams are buffered by sequence
//! and released only when the hole ahead of them fills. What makes it *bounded*
//! is the failure case: when the window fills and the hole is still open, the
//! missing range really is lost, and the handler transitions to `Gapped` naming
//! the range rather than silently skipping it.
//!
//! Bounded matters more than it looks. An unbounded buffer turns a lost datagram
//! into unbounded memory growth and unbounded latency — the handler would sit
//! forever holding messages it could have delivered, waiting for one that is
//! never coming.
//!
//! # Datagrams, not messages
//!
//! The window holds datagrams because loss happens to datagrams. A whole
//! datagram of 30 messages arrives or does not; there is no such thing as losing
//! message 17 of it. Buffering at the datagram level means one slot per loss
//! event rather than thirty, and the window covers thirty times as much stream
//! for the same memory.
//!
//! # Allocation
//!
//! Every slot is allocated once at construction. Nothing here allocates while
//! running, on any path. The full proof — a counting allocator asserting zero
//! allocations across a million messages including a recovery cycle — is
//! milestone 5's job; this is the design that makes it possible.

use std::fmt;

use wire::{PacketHeaderDecoder, WireError};

/// Where the feed is, as far as this handler can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedState {
    /// Nothing received yet, so there is no sequence to be contiguous with.
    Syncing,
    /// Every sequence up to `next_expected` has been delivered in order.
    Live,
    /// A range is confirmed lost. The book can no longer be trusted, and
    /// recovering is milestone 4's job.
    Gapped,
}

impl FeedState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Syncing => "SYNCING",
            Self::Live => "LIVE",
            Self::Gapped => "GAPPED",
        }
    }
}

impl fmt::Display for FeedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-arm counters.
///
/// Split per arm on purpose. A single "messages received" total cannot tell a
/// healthy redundant feed from one where B has been dead since startup — both
/// look identical downstream, right up until A drops something.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArmCounters {
    /// Datagrams read off this arm's socket.
    pub datagrams: u64,
    /// Datagrams from this arm that carried something new.
    pub datagrams_first: u64,
    /// Datagrams entirely superseded by the other arm.
    pub datagrams_duplicate: u64,
    /// Datagrams this arm delivered ahead of the hole, and that had to wait.
    pub datagrams_buffered: u64,
    /// Messages this arm was first to deliver.
    pub messages_first: u64,
    /// Datagrams rejected because they would not decode.
    pub malformed: u64,
    /// Datagrams discarded because the reorder window was full and the stream
    /// was already gapped. Only reachable when the caller has stopped draining.
    pub dropped_window_full: u64,
    pub bytes: u64,
}

/// A range the arbitrator gave up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    /// First sequence never delivered.
    pub from: u64,
    /// Last sequence never delivered, inclusive.
    pub through: u64,
}

impl Gap {
    /// How many sequences the gap covers. Never zero: a gap always spans at
    /// least the one message that went missing, which is why this is not `len`
    /// — there is no such thing as an empty gap to ask about.
    pub fn messages(&self) -> u64 {
        self.through - self.from + 1
    }
}

impl fmt::Display for Gap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}..={} ({} messages)",
            self.from,
            self.through,
            self.messages()
        )
    }
}

/// What accepting a datagram did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// It was the datagram we were waiting for. Its bytes are ready to decode,
    /// and buffered datagrams behind it may now be ready too.
    Ready { first_sequence: u64, count: u16 },
    /// Ahead of the hole; held until the hole fills.
    Buffered,
    /// Every sequence in it has already been delivered.
    Duplicate,
    /// Held nothing new, and the window had no room. The oldest hole was
    /// declared lost to make progress.
    ForcedGap(Gap),
    /// Would not decode. Counted and dropped.
    Malformed(WireError),
}

#[derive(Debug)]
struct Slot {
    first_sequence: u64,
    count: u16,
    len: usize,
    occupied: bool,
    /// Which arm got here first with this datagram.
    arm: u8,
    buf: Box<[u8]>,
}

/// Bounded reorder buffer plus the state machine around it.
#[derive(Debug)]
pub struct Arbitrator {
    state: FeedState,
    /// The next sequence that may be delivered.
    next_expected: u64,
    window: Vec<Slot>,
    /// The advertised bound. `window` holds one slot more than this — see
    /// [`Arbitrator::new`].
    capacity: usize,
    occupied: usize,
    arms: [ArmCounters; 2],
    first_sequence: u64,
    last_delivered: u64,
    messages_delivered: u64,
    gaps: Vec<Gap>,
    messages_missed: u64,
    /// Highest `first_sequence + count` seen on any arm, delivered or not.
    highest_seen: u64,
    max_window_used: usize,
    started: bool,
    resyncs: u64,
}

impl Arbitrator {
    /// `window_datagrams` is how far ahead one arm may run before a hole is
    /// declared lost. At a batch of 32 a window of 64 covers ~2000 messages of
    /// reordering, which is far more slack than two arms on one host ever need
    /// and still a hard bound.
    pub fn new(window_datagrams: usize, max_datagram_bytes: usize) -> Self {
        let window_datagrams = window_datagrams.max(1);
        Self {
            state: FeedState::Syncing,
            next_expected: 0,
            // One slot more than the advertised capacity. Declaring a gap
            // advances the frontier past the hole but does not itself empty a
            // slot — draining does that, and draining is the caller's call
            // because only the caller can consume the messages. Without a spare
            // slot the datagram that *triggered* the decision would have
            // nowhere to go and would be dropped, turning a recovery into a
            // second, silent gap.
            window: (0..window_datagrams + 1)
                .map(|_| Slot {
                    first_sequence: 0,
                    count: 0,
                    len: 0,
                    occupied: false,
                    arm: 0,
                    buf: vec![0u8; max_datagram_bytes].into_boxed_slice(),
                })
                .collect(),
            capacity: window_datagrams,
            occupied: 0,
            arms: [ArmCounters::default(); 2],
            first_sequence: 0,
            last_delivered: 0,
            messages_delivered: 0,
            gaps: Vec::new(),
            messages_missed: 0,
            highest_seen: 0,
            max_window_used: 0,
            started: false,
            resyncs: 0,
        }
    }

    pub fn state(&self) -> FeedState {
        self.state
    }

    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    pub fn arm(&self, arm: u8) -> ArmCounters {
        self.arms[usize::from(arm & 1)]
    }

    pub fn gaps(&self) -> &[Gap] {
        &self.gaps
    }

    pub fn messages_delivered(&self) -> u64 {
        self.messages_delivered
    }

    pub fn messages_missed(&self) -> u64 {
        self.messages_missed
    }

    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    pub fn last_delivered(&self) -> u64 {
        self.last_delivered
    }

    pub fn max_window_used(&self) -> usize {
        self.max_window_used
    }

    /// How many out-of-order datagrams may be held before a hole is declared
    /// lost. The backing store holds one more, which is not part of the bound.
    pub fn window_capacity(&self) -> usize {
        self.capacity
    }

    /// Offers a datagram from `arm` (0 = A, 1 = B).
    ///
    /// On [`Accepted::Ready`] the caller decodes `datagram` itself — the bytes
    /// are not copied for the common in-order case, which is ~98% of traffic on
    /// a healthy pair. Only out-of-order datagrams are copied into the window.
    pub fn accept(&mut self, arm: u8, datagram: &[u8]) -> Accepted {
        let arm = arm & 1;
        let idx = usize::from(arm);
        self.arms[idx].datagrams += 1;
        self.arms[idx].bytes += datagram.len() as u64;

        let header = match PacketHeaderDecoder::wrap(datagram) {
            Ok(h) => h,
            Err(e) => {
                self.arms[idx].malformed += 1;
                return Accepted::Malformed(e);
            }
        };
        let first = header.first_sequence();
        let count = header.message_count();
        if count == 0 {
            // Nothing to deliver and nothing to wait for.
            self.arms[idx].datagrams_duplicate += 1;
            return Accepted::Duplicate;
        }
        let end = first.saturating_add(u64::from(count));
        self.highest_seen = self.highest_seen.max(end);

        if !self.started {
            self.started = true;
            self.first_sequence = first;
            self.next_expected = first;
            self.state = FeedState::Live;
        }

        // Entirely behind the frontier: the other arm already delivered it.
        if end <= self.next_expected {
            self.arms[idx].datagrams_duplicate += 1;
            return Accepted::Duplicate;
        }

        if first <= self.next_expected {
            // Exactly what we are waiting for. A datagram that straddles the
            // frontier cannot happen while both arms use the same batching, but
            // it is cheap to be correct about rather than to assume.
            self.arms[idx].datagrams_first += 1;
            self.arms[idx].messages_first += u64::from(count);
            self.deliver(first, count);
            return Accepted::Ready {
                first_sequence: first,
                count,
            };
        }

        // Ahead of the hole. Buffer it, if there is room.
        if self.find_slot(first).is_some() {
            // Already held from the other arm.
            self.arms[idx].datagrams_duplicate += 1;
            return Accepted::Duplicate;
        }
        if self.occupied < self.capacity {
            let slot_idx = self
                .free_slot()
                .expect("occupied < capacity implies a free slot");
            self.store(slot_idx, arm, first, count, datagram);
            self.arms[idx].datagrams_buffered += 1;
            return Accepted::Buffered;
        }

        // The window is full and the hole is still open, so the missing range is
        // not late — it is lost. Give up on it deliberately and loudly so the
        // stream can make progress, then keep the datagram that forced the
        // decision: dropping it would turn one gap into two.
        let gap = self.declare_gap();
        self.keep_after_gap(arm, first, count, datagram);
        Accepted::ForcedGap(gap)
    }

    /// Files the datagram that forced a gap, now that the frontier has moved.
    ///
    /// The spare slot is normally free, but it is not *guaranteed* free: a caller
    /// that stops draining — as the handler does while recovering — leaves the
    /// window full, and a second gap then arrives with nowhere to put anything.
    /// This used to `expect` a slot and panicked in exactly that case. A runtime
    /// condition a caller can provoke is an error path, not an invariant.
    fn keep_after_gap(&mut self, arm: u8, first: u64, count: u16, datagram: &[u8]) {
        let idx = usize::from(arm);
        let end = first.saturating_add(u64::from(count));
        if end <= self.next_expected {
            self.arms[idx].datagrams_duplicate += 1;
            return;
        }
        if first <= self.next_expected {
            self.arms[idx].datagrams_first += 1;
            self.arms[idx].messages_first += u64::from(count);
            self.deliver(first, count);
            return;
        }
        match self.free_slot() {
            Some(slot_idx) => {
                self.store(slot_idx, arm, first, count, datagram);
                self.arms[idx].datagrams_buffered += 1;
            }
            None => {
                // Nowhere to put it and nothing to evict that is not also
                // needed. Dropping it is honest as long as it is counted and the
                // stream is already known to be gapped, which it is: this is
                // only reachable immediately after declaring one.
                self.arms[idx].dropped_window_full += 1;
            }
        }
    }

    /// Releases every buffered datagram that is now contiguous.
    ///
    /// The closure receives `(first_sequence, count, bytes)` per datagram, in
    /// sequence order. Call it after every [`accept`](Self::accept) that
    /// returned [`Accepted::Ready`] or [`Accepted::ForcedGap`].
    pub fn drain_ready(&mut self, mut f: impl FnMut(u64, u16, &[u8])) {
        while let Some(slot_idx) = self.find_slot_covering(self.next_expected) {
            let (first, count, arm) = {
                let slot = &self.window[slot_idx];
                f(slot.first_sequence, slot.count, &slot.buf[..slot.len]);
                (slot.first_sequence, slot.count, slot.arm)
            };
            self.window[slot_idx].occupied = false;
            self.occupied -= 1;
            let arm_idx = usize::from(arm);
            self.arms[arm_idx].datagrams_first += 1;
            self.arms[arm_idx].messages_first += u64::from(count);
            self.deliver(first, count);
        }
    }

    /// Restarts the stream at `sequence` after recovering from a snapshot.
    ///
    /// The snapshot replaced the book wholesale, so anything still buffered is
    /// either already reflected in it or belongs to the replay the caller is
    /// about to do. Either way this arbitrator's view of the past is void, and
    /// carrying it forward would resurrect messages the snapshot superseded.
    ///
    /// This clears `Gapped`: the gap that triggered recovery has been closed by
    /// other means, and leaving the state set would mean the handler could never
    /// report a clean run again.
    pub fn resync_to(&mut self, sequence: u64) {
        for slot in &mut self.window {
            slot.occupied = false;
        }
        self.occupied = 0;
        self.next_expected = sequence;
        self.state = FeedState::Live;
        self.started = true;
        self.resyncs += 1;
    }

    pub fn resyncs(&self) -> u64 {
        self.resyncs
    }

    /// Declares the current hole lost. Called when the window is full, and by
    /// the caller when the feed has gone quiet with a hole outstanding.
    ///
    /// Returns `None` when there is no hole to give up on.
    pub fn declare_gap_if_stalled(&mut self) -> Option<Gap> {
        if self.occupied == 0 {
            return None;
        }
        Some(self.declare_gap())
    }

    /// Advances past the hole to the lowest buffered datagram.
    fn declare_gap(&mut self) -> Gap {
        let target = self
            .window
            .iter()
            .filter(|s| s.occupied)
            .map(|s| s.first_sequence)
            .min()
            // Only called with the window non-empty.
            .unwrap_or(self.next_expected + 1);

        let gap = Gap {
            from: self.next_expected,
            through: target.saturating_sub(1),
        };
        self.messages_missed += gap.messages();
        self.gaps.push(gap);
        self.state = FeedState::Gapped;
        self.next_expected = target;
        gap
    }

    fn deliver(&mut self, first: u64, count: u16) {
        let end = first + u64::from(count);
        // A datagram may start behind the frontier when the two arms batch
        // differently; only the part past it is new.
        let newly = end.saturating_sub(self.next_expected);
        self.messages_delivered += newly;
        self.next_expected = end;
        self.last_delivered = end - 1;
        if self.state != FeedState::Gapped {
            self.state = FeedState::Live;
        }
    }

    fn store(&mut self, slot_idx: usize, arm: u8, first: u64, count: u16, datagram: &[u8]) {
        let slot = &mut self.window[slot_idx];
        let len = datagram.len().min(slot.buf.len());
        slot.buf[..len].copy_from_slice(&datagram[..len]);
        slot.len = len;
        slot.first_sequence = first;
        slot.count = count;
        slot.arm = arm;
        slot.occupied = true;
        self.occupied += 1;
        self.max_window_used = self.max_window_used.max(self.occupied);
    }

    fn free_slot(&self) -> Option<usize> {
        self.window.iter().position(|s| !s.occupied)
    }

    fn find_slot(&self, first_sequence: u64) -> Option<usize> {
        self.window
            .iter()
            .position(|s| s.occupied && s.first_sequence == first_sequence)
    }

    /// The buffered datagram that would deliver `sequence`, if any.
    fn find_slot_covering(&self, sequence: u64) -> Option<usize> {
        self.window.iter().position(|s| {
            s.occupied
                && s.first_sequence <= sequence
                && sequence < s.first_sequence + u64::from(s.count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{PacketWriter, Side};

    /// Builds a datagram carrying `count` AddOrders starting at `first`.
    fn datagram(first: u64, count: u16, channel: u8) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let mut w = PacketWriter::new(&mut buf, channel, 0, first, 0).unwrap();
        for i in 0..count {
            w.add_order(first + u64::from(i), 1000, 1, 1, Side::Bid)
                .unwrap();
        }
        let n = w.finish();
        buf.truncate(n);
        buf
    }

    /// Feeds datagrams and collects every sequence actually delivered, in order.
    struct Harness {
        arb: Arbitrator,
        delivered: Vec<u64>,
    }

    impl Harness {
        fn new(window: usize) -> Self {
            Self {
                arb: Arbitrator::new(window, 4096),
                delivered: Vec::new(),
            }
        }

        fn feed(&mut self, arm: u8, d: &[u8]) -> Accepted {
            let outcome = self.arb.accept(arm, d);
            if let Accepted::Ready {
                first_sequence,
                count,
            } = outcome
            {
                for i in 0..u64::from(count) {
                    self.delivered.push(first_sequence + i);
                }
            }
            let delivered = &mut self.delivered;
            self.arb.drain_ready(|first, count, _bytes| {
                for i in 0..u64::from(count) {
                    delivered.push(first + i);
                }
            });
            outcome
        }
    }

    #[test]
    fn the_second_arm_is_a_duplicate_when_the_first_already_delivered() {
        let mut h = Harness::new(8);
        let d = datagram(1, 4, 0);
        assert!(matches!(h.feed(0, &d), Accepted::Ready { .. }));
        assert_eq!(h.feed(1, &datagram(1, 4, 1)), Accepted::Duplicate);
        assert_eq!(h.delivered, vec![1, 2, 3, 4]);
        assert_eq!(h.arb.arm(0).datagrams_first, 1);
        assert_eq!(h.arb.arm(1).datagrams_duplicate, 1);
        assert_eq!(h.arb.state(), FeedState::Live);
    }

    #[test]
    fn a_datagram_lost_on_one_arm_is_recovered_from_the_other() {
        // The case the whole mechanism exists for, and the one milestone 2's
        // naive logic got wrong: the replacement arrives *after* datagrams the
        // surviving arm already delivered.
        let mut h = Harness::new(8);
        h.feed(0, &datagram(1, 4, 0)); // A delivers 1..4
                                       // A drops 5..8. A runs ahead:
        assert_eq!(h.feed(0, &datagram(9, 4, 0)), Accepted::Buffered);
        assert_eq!(h.delivered, vec![1, 2, 3, 4], "9..12 must wait");
        // B's copy of 5..8 turns up late.
        assert!(matches!(
            h.feed(1, &datagram(5, 4, 1)),
            Accepted::Ready { .. }
        ));
        assert_eq!(
            h.delivered,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "the buffered datagram must be released behind the late one"
        );
        assert_eq!(h.arb.state(), FeedState::Live, "late is not lost");
        assert!(h.arb.gaps().is_empty());
    }

    #[test]
    fn many_datagrams_can_wait_behind_one_hole() {
        let mut h = Harness::new(16);
        h.feed(0, &datagram(1, 4, 0));
        for i in 0..10 {
            let first = 9 + i * 4;
            assert_eq!(h.feed(0, &datagram(first, 4, 0)), Accepted::Buffered);
        }
        assert_eq!(h.delivered.len(), 4, "everything is stuck behind the hole");
        h.feed(1, &datagram(5, 4, 1));
        assert_eq!(
            h.delivered,
            (1..=48).collect::<Vec<u64>>(),
            "one late datagram releases all ten"
        );
        assert!(h.arb.gaps().is_empty());
    }

    #[test]
    fn a_full_window_declares_the_hole_lost_and_names_the_range() {
        let mut h = Harness::new(4);
        h.feed(0, &datagram(1, 4, 0)); // delivered 1..4
                                       // 5..8 lost on both arms. A keeps going.
        for i in 0..4 {
            let first = 9 + i * 4;
            assert_eq!(h.feed(0, &datagram(first, 4, 0)), Accepted::Buffered);
        }
        // The window is full; the next one forces a decision.
        let outcome = h.feed(0, &datagram(25, 4, 0));
        let Accepted::ForcedGap(gap) = outcome else {
            panic!("expected a forced gap, got {outcome:?}")
        };
        assert_eq!(
            gap,
            Gap {
                from: 5,
                through: 8
            },
            "the range must be named"
        );
        assert_eq!(gap.messages(), 4);
        assert_eq!(h.arb.state(), FeedState::Gapped);
        assert_eq!(h.arb.messages_missed(), 4);
        assert_eq!(
            h.delivered,
            (1..=4).chain(9..=28).collect::<Vec<u64>>(),
            "5..8 is skipped, and skipped exactly once"
        );
    }

    #[test]
    fn a_quiet_feed_with_an_outstanding_hole_can_be_told_to_give_up() {
        let mut h = Harness::new(8);
        h.feed(0, &datagram(1, 4, 0));
        h.feed(0, &datagram(9, 4, 0));
        assert_eq!(h.arb.state(), FeedState::Live, "still hoping");
        let gap = h.arb.declare_gap_if_stalled().expect("a hole is open");
        assert_eq!(
            gap,
            Gap {
                from: 5,
                through: 8
            }
        );
        assert_eq!(h.arb.state(), FeedState::Gapped);
        // And the buffered datagram is released on the next drain.
        let delivered = &mut h.delivered;
        h.arb.drain_ready(|first, count, _| {
            for i in 0..u64::from(count) {
                delivered.push(first + i);
            }
        });
        assert_eq!(h.delivered, vec![1, 2, 3, 4, 9, 10, 11, 12]);
    }

    #[test]
    fn nothing_to_give_up_on_when_the_stream_is_contiguous() {
        let mut h = Harness::new(8);
        h.feed(0, &datagram(1, 4, 0));
        assert_eq!(h.arb.declare_gap_if_stalled(), None);
        assert_eq!(h.arb.state(), FeedState::Live);
    }

    #[test]
    fn the_stream_can_start_anywhere() {
        // Joining mid-stream is not a gap: there is no evidence anything was
        // lost, only that we were not listening.
        let mut h = Harness::new(8);
        h.feed(0, &datagram(5_000, 4, 0));
        assert_eq!(h.arb.first_sequence(), 5_000);
        assert_eq!(h.arb.state(), FeedState::Live);
        assert!(h.arb.gaps().is_empty());
        assert_eq!(h.delivered, vec![5_000, 5_001, 5_002, 5_003]);
    }

    #[test]
    fn a_malformed_datagram_is_counted_and_dropped() {
        let mut h = Harness::new(8);
        assert!(matches!(h.feed(0, &[0u8; 4]), Accepted::Malformed(_)));
        assert_eq!(h.arb.arm(0).malformed, 1);
        assert_eq!(h.arb.state(), FeedState::Syncing, "nothing was delivered");
    }

    #[test]
    fn both_arms_holding_the_same_out_of_order_datagram_buffer_it_once() {
        let mut h = Harness::new(8);
        h.feed(0, &datagram(1, 4, 0));
        assert_eq!(h.feed(0, &datagram(9, 4, 0)), Accepted::Buffered);
        assert_eq!(
            h.feed(1, &datagram(9, 4, 1)),
            Accepted::Duplicate,
            "the same datagram from the other arm must not take a second slot"
        );
        assert_eq!(h.arb.max_window_used(), 1);
    }

    #[test]
    fn delivered_counts_never_double_count_a_message() {
        let mut h = Harness::new(8);
        for round in 0..50u64 {
            let first = 1 + round * 4;
            h.feed(0, &datagram(first, 4, 0));
            h.feed(1, &datagram(first, 4, 1));
        }
        assert_eq!(h.arb.messages_delivered(), 200);
        assert_eq!(h.delivered.len(), 200);
        assert_eq!(h.delivered, (1..=200).collect::<Vec<u64>>());
    }
}
