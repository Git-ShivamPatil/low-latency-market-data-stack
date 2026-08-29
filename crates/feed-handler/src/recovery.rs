//! Getting back to a book you can trust after losing messages.
//!
//! # The problem a snapshot solves and the one it does not
//!
//! When a range is lost on both arms the handler's book is wrong from that point
//! on, and no amount of further live traffic fixes it: a `DeleteOrder` for an
//! order the handler never saw added does not restore the ones it missed.
//!
//! The snapshot cycle publishes the whole book every couple of seconds, tagged
//! with the incremental sequence it reflects. So recovery is: throw away the
//! book, adopt the snapshot, and resume the live stream from the sequence the
//! snapshot claims.
//!
//! The part that is easy to get wrong is what happens to live traffic *during*
//! recovery. A snapshot consistent as of sequence `S` arrives at wall-clock time
//! when the live stream is already at `S + k`. Those `k` messages are not in the
//! snapshot and are not coming again. Dropping them puts the book quietly wrong
//! in a new way; applying them without checking would apply some twice.
//!
//! So live traffic is **buffered** from the moment recovery starts, and once the
//! snapshot is adopted the buffer is replayed from `lastSequence + 1`, skipping
//! anything the snapshot already covers. That reconciliation is the whole
//! mechanism, and it is what `handles_a_gap_then_reconciles_buffered_traffic`
//! pins down.
//!
//! # Bounded
//!
//! The buffer is bounded and recovery is timed. If the buffer fills or the
//! deadline passes, recovery has failed and the handler says so rather than
//! quietly holding traffic forever. A recovery path with no failure mode is a
//! stall waiting to happen.

use std::time::{Duration, Instant};

/// Where recovery is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Not recovering. The live stream is authoritative.
    Idle,
    /// A gap was declared. Live traffic is being buffered while we wait for a
    /// snapshot cycle that starts at or after the gap.
    AwaitingSnapshot {
        /// The first sequence known to be missing.
        gap_from: u64,
    },
    /// A snapshot has been adopted and the buffered live traffic is being
    /// replayed on top of it.
    Reconciling { from_sequence: u64 },
}

/// How a recovery ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveredBy {
    /// The replay service served the exact missing range. The book was never
    /// discarded and the messages themselves were recovered, not just the state
    /// they produced.
    Replay,
    /// A snapshot cycle replaced the books wholesale. Cheaper on the publisher
    /// and always available, but the messages in the gap are gone for good.
    Snapshot,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryStats {
    pub attempts: u64,
    pub completed: u64,
    pub failed: u64,
    /// Datagrams held while waiting for a snapshot, summed across attempts.
    pub datagrams_buffered: u64,
    /// Messages the snapshot already covered, so they were not replayed.
    pub messages_skipped: u64,
    /// Messages replayed from the buffer on top of the snapshot.
    pub messages_replayed: u64,
    pub snapshots_seen: u64,
    /// Recoveries closed by the replay service.
    pub by_replay: u64,
    /// Recoveries closed by a snapshot cycle.
    pub by_snapshot: u64,
    /// Replay requests that came back unusable, so the snapshot path was used.
    pub replay_refused: u64,
    /// Messages the replay service handed back.
    pub replay_messages: u64,
    /// Snapshot fragments discarded because their cycle was already stale.
    pub snapshots_discarded: u64,
    pub last_recovery_millis: u64,
    pub worst_recovery_millis: u64,
}

/// One datagram held during recovery.
#[derive(Debug)]
struct Held {
    first_sequence: u64,
    count: u16,
    len: usize,
    buf: Box<[u8]>,
}

/// Buffers live traffic across a recovery and replays what the snapshot missed.
#[derive(Debug)]
pub struct RecoveryBuffer {
    state: Recovery,
    held: Vec<Held>,
    occupied: usize,
    capacity: usize,
    started: Option<Instant>,
    deadline: Duration,
    stats: RecoveryStats,
}

/// Why a recovery attempt ended without a trustworthy book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFailure {
    /// More live traffic arrived than the buffer can hold.
    BufferFull { capacity: usize },
    /// No usable snapshot arrived in time.
    TimedOut { after: Duration },
}

impl std::fmt::Display for RecoveryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BufferFull { capacity } => write!(
                f,
                "the recovery buffer filled at {capacity} datagrams before a snapshot arrived"
            ),
            Self::TimedOut { after } => write!(
                f,
                "no usable snapshot arrived within {}ms",
                after.as_millis()
            ),
        }
    }
}

impl RecoveryBuffer {
    pub fn new(capacity: usize, max_datagram_bytes: usize, deadline: Duration) -> Self {
        let capacity = capacity.max(1);
        Self {
            state: Recovery::Idle,
            held: (0..capacity)
                .map(|_| Held {
                    first_sequence: 0,
                    count: 0,
                    len: 0,
                    buf: vec![0u8; max_datagram_bytes].into_boxed_slice(),
                })
                .collect(),
            occupied: 0,
            capacity,
            started: None,
            deadline,
            stats: RecoveryStats::default(),
        }
    }

    pub fn state(&self) -> Recovery {
        self.state
    }

    pub fn stats(&self) -> RecoveryStats {
        self.stats
    }

    pub fn is_recovering(&self) -> bool {
        !matches!(self.state, Recovery::Idle)
    }

    /// Begins recovery. Live traffic from here is held until a snapshot lands.
    pub fn begin(&mut self, gap_from: u64, now: Instant) {
        if self.is_recovering() {
            // Already recovering; a second gap inside the first does not restart
            // the clock, or a stream losing messages steadily would never time
            // out and never report failure.
            return;
        }
        self.state = Recovery::AwaitingSnapshot { gap_from };
        self.started = Some(now);
        self.occupied = 0;
        self.stats.attempts += 1;
    }

    /// Holds one live datagram. Returns an error if the buffer is exhausted.
    pub fn hold(
        &mut self,
        first_sequence: u64,
        count: u16,
        datagram: &[u8],
    ) -> Result<(), RecoveryFailure> {
        if self.occupied == self.capacity {
            return Err(RecoveryFailure::BufferFull {
                capacity: self.capacity,
            });
        }
        let slot = &mut self.held[self.occupied];
        let len = datagram.len().min(slot.buf.len());
        slot.buf[..len].copy_from_slice(&datagram[..len]);
        slot.len = len;
        slot.first_sequence = first_sequence;
        slot.count = count;
        self.occupied += 1;
        self.stats.datagrams_buffered += 1;
        Ok(())
    }

    /// Checks whether recovery has run out of time.
    pub fn check_deadline(&mut self, now: Instant) -> Result<(), RecoveryFailure> {
        if let Some(started) = self.started {
            if self.is_recovering() && now.duration_since(started) >= self.deadline {
                return Err(RecoveryFailure::TimedOut {
                    after: self.deadline,
                });
            }
        }
        Ok(())
    }

    /// Accepts a snapshot that is consistent as of `last_sequence`.
    ///
    /// Returns `false` when the snapshot is too old to close the gap — it
    /// reflects a point *before* the messages we are missing, so adopting it
    /// would throw away good state and still leave the hole.
    pub fn snapshot_is_usable(&self, last_sequence: u64) -> bool {
        match self.state {
            Recovery::AwaitingSnapshot { gap_from } => last_sequence + 1 >= gap_from,
            _ => false,
        }
    }

    /// Moves to reconciliation after a snapshot has been adopted.
    pub fn adopt_snapshot(&mut self, last_sequence: u64) {
        self.stats.snapshots_seen += 1;
        self.stats.by_snapshot += 1;
        self.state = Recovery::Reconciling {
            from_sequence: last_sequence + 1,
        };
    }

    /// The range this recovery is waiting to have filled, if any.
    pub fn gap_from(&self) -> Option<u64> {
        match self.state {
            Recovery::AwaitingSnapshot { gap_from } => Some(gap_from),
            _ => None,
        }
    }

    /// Moves to reconciliation after the replay service filled the hole exactly.
    ///
    /// Unlike a snapshot, replay does not replace the book — it supplies the
    /// missing messages, which the caller applies before this is called. So the
    /// held traffic is replayed in full from `through + 1`: none of it was
    /// covered by anything.
    ///
    /// Returns `false` when this recovery is no longer outstanding. A replay
    /// request runs on its own thread, so its answer can arrive *after* a
    /// snapshot has already closed the same gap — and applying it then would
    /// re-apply messages the book already has. The caller must not proceed on a
    /// `false`.
    #[must_use]
    pub fn adopt_replay(&mut self, through: u64, messages: u64) -> bool {
        if !matches!(self.state, Recovery::AwaitingSnapshot { .. }) {
            return false;
        }
        self.stats.by_replay += 1;
        self.stats.replay_messages += messages;
        self.state = Recovery::Reconciling {
            from_sequence: through + 1,
        };
        true
    }

    pub fn note_replay_refused(&mut self) {
        self.stats.replay_refused += 1;
    }

    /// Replays the held datagrams that the snapshot did not already cover.
    ///
    /// The closure gets `(skip_below, bytes)` per datagram, in the order they
    /// were held — which is the order they arrived, which is sequence order,
    /// because the arbitrator only ever hands over contiguous traffic.
    ///
    /// `skip_below` is the load-bearing argument. A datagram can **straddle** the
    /// snapshot boundary: it starts at a sequence the snapshot already reflects
    /// and ends past it. Replaying such a datagram whole double-applies its
    /// first few messages, and double-applying an `AddOrder` or a `Reduce` puts
    /// the book quietly and permanently wrong. Batching makes this the normal
    /// case rather than a corner: with 32 messages per datagram, the boundary
    /// lands mid-datagram unless it lands exactly on a seam.
    ///
    /// So the caller is told where to start *within* the datagram, and skips
    /// messages below it while decoding.
    pub fn replay(&mut self, mut f: impl FnMut(u64, &[u8])) -> u64 {
        let from = match self.state {
            Recovery::Reconciling { from_sequence } => from_sequence,
            _ => return 0,
        };
        let mut replayed = 0u64;
        for slot in self.held.iter().take(self.occupied) {
            let end = slot.first_sequence + u64::from(slot.count);
            if end <= from {
                // Entirely covered by the snapshot.
                self.stats.messages_skipped += u64::from(slot.count);
                continue;
            }
            let skipped_here = from.saturating_sub(slot.first_sequence);
            self.stats.messages_skipped += skipped_here;
            f(from, &slot.buf[..slot.len]);
            replayed += u64::from(slot.count) - skipped_here;
        }
        self.stats.messages_replayed += replayed;
        replayed
    }

    /// Recovery succeeded. Returns how long it took.
    pub fn complete(&mut self, now: Instant) -> Duration {
        let elapsed = self
            .started
            .map(|s| now.duration_since(s))
            .unwrap_or_default();
        self.stats.completed += 1;
        self.stats.last_recovery_millis = elapsed.as_millis() as u64;
        self.stats.worst_recovery_millis = self
            .stats
            .worst_recovery_millis
            .max(self.stats.last_recovery_millis);
        self.reset();
        elapsed
    }

    pub fn fail(&mut self) {
        self.stats.failed += 1;
        self.reset();
    }

    fn reset(&mut self) {
        self.state = Recovery::Idle;
        self.started = None;
        self.occupied = 0;
    }

    pub fn held_datagrams(&self) -> usize {
        self.occupied
    }

    /// The lowest sequence still held, if anything is.
    ///
    /// Used to check that a replay actually closed the hole: if the held traffic
    /// does not start where the replay ended, another gap opened while the
    /// request was in flight and there is still something missing.
    pub fn first_held_sequence(&self) -> Option<u64> {
        self.held
            .iter()
            .take(self.occupied)
            .map(|h| h.first_sequence)
            .min()
    }

    /// Keeps the recovery open with a new starting point.
    ///
    /// A replay covers the range that was asked for, which was fixed when the
    /// request went out. Gaps that opened while it was in flight are not in it,
    /// so completing on the answer alone would leave a hole in the book while
    /// reporting success. This puts the recovery back into waiting with the
    /// remaining hole named, so the next request or the next snapshot closes it.
    pub fn reopen(&mut self, gap_from: u64) {
        self.state = Recovery::AwaitingSnapshot { gap_from };
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer() -> RecoveryBuffer {
        RecoveryBuffer::new(8, 256, Duration::from_millis(500))
    }

    #[test]
    fn a_fresh_buffer_is_idle() {
        let b = buffer();
        assert_eq!(b.state(), Recovery::Idle);
        assert!(!b.is_recovering());
    }

    #[test]
    fn beginning_twice_does_not_restart_the_clock() {
        // A stream losing messages steadily would otherwise never time out and
        // never report failure.
        let mut b = buffer();
        let t0 = Instant::now();
        b.begin(100, t0);
        b.begin(200, t0 + Duration::from_millis(400));
        assert_eq!(b.state(), Recovery::AwaitingSnapshot { gap_from: 100 });
        assert_eq!(b.stats().attempts, 1);
        assert!(b.check_deadline(t0 + Duration::from_millis(600)).is_err());
    }

    #[test]
    fn the_buffer_is_bounded() {
        let mut b = RecoveryBuffer::new(2, 64, Duration::from_secs(1));
        b.begin(10, Instant::now());
        assert!(b.hold(10, 4, &[0u8; 32]).is_ok());
        assert!(b.hold(14, 4, &[0u8; 32]).is_ok());
        assert_eq!(
            b.hold(18, 4, &[0u8; 32]),
            Err(RecoveryFailure::BufferFull { capacity: 2 })
        );
    }

    #[test]
    fn a_stale_snapshot_is_refused() {
        // It reflects a point before the missing messages, so adopting it would
        // discard good state and still leave the hole.
        let mut b = buffer();
        b.begin(1_000, Instant::now());
        assert!(!b.snapshot_is_usable(500), "500 is well before the gap");
        assert!(!b.snapshot_is_usable(998), "998 still misses sequence 999");
        assert!(
            b.snapshot_is_usable(999),
            "999 covers everything before the gap"
        );
        assert!(b.snapshot_is_usable(5_000), "a later snapshot is fine too");
    }

    #[test]
    fn replay_skips_what_the_snapshot_already_covered() {
        let mut b = buffer();
        b.begin(100, Instant::now());
        // Live traffic kept arriving while we waited.
        b.hold(100, 10, &[1u8; 32]).unwrap();
        b.hold(110, 10, &[2u8; 32]).unwrap();
        b.hold(120, 10, &[3u8; 32]).unwrap();

        // The snapshot reflects everything through 109.
        b.adopt_snapshot(109);
        let mut seen = Vec::new();
        let replayed = b.replay(|skip_below, _| seen.push(skip_below));

        assert_eq!(
            seen.len(),
            2,
            "the datagram the snapshot fully covers must not be replayed at all"
        );
        assert_eq!(replayed, 20);
        assert_eq!(b.stats().messages_skipped, 10);
    }

    #[test]
    fn replay_delivers_everything_when_the_snapshot_predates_the_buffer() {
        let mut b = buffer();
        b.begin(100, Instant::now());
        b.hold(100, 10, &[1u8; 32]).unwrap();
        b.hold(110, 10, &[2u8; 32]).unwrap();
        b.adopt_snapshot(99);
        let mut calls = 0;
        let replayed = b.replay(|_, _| calls += 1);
        assert_eq!(calls, 2);
        assert_eq!(replayed, 20);
        assert_eq!(b.stats().messages_skipped, 0);
    }

    #[test]
    fn a_datagram_straddling_the_boundary_reports_where_to_resume() {
        // The bug this exists to prevent: replaying such a datagram whole
        // double-applies the messages the snapshot already reflects, and a
        // double-applied AddOrder leaves the book permanently wrong. With 32
        // messages per datagram the boundary lands mid-datagram unless it lands
        // exactly on a seam, so this is the normal case.
        let mut b = buffer();
        b.begin(100, Instant::now());
        b.hold(100, 10, &[1u8; 32]).unwrap();
        // The snapshot covers through 104, so 100..=104 are already in the book.
        b.adopt_snapshot(104);

        let mut seen = Vec::new();
        let replayed = b.replay(|skip_below, _| seen.push(skip_below));
        assert_eq!(seen, vec![105], "the caller must be told to resume at 105");
        assert_eq!(replayed, 5, "only 105..=109 are new");
        assert_eq!(b.stats().messages_skipped, 5);
    }

    #[test]
    fn completing_records_the_time_and_returns_to_idle() {
        let mut b = buffer();
        let t0 = Instant::now();
        b.begin(1, t0);
        b.hold(1, 4, &[0u8; 16]).unwrap();
        b.adopt_snapshot(0);
        let elapsed = b.complete(t0 + Duration::from_millis(42));
        assert_eq!(elapsed.as_millis(), 42);
        assert_eq!(b.state(), Recovery::Idle);
        assert_eq!(b.stats().completed, 1);
        assert_eq!(b.stats().last_recovery_millis, 42);
        assert_eq!(b.held_datagrams(), 0, "the buffer is reusable");
    }

    #[test]
    fn the_worst_case_is_kept_not_just_the_last() {
        let mut b = buffer();
        let t0 = Instant::now();
        b.begin(1, t0);
        b.complete(t0 + Duration::from_millis(90));
        b.begin(2, t0);
        b.complete(t0 + Duration::from_millis(20));
        assert_eq!(b.stats().last_recovery_millis, 20);
        assert_eq!(
            b.stats().worst_recovery_millis,
            90,
            "a threshold assertion has to see the worst case, not the most recent"
        );
    }

    #[test]
    fn failing_returns_to_idle_so_the_next_gap_can_be_attempted() {
        let mut b = buffer();
        b.begin(1, Instant::now());
        b.fail();
        assert_eq!(b.state(), Recovery::Idle);
        assert_eq!(b.stats().failed, 1);
        assert_eq!(b.stats().completed, 0);
    }

    #[test]
    fn nothing_is_replayed_while_still_waiting_for_a_snapshot() {
        let mut b = buffer();
        b.begin(1, Instant::now());
        b.hold(1, 4, &[0u8; 16]).unwrap();
        let mut called = false;
        b.replay(|_, _| called = true);
        assert!(
            !called,
            "replay before adopting a snapshot would be premature"
        );
    }
}
