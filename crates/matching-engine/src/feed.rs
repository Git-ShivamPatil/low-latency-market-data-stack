//! Packing messages into datagrams and putting them on both channels.
//!
//! # Batching is the design, not a tuning pass
//!
//! At ~40 bytes per incremental update, a million messages a second is only
//! ~40MB/s — nothing. The binding constraint is packets per second: a kernel UDP
//! path tops out somewhere around 300-600K pps per core even with batched
//! syscalls, so one message per datagram puts the ceiling below the target no
//! matter how fast the encoder is. Sixteen to thirty-two messages per datagram
//! turns 1M msg/s into 31-62K pps, which is unremarkable.
//!
//! Hence [`FeedPublisher::batch_size`] is a visible knob rather than a constant,
//! and hence the number has to be published next to any throughput figure. A
//! reader who assumes one message per packet is reading a much stronger claim
//! than the one this project makes.
//!
//! # A and B carry the same sequences
//!
//! The two channels are mirrors: the same messages with the same sequence
//! numbers, sent twice. That is the point — a consumer takes whichever datagram
//! arrives first and discards the other, so an isolated loss on one arm costs
//! nothing. Only `packetHeader.channel` differs between the two copies, which is
//! why the header is rewritten per arm rather than the body being rebuilt.

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use transport::Publisher;
use wire::{ModifyReason, Side, WireError};

use crate::rng::Rng;

/// How simulated loss is correlated between the two arms.
///
/// This distinction is not pedantry — it decides what the redundancy test can
/// actually prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMode {
    /// Each arm decides on its own. This is what a real network does, and it
    /// means some datagrams are lost on *both* arms: at 2% per arm, 0.04% of
    /// datagrams vanish entirely. Over 10M messages in ~333K datagrams that is
    /// around 133 unavoidable double-losses, so "zero gaps" is arithmetically
    /// impossible here and claiming it would be false.
    Independent,
    /// A dropped datagram is dropped on exactly one arm, chosen at random.
    ///
    /// Not physically realistic, and that is the point: it isolates the
    /// arbitration logic from the statistics. Under this mode "single-arm loss
    /// costs nothing" is a property the code either has or does not, and zero
    /// arbitrated gaps is a theorem rather than a probability.
    Exclusive,
    /// The same datagrams are dropped on both arms. Redundancy cannot help, so
    /// the handler must detect the loss and name the range.
    Correlated,
}

impl DropMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Exclusive => "exclusive",
            Self::Correlated => "correlated",
        }
    }
}

impl std::fmt::Display for DropMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for DropMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "independent" => Ok(Self::Independent),
            "exclusive" => Ok(Self::Exclusive),
            "correlated" => Ok(Self::Correlated),
            other => Err(format!(
                "unknown drop mode {other:?}: expected independent, exclusive or correlated"
            )),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FeedStats {
    pub messages: u64,
    pub datagrams: u64,
    pub bytes: u64,
    /// Datagrams cut short because the next message would not fit, rather than
    /// because the batch was full. A high count means `max_datagram_bytes` and
    /// `batch_size` disagree about what a batch is.
    pub size_flushes: u64,
    /// Datagrams sent because the flush interval expired with a partial batch.
    pub timer_flushes: u64,
    /// Snapshot datagrams sent. Counted separately: they are not part of the
    /// incremental stream and must not inflate its throughput.
    pub snapshot_datagrams: u64,
    /// Datagrams deliberately not sent, per arm, by the loss injector.
    pub dropped: [u64; 2],
    /// Datagrams dropped on **both** arms, so redundancy could not help.
    ///
    /// This is the number that predicts how many gaps the handler must report.
    /// Under `exclusive` loss it is zero by construction; under `independent`
    /// loss at rate p it is about p² of all datagrams, which is why "zero gaps
    /// under independent loss" is not a claim this project can make.
    pub dropped_both: u64,
    /// Messages carried by datagrams dropped on both arms.
    pub messages_lost_both: u64,
}

impl FeedStats {
    pub fn messages_per_datagram(&self) -> f64 {
        if self.datagrams == 0 {
            0.0
        } else {
            self.messages as f64 / self.datagrams as f64
        }
    }
}

/// Owns the datagram buffer, the sequence counter, and both channels.
#[derive(Debug)]
pub struct FeedPublisher {
    a: Publisher,
    b: Publisher,
    buf: Vec<u8>,
    /// Write position within `buf`. Never rewinds except on flush.
    pos: usize,
    count: u16,
    /// Sequence of the message at index 0 of the current datagram.
    first_seq: u64,
    /// Sequence the next message will be assigned.
    next_seq: u64,
    batch_size: u16,
    stats: FeedStats,
    /// Probability that a datagram is dropped rather than sent. 0 disables the
    /// injector entirely, including its RNG draw.
    drop_rate: f64,
    drop_mode: DropMode,
    /// Seeded separately from the order-flow generator so that turning loss on
    /// does not change which orders are produced. Without that, a run with loss
    /// and a run without would not be comparable.
    drop_rng: Rng,
    /// A separate buffer for snapshot datagrams so a cycle cannot disturb a
    /// half-filled incremental batch.
    snapshot_buf: Vec<u8>,
    /// Snapshot datagrams have their own sequence space; see `publish_snapshot`.
    snapshot_seq: u64,
    /// When self-checking, every datagram is decoded and replayed into here
    /// before it is sent.
    ///
    /// It lives on the publisher rather than in the run loop so that *every*
    /// flush is covered, including the ones `emit` triggers internally when a
    /// batch fills. Verifying from outside would either miss those or force a
    /// flush per message, which would quietly disable the batching this is
    /// supposed to be checking.
    shadow: Option<book::Books>,
}

impl FeedPublisher {
    pub fn new(a: Publisher, b: Publisher, batch_size: u16, max_datagram_bytes: usize) -> Self {
        Self {
            a,
            b,
            // Allocated once, at startup. Nothing on the publish path allocates
            // after this point.
            buf: vec![0u8; max_datagram_bytes],
            pos: wire::PACKET_HEADER_LEN,
            count: 0,
            first_seq: 1,
            next_seq: 1,
            batch_size: batch_size.max(1),
            stats: FeedStats::default(),
            drop_rate: 0.0,
            drop_mode: DropMode::Independent,
            drop_rng: Rng::new(0x0105_0B10_5510_0000),
            snapshot_buf: vec![0u8; max_datagram_bytes],
            snapshot_seq: 1,
            shadow: None,
        }
    }

    /// Turns on simulated packet loss.
    ///
    /// The publisher simply does not call `send` for a dropped datagram. That is
    /// deliberately cruder than a network emulator and deliberately at the right
    /// layer: what the handler has to survive is a datagram that never arrives,
    /// and reproducing that exactly needs no `tc qdisc`, no privileges, and no
    /// second machine.
    pub fn set_loss(&mut self, rate: f64, mode: DropMode, seed: u64) {
        self.drop_rate = rate.clamp(0.0, 1.0);
        self.drop_mode = mode;
        self.drop_rng = Rng::new(seed);
    }

    pub fn loss_enabled(&self) -> bool {
        self.drop_rate > 0.0
    }

    /// Decides, once per datagram, which arms will not receive it.
    fn drop_arms(&mut self) -> [bool; 2] {
        if self.drop_rate <= 0.0 {
            return [false, false];
        }
        match self.drop_mode {
            DropMode::Independent => [
                self.drop_rng.chance(self.drop_rate),
                self.drop_rng.chance(self.drop_rate),
            ],
            DropMode::Exclusive => {
                if self.drop_rng.chance(self.drop_rate) {
                    // Exactly one arm, chosen fairly.
                    if self.drop_rng.chance(0.5) {
                        [true, false]
                    } else {
                        [false, true]
                    }
                } else {
                    [false, false]
                }
            }
            DropMode::Correlated => {
                let lost = self.drop_rng.chance(self.drop_rate);
                [lost, lost]
            }
        }
    }

    /// Decode every datagram before sending it and rebuild a book from the
    /// result. Costs a full decode and book update per message, so it is opt-in.
    pub fn enable_self_check(&mut self) {
        self.shadow = Some(book::Books::new());
    }

    /// The book rebuilt from the bytes actually sent, if self-checking.
    pub fn shadow(&self) -> Option<&book::Books> {
        self.shadow.as_ref()
    }

    pub fn stats(&self) -> FeedStats {
        self.stats
    }

    /// The highest sequence handed out so far, or 0 before anything published.
    pub fn last_sequence(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// Encodes one message into the current datagram, flushing first if it will
    /// not fit or the batch is already full.
    ///
    /// `encode` is `Fn` rather than `FnOnce` because it may be called twice: once
    /// speculatively, and again after a flush has made room.
    fn emit(&mut self, encode: impl Fn(&mut [u8]) -> Result<usize, WireError>) -> io::Result<u64> {
        if self.count >= self.batch_size {
            self.flush()?;
        }
        if self.count == 0 {
            self.first_seq = self.next_seq;
        }

        let written = match encode(&mut self.buf[self.pos..]) {
            Ok(n) => n,
            Err(WireError::ShortBuffer { .. }) => {
                // The message does not fit in what is left. Send what we have
                // and try again into an empty datagram.
                if self.count == 0 {
                    // An empty datagram could not hold it either, so the buffer
                    // is simply too small for this message. Configuration bug,
                    // not a runtime condition.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "a single message does not fit in feed.max_datagram_bytes ({})",
                            self.buf.len()
                        ),
                    ));
                }
                self.stats.size_flushes += 1;
                self.flush()?;
                self.first_seq = self.next_seq;
                encode(&mut self.buf[self.pos..]).map_err(wire_to_io)?
            }
            Err(e) => return Err(wire_to_io(e)),
        };

        self.pos += written;
        self.count += 1;
        let seq = self.next_seq;
        self.next_seq += 1;
        self.stats.messages += 1;
        Ok(seq)
    }

    /// Sends the current datagram on both arms and starts a new one.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.count == 0 {
            return Ok(());
        }
        // One clock read per datagram, not per message. At a batch of 32 this
        // is the difference between 31K and 1M `clock_gettime` calls a second.
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let len = self.pos;
        let count = self.count;
        let first_seq = self.first_seq;

        // Decided once per datagram so both arms see the same coin flip where
        // the mode says they should.
        let drop = self.drop_arms();
        if drop[0] && drop[1] {
            self.stats.dropped_both += 1;
            self.stats.messages_lost_both += u64::from(count);
        }

        // The header is written ONCE, before any arm is considered. Writing it
        // inside the loop meant that a datagram dropped on both arms never got a
        // header at all, and the self-check below then decoded whatever was left
        // in the buffer from the previous datagram.
        wire::encode_packet_header(&mut self.buf, 0, 0, first_seq, now_ns).map_err(wire_to_io)?;
        wire::patch_message_count(&mut self.buf, count).map_err(wire_to_io)?;

        // The two arms differ only in packetHeader.channel.
        for (channel, publisher) in [(0u8, &self.a), (1u8, &self.b)] {
            let arm = usize::from(channel);
            if drop[arm] {
                self.stats.dropped[arm] += 1;
                continue;
            }
            wire::patch_packet_channel(&mut self.buf, channel).map_err(wire_to_io)?;
            publisher.send(&self.buf[..len])?;
        }

        // Replay the bytes that just went out. Disjoint field borrows, so the
        // reader over `buf` and the mutable `shadow` coexist.
        if let Some(shadow) = self.shadow.as_mut() {
            let reader = wire::PacketReader::new(&self.buf[..len]).map_err(wire_to_io)?;
            for m in reader.messages() {
                let (seq, msg) = m.map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("self-check: could not decode what was just encoded: {e}"),
                    )
                })?;
                book::apply_message(shadow, &msg).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("self-check: sequence {seq} does not apply: {e}"),
                    )
                })?;
            }
        }

        self.stats.datagrams += 1;
        // Only what actually went on the wire. Counting both arms
        // unconditionally would overstate bytes sent whenever loss is injected.
        let sent_arms = u64::from(!drop[0]) + u64::from(!drop[1]);
        self.stats.bytes += (len as u64) * sent_arms;
        self.pos = wire::PACKET_HEADER_LEN;
        self.count = 0;
        self.first_seq = self.next_seq;
        Ok(())
    }

    /// Flushes a partially-filled datagram because the flush interval expired.
    pub fn flush_on_timer(&mut self) -> io::Result<()> {
        if self.count == 0 {
            return Ok(());
        }
        self.stats.timer_flushes += 1;
        self.flush()
    }

    pub fn describe(&self) -> String {
        format!(
            "A: {}\n  B: {}\n  batching {} messages per datagram, {} byte cap",
            self.a.describe(),
            self.b.describe(),
            self.batch_size,
            self.buf.len()
        )
    }

    pub fn add_order(
        &mut self,
        order_id: u64,
        price: i64,
        quantity: u32,
        symbol_id: u16,
        side: Side,
    ) -> io::Result<u64> {
        self.emit(|buf| wire::encode_add_order(buf, order_id, price, quantity, symbol_id, side))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn modify_order(
        &mut self,
        order_id: u64,
        new_price: i64,
        new_quantity: u32,
        symbol_id: u16,
        side: Side,
        reason: ModifyReason,
    ) -> io::Result<u64> {
        self.emit(|buf| {
            wire::encode_modify_order(
                buf,
                order_id,
                new_price,
                new_quantity,
                symbol_id,
                side,
                reason,
            )
        })
    }

    pub fn delete_order(&mut self, order_id: u64, symbol_id: u16, side: Side) -> io::Result<u64> {
        self.emit(|buf| wire::encode_delete_order(buf, order_id, symbol_id, side))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn trade(
        &mut self,
        trade_id: u64,
        aggressor_order_id: u64,
        resting_order_id: u64,
        price: i64,
        quantity: u32,
        symbol_id: u16,
        aggressor_side: Side,
    ) -> io::Result<u64> {
        self.emit(|buf| {
            wire::encode_trade(
                buf,
                trade_id,
                aggressor_order_id,
                resting_order_id,
                price,
                quantity,
                symbol_id,
                aggressor_side,
            )
        })
    }

    pub fn heartbeat(&mut self, last_sequence: u64) -> io::Result<u64> {
        self.emit(|buf| wire::encode_heartbeat(buf, last_sequence))
    }

    /// Publishes a full snapshot of every symbol on both arms.
    ///
    /// # Why snapshots share the channels but not the sequence space
    ///
    /// They ride the same two sockets, marked with `PACKET_FLAG_SNAPSHOT`, and
    /// carry their **own** sequence numbering. A consumer routes on the flag:
    /// snapshot datagrams never reach the incremental arbitrator, so they cannot
    /// look like a gap or a duplicate in the live stream.
    ///
    /// Sharing one sequence space would be worse than it sounds. A handler that
    /// is `LIVE` would see snapshot messages as increments and apply a whole book
    /// on top of the one it already has; one that is `GAPPED` could not tell
    /// whether a snapshot filled its hole or widened it.
    ///
    /// # Consistency
    ///
    /// Each `Snapshot` carries `lastSequence`: the incremental sequence the book
    /// reflects. Because the engine is single-threaded and the incremental buffer
    /// is flushed first, that is exactly `last_sequence()` at entry, and nothing
    /// can change the book while the cycle is being written.
    ///
    /// # Fragmentation
    ///
    /// A symbol with more resting orders than fit in one datagram is split
    /// across several `Snapshot` messages, one per datagram, and only the last
    /// carries `SNAPSHOT_FLAG_LAST_FRAGMENT`. A consumer must not treat a partial
    /// book as complete.
    pub fn publish_snapshot(&mut self, books: &book::Books) -> io::Result<SnapshotCycle> {
        // Anything still buffered belongs before the snapshot, or the snapshot
        // would claim to reflect messages that have not gone out yet.
        self.flush()?;

        let last_sequence = self.last_sequence();
        let mut cycle = SnapshotCycle {
            sequence: self.snapshot_seq,
            last_sequence,
            datagrams: 0,
            orders: 0,
            symbols: 0,
        };

        // The last symbol needs to be known in advance so its final fragment can
        // carry the cycle-end marker. A consumer recovering from a gap waits for
        // that marker rather than for any one symbol to finish.
        let symbol_count = books.iter().count();
        let mut first_fragment_of_cycle = true;
        for (index, (symbol_id, bookref)) in books.iter().enumerate() {
            let is_last_symbol = index + 1 == symbol_count;
            let mut orders = bookref.orders_in_queue_order(Side::Bid);
            orders.extend(bookref.orders_in_queue_order(Side::Ask));
            cycle.symbols += 1;

            let mut written = 0usize;
            // A symbol with no resting orders still gets one empty fragment.
            // Silence is ambiguous: a recovering consumer cannot tell "this book
            // is empty" from "this symbol was omitted" without it.
            loop {
                let head = wire::PACKET_HEADER_LEN;
                let mut packed = 0usize;
                let n = {
                    let mut e = wire::SnapshotEncoder::start(
                        &mut self.snapshot_buf[head..],
                        last_sequence,
                        *symbol_id,
                        0,
                    )
                    .map_err(wire_to_io)?;
                    while written + packed < orders.len() {
                        let o = orders[written + packed];
                        if e.push_order(o.order_id, o.price, o.quantity, o.side)
                            .is_err()
                        {
                            break;
                        }
                        packed += 1;
                    }
                    e.finish()
                };
                if packed == 0 && !orders.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "not one snapshot order fits in feed.max_datagram_bytes",
                    ));
                }
                written += packed;
                // Rewrite the root block with the flags set. Cheaper and less
                // error-prone than threading them through the encoder, since
                // whether this is the last fragment is only known now.
                let last_fragment = written >= orders.len();
                let mut flags = 0u8;
                if first_fragment_of_cycle {
                    flags |= wire::SNAPSHOT_FLAG_CYCLE_START;
                    first_fragment_of_cycle = false;
                }
                if last_fragment {
                    flags |= wire::SNAPSHOT_FLAG_LAST_FRAGMENT;
                    if is_last_symbol {
                        flags |= wire::SNAPSHOT_FLAG_CYCLE_END;
                    }
                }
                if flags != 0 {
                    wire::patch_snapshot_flags(&mut self.snapshot_buf[head..], flags)
                        .map_err(wire_to_io)?;
                }

                self.send_snapshot_datagram(head + n)?;
                cycle.datagrams += 1;
                cycle.orders += packed as u64;
                if last_fragment {
                    break;
                }
            }
        }
        Ok(cycle)
    }

    fn send_snapshot_datagram(&mut self, len: usize) -> io::Result<()> {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let seq = self.snapshot_seq;
        self.snapshot_seq += 1;

        // Snapshots are deliberately NOT subject to the loss injector. Injected
        // loss models the network between the engine and a handler; a snapshot
        // that recovery depends on being dropped by the test harness would make
        // recovery untestable rather than more realistic. Real loss on the
        // snapshot channel is a separate scenario, and the recovery tests drive
        // it explicitly instead of leaving it to chance.
        wire::encode_packet_header(
            &mut self.snapshot_buf,
            0,
            wire::PACKET_FLAG_SNAPSHOT,
            seq,
            now_ns,
        )
        .map_err(wire_to_io)?;
        wire::patch_message_count(&mut self.snapshot_buf, 1).map_err(wire_to_io)?;
        for (channel, publisher) in [(0u8, &self.a), (1u8, &self.b)] {
            wire::patch_packet_channel(&mut self.snapshot_buf, channel).map_err(wire_to_io)?;
            publisher.send(&self.snapshot_buf[..len])?;
        }
        self.stats.snapshot_datagrams += 1;
        Ok(())
    }
}

/// What one snapshot cycle put on the wire.
#[derive(Debug, Clone, Copy, Default)]
pub struct SnapshotCycle {
    /// First snapshot-space sequence used by this cycle.
    pub sequence: u64,
    /// The incremental sequence the books reflect.
    pub last_sequence: u64,
    pub datagrams: u64,
    pub orders: u64,
    pub symbols: u64,
}

fn wire_to_io(e: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use transport::{Receiver, SocketOptions, TransportMode};
    use wire::{Message, PacketReader};

    fn loopback_opts() -> SocketOptions {
        SocketOptions {
            interface: Ipv4Addr::LOCALHOST,
            ttl: 0,
            loopback: true,
            buffer_bytes: 1024 * 1024,
        }
    }

    /// Wires a publisher to two real sockets over loopback unicast, so these
    /// tests exercise the same send path the binary does.
    fn rig(batch_size: u16, max_bytes: usize) -> (FeedPublisher, Receiver, Receiver) {
        let ra = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            loopback_opts(),
        )
        .unwrap();
        let rb = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            loopback_opts(),
        )
        .unwrap();
        let pa =
            Publisher::bind(TransportMode::UnicastFanout, &[to_v4(&ra)], loopback_opts()).unwrap();
        let pb =
            Publisher::bind(TransportMode::UnicastFanout, &[to_v4(&rb)], loopback_opts()).unwrap();
        for r in [&ra, &rb] {
            r.set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
        }
        (FeedPublisher::new(pa, pb, batch_size, max_bytes), ra, rb)
    }

    fn to_v4(r: &Receiver) -> SocketAddrV4 {
        // The receivers bind port 0, so ask the socket which port it was given.
        match r.local_addr().expect("bound socket has an address") {
            std::net::SocketAddr::V4(a) => a,
            other => panic!("expected an IPv4 socket, got {other}"),
        }
    }

    fn drain(r: &Receiver) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = [0u8; 2048];
        r.set_read_timeout(Some(std::time::Duration::from_millis(200)))
            .unwrap();
        while let Ok(n) = r.recv(&mut buf) {
            out.push(buf[..n].to_vec());
        }
        out
    }

    #[test]
    fn messages_are_packed_until_the_batch_is_full() {
        let (mut feed, ra, _rb) = rig(4, 1400);
        for i in 0..4u64 {
            feed.add_order(i, 100, 1, 1, Side::Bid).unwrap();
        }
        // Four messages at a batch size of four: still buffered, nothing sent.
        assert_eq!(feed.stats().datagrams, 0);
        feed.flush().unwrap();

        let grams = drain(&ra);
        assert_eq!(grams.len(), 1, "one datagram, not four");
        let r = PacketReader::new(&grams[0]).unwrap();
        assert_eq!(r.header().message_count(), 4);
        assert_eq!(r.header().first_sequence(), 1);
    }

    #[test]
    fn both_arms_carry_the_same_bytes_apart_from_the_channel() {
        let (mut feed, ra, rb) = rig(2, 1400);
        feed.add_order(1, 100, 1, 1, Side::Bid).unwrap();
        feed.flush().unwrap();

        let ga = drain(&ra);
        let gb = drain(&rb);
        assert_eq!(ga.len(), 1);
        assert_eq!(gb.len(), 1);
        assert_eq!(ga[0].len(), gb[0].len());

        let ha = PacketReader::new(&ga[0]).unwrap().header();
        let hb = PacketReader::new(&gb[0]).unwrap().header();
        assert_eq!(ha.channel(), 0);
        assert_eq!(hb.channel(), 1);
        assert_eq!(
            ha.first_sequence(),
            hb.first_sequence(),
            "the arms are mirrors; a consumer takes whichever lands first"
        );

        // Everything except the channel byte at offset 6 must be identical.
        let mut a = ga[0].clone();
        let mut b = gb[0].clone();
        a[6] = 0;
        b[6] = 0;
        assert_eq!(a, b);
    }

    #[test]
    fn sequences_are_contiguous_across_datagram_boundaries() {
        // The property the handler's gap detection depends on: batching must not
        // introduce a hole at the seam between two datagrams.
        let (mut feed, ra, _rb) = rig(3, 1400);
        for i in 0..10u64 {
            feed.add_order(i, 100, 1, 1, Side::Bid).unwrap();
        }
        feed.flush().unwrap();

        let mut seen = Vec::new();
        for g in drain(&ra) {
            let r = PacketReader::new(&g).unwrap();
            for m in r.messages() {
                seen.push(m.unwrap().0);
            }
        }
        assert_eq!(seen, (1..=10).collect::<Vec<u64>>());
    }

    #[test]
    fn a_datagram_too_small_for_the_batch_flushes_on_size_instead() {
        // 24 header + 4 x (8 + 24) = 152 would be needed for a batch of four
        // AddOrders; give it room for two and it must cut the datagram short
        // rather than overrun or drop.
        let (mut feed, ra, _rb) = rig(4, 24 + 2 * (8 + 24));
        for i in 0..4u64 {
            feed.add_order(i, 100, 1, 1, Side::Bid).unwrap();
        }
        feed.flush().unwrap();

        assert!(
            feed.stats().size_flushes > 0,
            "the size cap should have bitten"
        );
        let mut seen = Vec::new();
        for g in drain(&ra) {
            let r = PacketReader::new(&g).unwrap();
            for m in r.messages() {
                seen.push(m.unwrap().0);
            }
        }
        assert_eq!(seen, vec![1, 2, 3, 4], "no message may be lost at the seam");
    }

    #[test]
    fn a_message_larger_than_the_datagram_is_a_configuration_error() {
        let (mut feed, _ra, _rb) = rig(4, wire::PACKET_HEADER_LEN + 8);
        let err = feed.add_order(1, 100, 1, 1, Side::Bid).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn flushing_an_empty_datagram_sends_nothing() {
        let (mut feed, ra, _rb) = rig(4, 1400);
        feed.flush().unwrap();
        feed.flush_on_timer().unwrap();
        assert_eq!(feed.stats().datagrams, 0);
        assert!(drain(&ra).is_empty());
    }

    #[test]
    fn every_message_type_survives_the_round_trip() {
        let (mut feed, ra, _rb) = rig(16, 1400);
        feed.add_order(1, 1000, 5, 7, Side::Bid).unwrap();
        feed.modify_order(1, 1000, 3, 7, Side::Bid, ModifyReason::Reduce)
            .unwrap();
        feed.trade(9, 2, 1, 1000, 2, 7, Side::Ask).unwrap();
        feed.delete_order(1, 7, Side::Bid).unwrap();
        feed.heartbeat(4).unwrap();
        feed.flush().unwrap();

        let grams = drain(&ra);
        let r = PacketReader::new(&grams[0]).unwrap();
        let kinds: Vec<u16> = r.messages().map(|m| m.unwrap().1.template_id()).collect();
        assert_eq!(
            kinds,
            vec![
                wire::template::ADD_ORDER,
                wire::template::MODIFY_ORDER,
                wire::template::TRADE,
                wire::template::DELETE_ORDER,
                wire::template::HEARTBEAT,
            ]
        );

        // And the fields survived.
        let r = PacketReader::new(&grams[0]).unwrap();
        let (_, first) = r.messages().next().unwrap().unwrap();
        let Message::AddOrder(d) = first else {
            panic!("expected AddOrder")
        };
        assert_eq!(d.order_id(), 1);
        assert_eq!(d.price(), 1000);
        assert_eq!(d.quantity(), 5);
        assert_eq!(d.symbol_id(), 7);
    }
}
