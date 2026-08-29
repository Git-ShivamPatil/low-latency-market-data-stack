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
            shadow: None,
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

        // The two arms differ only in packetHeader.channel, so the header is
        // rewritten in place per arm and the body is sent untouched.
        for (channel, publisher) in [(0u8, &self.a), (1u8, &self.b)] {
            wire::encode_packet_header(&mut self.buf, channel, 0, first_seq, now_ns)
                .map_err(wire_to_io)?;
            wire::patch_message_count(&mut self.buf, count).map_err(wire_to_io)?;
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
        self.stats.bytes += (len as u64) * 2;
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
