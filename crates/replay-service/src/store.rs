//! A bounded, contiguous history of published datagrams.
//!
//! # Bounded
//!
//! The store is a ring. Old datagrams are overwritten by new ones, so memory is
//! fixed and a service that has been running for a week holds the same amount as
//! one started a minute ago. What it costs is a horizon: a consumer that asks for
//! a range older than the ring gets [`Status::TooOld`] and has to recover from a
//! snapshot instead. That is the honest trade, and the horizon is reported in
//! every response so a consumer can see how close it came.
//!
//! # Contiguous
//!
//! The store refuses to hold a hole.
//!
//! Every datagram carries `firstSequence` and `messageCount`, so a gap in what
//! the store received is detectable on arrival. When one appears — the uplink
//! dropped a datagram, or reconnected after a break — the store **discards
//! everything before it** and restarts from the new datagram.
//!
//! That is deliberately destructive, and the alternative is worse. A store that
//! kept both sides of a hole would have to check every request against every
//! hole, and the failure mode of getting that wrong is serving a range with a
//! silent gap in it — a consumer that asked for help and got corruption. Holding
//! only a contiguous run makes "can I serve this?" a comparison of two numbers.

use crate::protocol::Status;

#[derive(Debug)]
struct Slot {
    first_sequence: u64,
    message_count: u16,
    len: usize,
    buf: Box<[u8]>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StoreStats {
    pub datagrams_stored: u64,
    pub messages_stored: u64,
    pub datagrams_evicted: u64,
    /// Times a discontinuity forced the store to restart. Non-zero means the
    /// uplink is lossy, which it is not supposed to be.
    pub discontinuities: u64,
    /// Datagrams ignored because they were older than what is already held.
    pub duplicates: u64,
}

/// What a range lookup found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    pub status: Status,
    /// Ring indices `[start, end)` covering the request, when `status` is `Ok`.
    pub start: usize,
    pub end: usize,
}

pub struct DatagramStore {
    slots: Vec<Slot>,
    /// Ring index of the oldest datagram held.
    head: usize,
    /// How many slots are occupied.
    len: usize,
    /// Sequence of the first message still held.
    first_sequence: u64,
    /// One past the last message held.
    next_sequence: u64,
    stats: StoreStats,
}

impl std::fmt::Debug for DatagramStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatagramStore")
            .field("capacity", &self.slots.len())
            .field("len", &self.len)
            .field("first_sequence", &self.first_sequence)
            .field("next_sequence", &self.next_sequence)
            .finish()
    }
}

impl DatagramStore {
    pub fn new(capacity: usize, max_datagram_bytes: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            slots: (0..capacity)
                .map(|_| Slot {
                    first_sequence: 0,
                    message_count: 0,
                    len: 0,
                    buf: vec![0u8; max_datagram_bytes].into_boxed_slice(),
                })
                .collect(),
            head: 0,
            len: 0,
            first_sequence: 0,
            next_sequence: 0,
            stats: StoreStats::default(),
        }
    }

    pub fn stats(&self) -> StoreStats {
        self.stats
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// The oldest sequence still held.
    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    /// One past the newest sequence held.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Files one published datagram.
    ///
    /// Snapshot datagrams are rejected: they live in their own sequence space,
    /// and mixing them into a store indexed by the incremental one would make
    /// every range lookup wrong.
    pub fn push(&mut self, datagram: &[u8]) -> Result<(), &'static str> {
        let header =
            wire::PacketHeaderDecoder::wrap(datagram).map_err(|_| "datagram does not decode")?;
        if header.is_snapshot() {
            return Err("snapshot datagrams have their own sequence space");
        }
        let first = header.first_sequence();
        let count = header.message_count();
        if count == 0 {
            return Ok(());
        }
        let end = first + u64::from(count);

        if self.len == 0 {
            self.first_sequence = first;
            self.next_sequence = first;
        } else if first < self.next_sequence {
            // Already have it, or it overlaps what we have. The uplink is
            // ordered, so this means a reconnect replayed something.
            self.stats.duplicates += 1;
            return Ok(());
        } else if first > self.next_sequence {
            // A hole. Everything before it becomes unservable, so drop it all
            // rather than hold a history with a lie in the middle.
            self.stats.discontinuities += 1;
            self.stats.datagrams_evicted += self.len as u64;
            self.head = 0;
            self.len = 0;
            self.first_sequence = first;
            self.next_sequence = first;
        }

        if self.len == self.slots.len() {
            // Full: evict the oldest.
            let evicted = &self.slots[self.head];
            self.first_sequence = evicted.first_sequence + u64::from(evicted.message_count);
            self.head = (self.head + 1) % self.slots.len();
            self.len -= 1;
            self.stats.datagrams_evicted += 1;
        }

        let index = (self.head + self.len) % self.slots.len();
        let slot = &mut self.slots[index];
        if datagram.len() > slot.buf.len() {
            return Err("datagram larger than the store's datagram size");
        }
        slot.buf[..datagram.len()].copy_from_slice(datagram);
        slot.len = datagram.len();
        slot.first_sequence = first;
        slot.message_count = count;
        self.len += 1;
        self.next_sequence = end;
        self.stats.datagrams_stored += 1;
        self.stats.messages_stored += u64::from(count);
        Ok(())
    }

    /// Finds the datagrams covering `[from, through]`.
    ///
    /// The returned indices are into [`datagram_at`], not into the ring.
    pub fn locate(&self, from: u64, through: u64) -> Located {
        let miss = |status| Located {
            status,
            start: 0,
            end: 0,
        };
        if from > through {
            return miss(Status::BadRequest);
        }
        if self.len == 0 {
            return miss(Status::TooOld);
        }
        if from < self.first_sequence {
            return miss(Status::TooOld);
        }
        if through >= self.next_sequence {
            return miss(Status::NotYet);
        }

        // The ring is contiguous and ordered, so a scan finds the boundaries.
        // Linear rather than binary: the ranges asked for are small, the ring is
        // cache-friendly, and a wrong binary search over a wrapped ring is a much
        // easier mistake to make than a slow one.
        let mut start = None;
        let mut end = 0usize;
        for i in 0..self.len {
            let slot = &self.slots[(self.head + i) % self.slots.len()];
            let slot_end = slot.first_sequence + u64::from(slot.message_count);
            if slot_end <= from {
                continue;
            }
            if start.is_none() {
                start = Some(i);
            }
            end = i + 1;
            if slot_end > through {
                break;
            }
        }
        match start {
            Some(start) => Located {
                status: Status::Ok,
                start,
                end,
            },
            // The bounds above already rule this out, so reaching it would mean
            // the ring and the sequence counters disagree.
            None => miss(Status::Incomplete),
        }
    }

    /// The `i`th datagram held, oldest first.
    pub fn datagram_at(&self, i: usize) -> Option<&[u8]> {
        if i >= self.len {
            return None;
        }
        let slot = &self.slots[(self.head + i) % self.slots.len()];
        Some(&slot.buf[..slot.len])
    }

    pub fn describe(&self) -> String {
        if self.len == 0 {
            return format!("empty, capacity {} datagrams", self.slots.len());
        }
        format!(
            "sequences {}..{} in {} of {} datagrams",
            self.first_sequence,
            self.next_sequence,
            self.len,
            self.slots.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{PacketWriter, Side};

    fn datagram(first: u64, count: u16) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let mut w = PacketWriter::new(&mut buf, 0, 0, first, 0).unwrap();
        for i in 0..count {
            w.add_order(first + u64::from(i), 1000, 1, 1, Side::Bid)
                .unwrap();
        }
        let n = w.finish();
        buf.truncate(n);
        buf
    }

    fn snapshot_datagram(first: u64) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let mut w = PacketWriter::new(&mut buf, 0, wire::PACKET_FLAG_SNAPSHOT, first, 0).unwrap();
        let n = {
            let mut e = wire::SnapshotEncoder::start(w.tail(), 1, 1, 0).unwrap();
            e.push_order(1, 1000, 1, Side::Bid).unwrap();
            e.finish()
        };
        w.commit(n).unwrap();
        let len = w.finish();
        buf.truncate(len);
        buf
    }

    fn filled(capacity: usize, datagrams: usize, batch: u16) -> DatagramStore {
        let mut s = DatagramStore::new(capacity, 4096);
        for i in 0..datagrams {
            let first = 1 + (i as u64) * u64::from(batch);
            s.push(&datagram(first, batch)).unwrap();
        }
        s
    }

    #[test]
    fn an_empty_store_can_serve_nothing() {
        let s = DatagramStore::new(8, 4096);
        assert_eq!(s.locate(1, 10).status, Status::TooOld);
        assert!(s.is_empty());
    }

    #[test]
    fn a_range_inside_the_history_is_located() {
        let s = filled(16, 10, 10); // sequences 1..=100
        let found = s.locate(25, 34);
        assert_eq!(found.status, Status::Ok);
        // 25..=34 spans the datagrams starting at 21 and 31.
        assert_eq!(found.end - found.start, 2);
        assert_eq!(s.first_sequence(), 1);
        assert_eq!(s.next_sequence(), 101);
    }

    #[test]
    fn a_range_that_fits_one_datagram_returns_one() {
        let s = filled(16, 10, 10);
        let found = s.locate(21, 30);
        assert_eq!(found.status, Status::Ok);
        assert_eq!(found.end - found.start, 1);
    }

    #[test]
    fn asking_for_history_that_has_been_evicted_says_so() {
        // The horizon is the honest cost of a bounded store, and a consumer has
        // to be able to tell "too late" from "broken".
        let s = filled(4, 10, 10); // only the last 4 datagrams survive
        assert_eq!(s.len(), 4);
        assert_eq!(s.first_sequence(), 61);
        assert_eq!(s.locate(1, 10).status, Status::TooOld);
        assert_eq!(s.locate(61, 70).status, Status::Ok);
    }

    #[test]
    fn asking_for_the_future_says_so_rather_than_serving_a_prefix() {
        let s = filled(16, 5, 10); // 1..=50
        assert_eq!(s.locate(45, 60).status, Status::NotYet);
        assert_eq!(
            s.locate(45, 50).status,
            Status::Ok,
            "the same start is fine once the end is in range"
        );
    }

    #[test]
    fn an_inverted_range_is_a_bad_request() {
        let s = filled(16, 5, 10);
        assert_eq!(s.locate(30, 20).status, Status::BadRequest);
    }

    #[test]
    fn a_hole_in_the_uplink_discards_the_history_before_it() {
        // The store must never hold a range with a silent gap in the middle:
        // serving that would be worse than refusing, because the consumer asked
        // for help and would get corruption.
        let mut s = DatagramStore::new(16, 4096);
        s.push(&datagram(1, 10)).unwrap();
        s.push(&datagram(11, 10)).unwrap();
        // 21..=30 never arrives.
        s.push(&datagram(31, 10)).unwrap();

        assert_eq!(s.stats().discontinuities, 1);
        assert_eq!(s.len(), 1, "everything before the hole is gone");
        assert_eq!(s.first_sequence(), 31);
        assert_eq!(
            s.locate(1, 10).status,
            Status::TooOld,
            "the pre-hole history must not be servable"
        );
        assert_eq!(s.locate(31, 40).status, Status::Ok);
    }

    #[test]
    fn a_replayed_datagram_after_a_reconnect_is_ignored() {
        let mut s = DatagramStore::new(16, 4096);
        s.push(&datagram(1, 10)).unwrap();
        s.push(&datagram(11, 10)).unwrap();
        s.push(&datagram(1, 10)).unwrap();
        assert_eq!(s.stats().duplicates, 1);
        assert_eq!(s.len(), 2);
        assert_eq!(s.next_sequence(), 21);
    }

    #[test]
    fn snapshot_datagrams_are_refused() {
        // They have their own sequence space; storing them alongside the
        // incremental stream would make every range lookup wrong.
        let mut s = DatagramStore::new(8, 4096);
        assert!(s.push(&snapshot_datagram(1)).is_err());
        assert!(s.is_empty());
    }

    #[test]
    fn a_datagram_that_does_not_decode_is_refused() {
        let mut s = DatagramStore::new(8, 4096);
        assert!(s.push(&[0u8; 4]).is_err());
    }

    #[test]
    fn the_ring_wraps_without_losing_order() {
        let s = filled(4, 20, 5); // 1..=100, only the last 4 datagrams held
        assert_eq!(s.len(), 4);
        assert_eq!(s.first_sequence(), 81);
        let found = s.locate(81, 100);
        assert_eq!(found.status, Status::Ok);
        assert_eq!(found.end - found.start, 4);

        // And the datagrams come back oldest first, in sequence order.
        let mut seen = Vec::new();
        for i in found.start..found.end {
            let d = s.datagram_at(i).unwrap();
            let h = wire::PacketHeaderDecoder::wrap(d).unwrap();
            seen.push(h.first_sequence());
        }
        assert_eq!(seen, vec![81, 86, 91, 96]);
    }

    #[test]
    fn every_message_in_a_located_range_is_actually_present() {
        // The property that makes a replay worth serving: no holes, nothing
        // missing at the edges.
        let s = filled(32, 20, 7); // 1..=140
        let found = s.locate(30, 100);
        assert_eq!(found.status, Status::Ok);

        let mut covered = Vec::new();
        for i in found.start..found.end {
            let d = s.datagram_at(i).unwrap();
            let h = wire::PacketHeaderDecoder::wrap(d).unwrap();
            for k in 0..u64::from(h.message_count()) {
                covered.push(h.first_sequence() + k);
            }
        }
        assert!(covered.first().is_some_and(|&f| f <= 30));
        assert!(covered.last().is_some_and(|&l| l >= 100));
        for (i, w) in covered.windows(2).enumerate() {
            assert_eq!(w[1], w[0] + 1, "gap at index {i}");
        }
    }
}
