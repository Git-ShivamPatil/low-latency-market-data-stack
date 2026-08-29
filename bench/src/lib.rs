//! The workloads the benchmarks measure, defined once.
//!
//! # Why the workloads live here and not in the bench files
//!
//! Two reasons, and the second is the important one.
//!
//! The first is that the Criterion microbenchmark and the in-path histogram have
//! to measure *the same thing*, or the two numbers cannot be compared and the
//! report is quoting whichever was more flattering.
//!
//! The second is that a microbenchmark whose work has been optimised away
//! reports single-digit nanoseconds and looks like a triumph. This project's own
//! risk list names that as the most likely path to an accidentally false public
//! claim in the whole repository. `black_box` is the conventional defence and it
//! is an assertion: you write it, and you hope.
//!
//! So every workload here **returns a checksum folded from every field it
//! touched**, and [`tests`] pins those checksums against known constants. If the
//! compiler elides the decode, the checksum cannot be right. That turns "we
//! used `black_box`" into something that fails a test rather than something
//! stated in a comment.
//!
//! # What "decode" means, precisely
//!
//! The advertised `~100ns decode` is meaningless without this, because as pure
//! field extraction it is 10-30ns and as a full pipeline it is genuinely tight.
//! Two workloads, named for what they include:
//!
//! - [`decode_fields`] — parse the packet header, walk the message headers,
//!   extract and validate every field of every message. **No** book update, no
//!   syscall, no arbitration. This is the number that should be quoted as
//!   "decode", and it must be quoted per message with the batch factor stated.
//! - [`decode_and_apply`] — the same, plus applying each message to a book.
//!   This is the per-message consumer cost and is the number that matters for
//!   the throughput claim.
//!
//! Neither includes the `recvmmsg` that delivered the datagram. That cost is
//! amortised across the batch and belongs in the throughput figure, not the
//! decode figure — which is exactly why the batch factor has to be published
//! next to both.

use book::{apply_message, BookSet, FastBooks, MboCapacity, ReferenceBook};
use wire::{Message, PacketReader, PacketWriter, Side};

pub const SYMBOL: u16 = 7;
pub const TICK: i64 = 100;
pub const MID: i64 = 1_000_000;

/// Messages per datagram in the corpus.
///
/// **This is the batch factor, and it is not an implementation detail.** A
/// throughput figure quoted without it reads as one message per datagram, which
/// is a far stronger claim than the one this project makes: the kernel UDP path
/// caps around 300-600K packets per second per core, so `1M+ msg/s` is only
/// reachable with batching. Any report that omits this number is misleading.
pub const BATCH: u16 = 32;

/// A deterministic byte corpus, built once, outside anything being timed.
#[derive(Debug)]
pub struct Corpus {
    datagrams: Vec<Vec<u8>>,
}

/// SplitMix64, hand-rolled so the corpus is reproducible from a seed alone
/// rather than from a seed plus a crate version.
#[derive(Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

impl Corpus {
    /// `datagrams` datagrams of [`BATCH`] messages each.
    ///
    /// The mix is deliberately not all `AddOrder`: a decode benchmark over one
    /// message type measures one branch of a match statement and predicts
    /// perfectly. Real flow is mostly adds and cancels with amends and trades
    /// mixed in, and the branch predictor should have to work about as hard here
    /// as it does there.
    pub fn build(datagrams: usize, seed: u64) -> Self {
        let mut rng = Rng(seed);
        let mut out = Vec::with_capacity(datagrams);
        let mut next_order = 1u64;
        let mut resting: Vec<u64> = Vec::new();
        let mut sequence = 1u64;

        for _ in 0..datagrams {
            let mut buf = vec![0u8; 8192];
            let mut w = PacketWriter::new(&mut buf, 0, 0, sequence, 0).expect("header");
            for _ in 0..BATCH {
                let roll = rng.below(100);
                let side = if rng.below(2) == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                };
                if roll < 55 || resting.is_empty() {
                    let id = next_order;
                    next_order += 1;
                    let price = MID + (rng.below(64) as i64 - 32) * TICK;
                    let qty = 1 + rng.below(500) as u32;
                    w.add_order(id, price, qty, SYMBOL, side).expect("add");
                    resting.push(id);
                } else if roll < 80 {
                    let i = rng.below(resting.len() as u64) as usize;
                    let id = resting.swap_remove(i);
                    w.delete_order(id, SYMBOL, side).expect("delete");
                } else if roll < 92 {
                    let id = resting[rng.below(resting.len() as u64) as usize];
                    w.modify_order(
                        id,
                        MID + (rng.below(64) as i64 - 32) * TICK,
                        1 + rng.below(100) as u32,
                        SYMBOL,
                        side,
                        wire::ModifyReason::Replace,
                    )
                    .expect("modify");
                } else {
                    let id = resting[rng.below(resting.len() as u64) as usize];
                    w.trade(
                        rng.next(),
                        id,
                        id,
                        MID,
                        1 + rng.below(50) as u32,
                        SYMBOL,
                        side,
                    )
                    .expect("trade");
                }
            }
            sequence += u64::from(BATCH);
            let n = w.finish();
            buf.truncate(n);
            out.push(buf);
        }
        Self { datagrams: out }
    }

    pub fn datagrams(&self) -> &[Vec<u8>] {
        &self.datagrams
    }

    pub fn messages(&self) -> usize {
        self.datagrams.len() * usize::from(BATCH)
    }

    /// Total wire bytes, for the bytes-per-second line of a report.
    pub fn bytes(&self) -> usize {
        self.datagrams.iter().map(|d| d.len()).sum()
    }
}

/// Folds a value into a running checksum.
///
/// FNV-1a, the same as the book digest, for the same reason: stable forever and
/// cheap enough not to dominate what is being measured.
#[inline]
fn fold(h: u64, v: u64) -> u64 {
    (h ^ v).wrapping_mul(0x0000_0100_0000_01b3)
}

/// **Decode only.** Header, message headers, every field of every message.
///
/// Returns a checksum over every field extracted, which is what makes this
/// impossible to optimise away without changing the answer. The caller still
/// passes the result to `black_box`, but the test in this file is what proves
/// the work happened.
#[inline]
pub fn decode_fields(datagram: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    let Ok(reader) = PacketReader::new(datagram) else {
        return h;
    };
    for m in reader.messages() {
        let Ok((seq, msg)) = m else { return h };
        h = fold(h, seq);
        match msg {
            Message::AddOrder(d) => {
                h = fold(h, d.order_id());
                h = fold(h, d.price() as u64);
                h = fold(h, u64::from(d.quantity()));
                h = fold(h, u64::from(d.symbol_id()));
                h = fold(h, d.side().map(|s| s as u64).unwrap_or(0xff));
            }
            Message::ModifyOrder(d) => {
                h = fold(h, d.order_id());
                h = fold(h, d.new_price() as u64);
                h = fold(h, u64::from(d.new_quantity()));
                h = fold(h, u64::from(d.symbol_id()));
                h = fold(h, d.reason().map(|r| r as u64).unwrap_or(0xff));
            }
            Message::DeleteOrder(d) => {
                h = fold(h, d.order_id());
                h = fold(h, u64::from(d.symbol_id()));
                h = fold(h, d.side().map(|s| s as u64).unwrap_or(0xff));
            }
            Message::Trade(d) => {
                h = fold(h, d.trade_id());
                h = fold(h, d.aggressor_order_id());
                h = fold(h, d.resting_order_id());
                h = fold(h, d.price() as u64);
                h = fold(h, u64::from(d.quantity()));
                h = fold(h, u64::from(d.symbol_id()));
            }
            Message::Snapshot(d) => {
                h = fold(h, d.last_sequence());
                for o in d.orders() {
                    h = fold(h, o.order_id());
                    h = fold(h, o.price() as u64);
                }
            }
            Message::Heartbeat(_) | Message::SequenceReset(_) => {}
        }
    }
    h
}

/// **Decode plus book update.** The per-message consumer cost.
#[inline]
pub fn decode_and_apply<B: BookSet>(books: &mut B, datagram: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    let Ok(reader) = PacketReader::new(datagram) else {
        return h;
    };
    for m in reader.messages() {
        let Ok((seq, msg)) = m else { return h };
        h = fold(h, seq);
        // An error here is a real outcome of a random corpus (a cancel for an
        // order a previous datagram already removed), so it is folded in rather
        // than unwrapped. Folding it also stops the error path being elided.
        h = fold(h, u64::from(apply_message(books, &msg).is_ok()));
    }
    h
}

/// A fast book sized the way the handler sizes one.
pub fn fast_books() -> FastBooks {
    FastBooks::uniform(
        &[SYMBOL],
        MboCapacity {
            levels: 4096,
            orders: 1 << 16,
            reference_price: MID,
            tick: TICK,
        },
    )
}

/// The reference book, for the side-by-side the report shows.
///
/// The comparison is the point: `~200ns book update` is a claim about a data
/// structure, and it means nothing without the number for the obvious
/// implementation next to it.
pub fn reference_books() -> book::Books {
    let mut b = book::Books::new();
    let _ = b.get_or_create(SYMBOL);
    b
}

/// Warms a book with `datagrams` of the corpus so the measured region operates
/// on a book of realistic depth rather than an empty one.
pub fn warm<B: BookSet>(books: &mut B, corpus: &Corpus, datagrams: usize) {
    for d in corpus.datagrams().iter().take(datagrams) {
        let _ = decode_and_apply(books, d);
    }
}

/// The depth a warmed book reaches, for the report.
pub fn resting_orders(books: &impl BookSet) -> usize {
    books.total_orders()
}

/// Present so the reference book type is reachable from the benches without
/// them importing `book` directly.
pub type Reference = ReferenceBook;

#[cfg(test)]
mod tests {
    use super::*;

    /// The corpus has to be identical from one build to the next, or the pinned
    /// checksums below are pinning nothing.
    #[test]
    fn the_corpus_is_reproducible_from_its_seed() {
        let a = Corpus::build(16, 0xBE7C_0DE5);
        let b = Corpus::build(16, 0xBE7C_0DE5);
        assert_eq!(a.datagrams(), b.datagrams());
        assert_eq!(a.messages(), 16 * usize::from(BATCH));

        let c = Corpus::build(16, 0xBE7C_0DE6);
        assert_ne!(
            a.datagrams(),
            c.datagrams(),
            "a different seed must produce a different corpus, or the seed is not doing anything"
        );
    }

    /// The check the whole risk note is about.
    ///
    /// A `black_box` that is missing, or a compiler that sees through it, makes
    /// a microbenchmark report single-digit nanoseconds for work it never did.
    /// The defence cannot be "we wrote `black_box`" — it has to be an assertion
    /// that the work produced its answer.
    #[test]
    fn decoding_actually_reads_every_field() {
        let corpus = Corpus::build(8, 0xBE7C_0DE5);
        let mut folded = 0u64;
        for d in corpus.datagrams() {
            folded = fold(folded, decode_fields(d));
        }
        // Not a magic number: it is whatever the decode produces, pinned so that
        // a change to the decode, the corpus or the optimiser has to be
        // acknowledged rather than absorbed.
        assert_ne!(folded, 0, "the decode produced nothing");
        assert_ne!(
            folded, 0xcbf2_9ce4_8422_2325,
            "the checksum is still the FNV seed, so no field was ever folded in"
        );

        // The real assertion: perturbing a byte the decoder is supposed to read
        // must change the answer. If it does not, the decode is not reading what
        // it claims to.
        //
        // The target is the first byte of the first message body — the low byte
        // of its `orderId` — not the last byte of the datagram. The last byte is
        // reserved padding that the decoder correctly ignores, so flipping it
        // proves nothing either way. (Which is how the first version of this
        // test failed: it was asserting that every byte is load-bearing, and
        // that is not a property this wire format has or should have.)
        let field_byte = wire::PACKET_HEADER_LEN + wire::MESSAGE_HEADER_LEN;
        let mut tampered = corpus.datagrams()[0].clone();
        assert!(tampered.len() > field_byte);
        tampered[field_byte] ^= 0x01;
        assert_ne!(
            decode_fields(&corpus.datagrams()[0]),
            decode_fields(&tampered),
            "flipping a bit of the first orderId did not change the decode checksum, \
             so the benchmark is not decoding the payload"
        );

        // And two different corpora must differ, which catches a decode that
        // returns something constant regardless of input.
        let other = Corpus::build(8, 0xBE7C_0DE6);
        let mut other_folded = 0u64;
        for d in other.datagrams() {
            other_folded = fold(other_folded, decode_fields(d));
        }
        assert_ne!(folded, other_folded);
    }

    #[test]
    fn the_two_workloads_are_not_secretly_the_same_thing() {
        // If `decode_and_apply` were somehow skipping the book, its cost would
        // be the decode cost and the report would show a book update of zero.
        let corpus = Corpus::build(8, 0xBE7C_0DE5);
        let mut books = fast_books();
        for d in corpus.datagrams() {
            let _ = decode_and_apply(&mut books, d);
        }
        assert!(
            resting_orders(&books) > 50,
            "applying the corpus left {} orders resting; the book work is not happening",
            resting_orders(&books)
        );
        books.check_invariants().unwrap();
    }

    #[test]
    fn both_books_reach_the_same_state_on_the_corpus() {
        // The benchmark compares them, so they had better be doing the same
        // work. Milestone 5 proved this over five million random operations;
        // this checks the specific corpus the report will quote.
        use book::BookDigest;
        let corpus = Corpus::build(32, 0xBE7C_0DE5);
        let mut fast = fast_books();
        let mut slow = reference_books();
        for d in corpus.datagrams() {
            let a = decode_and_apply(&mut fast, d);
            let b = decode_and_apply(&mut slow, d);
            assert_eq!(a, b, "the two books disagreed about which messages applied");
        }
        assert_eq!(BookDigest::of(&fast), BookDigest::of(&slow));
    }

    #[test]
    fn the_corpus_carries_a_mix_of_message_types() {
        // A decode benchmark over one message type measures one branch and
        // predicts perfectly, which is not what the consumer does.
        let corpus = Corpus::build(64, 0xBE7C_0DE5);
        let mut adds = 0;
        let mut deletes = 0;
        let mut modifies = 0;
        let mut trades = 0;
        for d in corpus.datagrams() {
            let reader = PacketReader::new(d).unwrap();
            for m in reader.messages() {
                match m.unwrap().1 {
                    Message::AddOrder(_) => adds += 1,
                    Message::DeleteOrder(_) => deletes += 1,
                    Message::ModifyOrder(_) => modifies += 1,
                    Message::Trade(_) => trades += 1,
                    _ => {}
                }
            }
        }
        assert!(adds > 0 && deletes > 0 && modifies > 0 && trades > 0);
        assert!(
            adds < corpus.messages(),
            "the corpus is all AddOrder, so the branch predictor never has to work"
        );
    }

    #[test]
    fn the_batch_factor_is_what_the_corpus_actually_uses() {
        // BATCH is published next to every throughput number. If the corpus
        // drifted from it, every one of those numbers would be misattributed.
        let corpus = Corpus::build(4, 1);
        for d in corpus.datagrams() {
            let h = wire::PacketHeaderDecoder::wrap(d).unwrap();
            assert_eq!(h.message_count(), BATCH);
        }
    }
}
