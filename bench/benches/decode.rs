//! `~100ns decode`, measured under a definition stated out loud.
//!
//! Run:
//!
//! ```text
//! scripts/bench.sh micro          # pinned, governor set, host gate first
//! cargo bench -p bench --bench decode   # raw, unpinned, not publishable
//! ```
//!
//! # What is being measured
//!
//! [`bench::decode_fields`]: the packet header, the message headers, and every
//! field of every message in one datagram. No book update, no syscall, no
//! arbitration. Divide by [`bench::BATCH`] for a per-message figure, and quote
//! the batch factor next to it or the number reads as something stronger than
//! it is.
//!
//! # What stops it being optimised away
//!
//! Both directions are closed. The input goes through `black_box` so the
//! compiler cannot constant-fold the corpus, and the output is a checksum
//! folded from every field — returned, `black_box`ed, and **pinned by a test**
//! in `bench::tests::decoding_actually_reads_every_field`. That test flips one
//! bit of the payload and requires the answer to change. A `black_box` that
//! stopped working would still compile; that test would not still pass.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use bench::{Corpus, BATCH};

fn decode(c: &mut Criterion) {
    let corpus = Corpus::build(256, 0xBE7C_0DE5);

    let mut group = c.benchmark_group("decode");
    // Throughput in messages, not datagrams: the per-message figure is the one
    // that gets quoted, so it is the one Criterion should report.
    group.throughput(Throughput::Elements(u64::from(BATCH)));

    group.bench_function("fields_per_datagram", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let d = &corpus.datagrams()[i % corpus.datagrams().len()];
            i += 1;
            black_box(bench::decode_fields(black_box(d)))
        })
    });

    // A single datagram, hot in L1. The optimistic number, and it is labelled
    // as such: it is not what a consumer taking datagrams off a socket sees,
    // because that one never has the same bytes twice.
    group.bench_function("fields_one_hot_datagram", |b| {
        let d = &corpus.datagrams()[0];
        b.iter(|| black_box(bench::decode_fields(black_box(d))))
    });

    group.finish();

    // The book update, measured from a warm book rather than an empty one. An
    // empty book has one level and no queue, which is the fastest the structure
    // will ever be and nothing like what it does in service.
    let mut group = c.benchmark_group("decode_and_apply");
    group.throughput(Throughput::Elements(u64::from(BATCH)));
    group.bench_function("fast_book", |b| {
        b.iter_batched_ref(
            || {
                let mut books = bench::fast_books();
                bench::warm(&mut books, &corpus, 64);
                books
            },
            |books| {
                let mut h = 0u64;
                for d in corpus.datagrams().iter().skip(64).take(16) {
                    h ^= bench::decode_and_apply(books, black_box(d));
                }
                black_box(h)
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(benches, decode);
criterion_main!(benches);
