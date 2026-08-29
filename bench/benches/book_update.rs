//! `~200ns book update`, and the number for the obvious implementation beside
//! it.
//!
//! # Why both books are benchmarked
//!
//! The claim is about a data structure, and a data-structure claim means nothing
//! on its own. "200ns" is a number; "200ns, where the `BTreeMap`-of-`VecDeque`
//! version of the same operations on the same corpus takes N" is a result.
//!
//! The reference book was kept for exactly this. It is the oracle the fast book
//! is differentially tested against — see `crates/book/tests/differential.rs` —
//! and it is the baseline the fast book is measured against. Deleting it after
//! milestone 5 would have thrown away both.
//!
//! # Warm books, not empty ones
//!
//! Every measurement here operates on a book already carrying orders. An empty
//! book has one price level and no queue to walk: it is the fastest either
//! structure will ever be, and it is nothing like what a consumer does. The
//! depth the corpus reaches is reported alongside the number, because the fast
//! book's cost depends on it and the reference book's depends on it much more.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use bench::{Corpus, BATCH};
use book::{BookSet, OrderBook};
use wire::Side;

const WARM_DATAGRAMS: usize = 128;
const MEASURED_DATAGRAMS: usize = 16;

fn book_update(c: &mut Criterion) {
    let corpus = Corpus::build(512, 0xBE7C_0DE5);

    // Reported so the number is attributable to a book of a stated depth.
    let depth = {
        let mut books = bench::fast_books();
        bench::warm(&mut books, &corpus, WARM_DATAGRAMS);
        bench::resting_orders(&books)
    };
    println!("book depth after {WARM_DATAGRAMS} datagrams: {depth} resting orders");

    let mut group = c.benchmark_group("book_update");
    group.throughput(Throughput::Elements(
        u64::from(BATCH) * MEASURED_DATAGRAMS as u64,
    ));

    group.bench_function("fast", |b| {
        b.iter_batched_ref(
            || {
                let mut books = bench::fast_books();
                bench::warm(&mut books, &corpus, WARM_DATAGRAMS);
                books
            },
            |books| {
                let mut h = 0u64;
                for d in corpus
                    .datagrams()
                    .iter()
                    .skip(WARM_DATAGRAMS)
                    .take(MEASURED_DATAGRAMS)
                {
                    h ^= bench::decode_and_apply(books, black_box(d));
                }
                black_box(h)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("reference", |b| {
        b.iter_batched_ref(
            || {
                let mut books = bench::reference_books();
                bench::warm(&mut books, &corpus, WARM_DATAGRAMS);
                books
            },
            |books| {
                let mut h = 0u64;
                for d in corpus
                    .datagrams()
                    .iter()
                    .skip(WARM_DATAGRAMS)
                    .take(MEASURED_DATAGRAMS)
                {
                    h ^= bench::decode_and_apply(books, black_box(d));
                }
                black_box(h)
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();

    // The single operations, isolated. Useful for attributing a regression to
    // one of them rather than to "the book", and the only place the touch-side
    // rescan after a level empties can be seen on its own.
    let mut ops = c.benchmark_group("book_op");
    ops.throughput(Throughput::Elements(1));

    ops.bench_function("add_fast", |b| {
        b.iter_batched_ref(
            || {
                let mut books = bench::fast_books();
                bench::warm(&mut books, &corpus, WARM_DATAGRAMS);
                (books, 10_000_000u64)
            },
            |(books, id)| {
                let book = books.get_or_create(bench::SYMBOL);
                *id += 1;
                black_box(OrderBook::add(
                    book,
                    black_box(*id),
                    Side::Bid,
                    black_box(bench::MID),
                    black_box(10),
                ))
            },
            BatchSize::LargeInput,
        )
    });

    ops.bench_function("delete_fast", |b| {
        b.iter_batched_ref(
            || {
                let mut books = bench::fast_books();
                bench::warm(&mut books, &corpus, WARM_DATAGRAMS);
                let book = books.get_or_create(bench::SYMBOL);
                // A batch of known-present ids to cancel, so the measured
                // operation is a hit rather than a miss.
                let mut ids = Vec::new();
                for i in 0..1024u64 {
                    let id = 20_000_000 + i;
                    OrderBook::add(book, id, Side::Bid, bench::MID, 10).unwrap();
                    ids.push(id);
                }
                (books, ids, 0usize)
            },
            |(books, ids, i)| {
                let book = books.get_or_create(bench::SYMBOL);
                let id = ids[*i % ids.len()];
                *i += 1;
                black_box(OrderBook::delete(book, black_box(id)).is_ok())
            },
            BatchSize::LargeInput,
        )
    });

    ops.finish();
}

criterion_group!(benches, book_update);
criterion_main!(benches);
