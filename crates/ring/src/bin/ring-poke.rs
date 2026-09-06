//! Drives one end of a ring, so a process in the other language can drive the
//! other end.
//!
//! ```text
//! ring-poke --path P --create --capacity 1024 --slot-size 128
//! ring-poke --path P --produce 10000 --symbol 7
//! ring-poke --path P --consume 10000 --symbol 7
//! ```
//!
//! The C++ twin is `cpp/ring/src/poke_main.cpp`, and the two derive their
//! expected field values by the same rule. That is what makes the interop test
//! able to say "message 6,231 was wrong" rather than only "the counts differ".
//!
//! The messages are real `NewOrder`s, so this checks two agreements at once:
//! that both languages put the ring's indices at the same offsets, and that
//! both put a message's fields at the same offsets. Pushing opaque bytes would
//! prove only the first — and the second is the one whose failure mode is a
//! plausible wrong number rather than an error.

use std::path::PathBuf;
use std::process::ExitCode;

use ring::{Consumer, Producer, Ring};
use wire::{encode_new_order, NewOrderDecoder, Side};

/// The same function the C++ side computes.
///
/// The fields are spread across the block rather than clustered at the front,
/// because an offset mistake in a trailing field is exactly the kind that a
/// lazier fixture would not notice.
struct Expected {
    client_order_id: u64,
    price: i64,
    quantity: u32,
    side: Side,
}

fn expected(i: u64) -> Expected {
    Expected {
        client_order_id: 0x1122_3344_0000_0000 + i,
        // Alternating sign, so a decoder that reads the price unsigned fails
        // rather than agreeing for half the run.
        price: if i.is_multiple_of(2) {
            1_000_000 + i as i64
        } else {
            -(1_000_000 + i as i64)
        },
        quantity: 100 + (i % 900) as u32,
        side: if i.is_multiple_of(3) {
            Side::Ask
        } else {
            Side::Bid
        },
    }
}

struct Args {
    path: PathBuf,
    create: bool,
    capacity: u32,
    slot_size: u32,
    produce: Option<u64>,
    consume: Option<u64>,
    symbol: u16,
    spin_ms: u64,
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: ring-poke --path P [--create --capacity N --slot-size N]\n\
         \x20                [--produce N] [--consume N] [--symbol N] [--spin-ms N]"
    );
    ExitCode::from(2)
}

fn parse() -> Option<Args> {
    let mut a = Args {
        path: PathBuf::new(),
        create: false,
        capacity: 1024,
        slot_size: 128,
        produce: None,
        consume: None,
        symbol: 7,
        spin_ms: 10_000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().unwrap_or_default();
        match arg.as_str() {
            "--path" => a.path = PathBuf::from(next()),
            "--create" => a.create = true,
            "--capacity" => a.capacity = next().parse().ok()?,
            "--slot-size" => a.slot_size = next().parse().ok()?,
            "--produce" => a.produce = Some(next().parse().ok()?),
            "--consume" => a.consume = Some(next().parse().ok()?),
            "--symbol" => a.symbol = next().parse().ok()?,
            "--spin-ms" => a.spin_ms = next().parse().ok()?,
            _ => return None,
        }
    }
    (!a.path.as_os_str().is_empty()).then_some(a)
}

fn main() -> ExitCode {
    let Some(args) = parse() else {
        return usage();
    };

    if args.create {
        if let Err(e) = Ring::create(&args.path, args.capacity, args.slot_size) {
            eprintln!("ring-poke: {e}");
            return ExitCode::FAILURE;
        }
        println!("created=1");
        println!("capacity={}", args.capacity);
        println!("slot_size={}", args.slot_size);
        if args.produce.is_none() && args.consume.is_none() {
            return ExitCode::SUCCESS;
        }
    }

    if let Some(want) = args.produce {
        match produce(&args, want) {
            Ok(()) => {}
            Err(code) => return code,
        }
    }
    if let Some(want) = args.consume {
        match consume(&args, want) {
            Ok(()) => {}
            Err(code) => return code,
        }
    }
    ExitCode::SUCCESS
}

fn produce(args: &Args, want: u64) -> Result<(), ExitCode> {
    let mut p: Producer = match Ring::open(&args.path) {
        Ok(r) => r.into_producer(),
        Err(e) => {
            eprintln!("ring-poke: {e}");
            return Err(ExitCode::FAILURE);
        }
    };
    let symbol = args.symbol;
    let mut sent = 0u64;
    let mut blocked = 0u64;
    // A bounded spin, not an unbounded one: a test that hangs when the other
    // side never starts fails a CI run by timeout and says nothing about why.
    let budget = args.spin_ms.saturating_mul(20_000);
    let mut spins = 0u64;
    while sent < want && spins < budget {
        let e = expected(sent);
        let r = p.push_with(|slot| {
            encode_new_order(slot, e.client_order_id, e.price, e.quantity, symbol, e.side).ok()
        });
        if r.is_err() {
            blocked += 1;
            spins += 1;
            continue;
        }
        sent += 1;
    }
    println!("produced={sent}");
    println!("blocked={blocked}");
    if sent != want {
        eprintln!("ring-poke: only produced {sent} of {want}");
        return Err(ExitCode::FAILURE);
    }
    Ok(())
}

fn consume(args: &Args, want: u64) -> Result<(), ExitCode> {
    let mut c: Consumer = match Ring::open(&args.path) {
        Ok(r) => r.into_consumer(),
        Err(e) => {
            eprintln!("ring-poke: {e}");
            return Err(ExitCode::FAILURE);
        }
    };
    let symbol = args.symbol;
    let mut got = 0u64;
    let mut mismatches = 0u64;
    let mut first_bad: i64 = -1;
    let budget = args.spin_ms.saturating_mul(20_000);
    let mut spins = 0u64;
    while got < want && spins < budget {
        let index = got;
        let seen = c.pop_with(|bytes| {
            let e = expected(index);
            let ok = NewOrderDecoder::wrap(bytes).is_ok_and(|d| {
                d.client_order_id() == e.client_order_id
                    && d.price() == e.price
                    && d.quantity() == e.quantity
                    && d.symbol_id() == symbol
                    && d.side().is_ok_and(|s| s == e.side)
            });
            if !ok {
                if first_bad < 0 {
                    first_bad = index as i64;
                }
                mismatches += 1;
            }
        });
        if seen.is_none() {
            spins += 1;
            continue;
        }
        got += 1;
    }
    println!("consumed={got}");
    println!("mismatches={mismatches}");
    println!("first_mismatch={first_bad}");
    if got != want || mismatches != 0 {
        eprintln!("ring-poke: consumed {got} of {want} with {mismatches} mismatch(es)");
        return Err(ExitCode::FAILURE);
    }
    Ok(())
}
