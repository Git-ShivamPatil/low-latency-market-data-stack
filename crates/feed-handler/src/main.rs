//! The feed handler binary.
//!
//! ```text
//! cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001
//! ```
//!
//! Published on the case-study page, so the binary name and the flag names are
//! part of the public surface.
//!
//! # What this milestone does and does not do
//!
//! It reads both arms, discards duplicates, rebuilds the books, and reports what
//! it sees. It **notices** a sequence gap and says so.
//!
//! It does not yet *recover* from one. Proper A/B arbitration with a bounded
//! dedup window and an explicit SYNCING/LIVE/GAPPED state machine is milestone
//! 3, and rebuilding after a gap from a snapshot or a replay is milestone 4.
//! Until then a gap is reported and the stream is resumed past it, with the
//! books explicitly marked as no longer trustworthy — which is honest, and is
//! also exactly the hole the next two milestones exist to fill.

use std::io::{self, Write};
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use book::{apply_message, BookDigest, Books, DigestLog};
use clap::Parser;
use mdconfig::Config;
use transport::{is_timeout, Receiver, TransportMode};
use wire::{Message, PacketReader};

mod stats;

use crate::stats::HandlerStats;

#[derive(Parser, Debug)]
#[command(
    name = "feed-handler",
    about = "Consumes the A/B binary feed and rebuilds the order books",
    long_about = None,
)]
struct Args {
    /// Path to the configuration file.
    #[arg(long, default_value = "configs/local.toml")]
    config: PathBuf,

    /// The A channel: multicast group and port, or the local bind address in
    /// unicast-fanout mode.
    #[arg(long, value_name = "ADDR")]
    feed_a: Option<SocketAddrV4>,

    /// The B channel. B mirrors A; whichever datagram lands first wins.
    #[arg(long, value_name = "ADDR")]
    feed_b: Option<SocketAddrV4>,

    /// Override `transport.mode`: `multicast` or `unicast-fanout`.
    #[arg(long)]
    transport: Option<String>,

    /// Stop after consuming this many messages. 0 runs until interrupted.
    #[arg(long)]
    messages: Option<u64>,

    /// Give up if nothing arrives for this long. 0 waits forever.
    #[arg(long)]
    idle_timeout: Option<u64>,

    /// Write `sequence digest` checkpoints here.
    #[arg(long, value_name = "PATH")]
    digest_path: Option<PathBuf>,

    /// Checkpoint every N sequences. Must match the engine's interval.
    #[arg(long)]
    digest_interval: Option<u64>,

    /// Print the top of book for each symbol on exit.
    #[arg(long)]
    show_book: bool,

    /// Levels per side to print with `--show-book`.
    #[arg(long, default_value_t = 5)]
    depth: usize,

    /// Write a machine-readable `key=value` summary of the run here.
    #[arg(long, value_name = "PATH")]
    summary_path: Option<PathBuf>,
}

fn main() -> ExitCode {
    match run() {
        Ok(clean) => {
            if clean {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("feed-handler: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut cfg = Config::load(&args.config)?;

    let mode: TransportMode = match &args.transport {
        Some(s) => s
            .parse()
            .map_err(|e: String| -> Box<dyn std::error::Error> { e.into() })?,
        None => cfg.transport.mode()?,
    };

    // The advertised command passes group addresses directly, so a flag beats
    // the file. In unicast mode the same flag names the local bind address.
    let addr_a = args.feed_a.unwrap_or(match mode {
        TransportMode::Multicast => cfg.feed.a.group,
        TransportMode::UnicastFanout => cfg.feed.a.unicast_bind,
    });
    let addr_b = args.feed_b.unwrap_or(match mode {
        TransportMode::Multicast => cfg.feed.b.group,
        TransportMode::UnicastFanout => cfg.feed.b.unicast_bind,
    });

    if let Some(v) = args.messages {
        cfg.handler.messages = v;
    }
    if let Some(v) = args.idle_timeout {
        cfg.handler.idle_timeout_seconds = v;
    }
    if let Some(v) = args.digest_interval {
        cfg.handler.digest_interval = v;
    }
    if args.digest_path.is_some() {
        cfg.handler.digest_path = args.digest_path.clone();
    }

    let opts = cfg.socket_options();
    let a = Receiver::bind(mode, addr_a, opts).map_err(|e| bind_hint(e, mode, "A", addr_a))?;
    let b = Receiver::bind(mode, addr_b, opts).map_err(|e| bind_hint(e, mode, "B", addr_b))?;

    // Non-blocking so one quiet arm never stalls the other. A datagram sitting
    // in B's buffer while A is idle must not wait on A's read.
    a.set_nonblocking(true)?;
    b.set_nonblocking(true)?;

    eprintln!("feed-handler");
    eprintln!("  A: {}", a.describe());
    eprintln!("  B: {}", b.describe());
    if a.buffer_was_clamped(opts.buffer_bytes) {
        eprintln!(
            "  note: the kernel granted {} KiB of receive buffer, not the {} KiB requested.\n\
             \x20       Raise net.core.rmem_max if gaps appear under load — a full receive\n\
             \x20       buffer looks exactly like network loss and is not.",
            a.granted_recv_buffer() / 1024,
            opts.buffer_bytes / 1024
        );
    }

    let mut books = Books::new();
    let mut stats = HandlerStats::default();
    let mut digest_log = DigestLog::open(cfg.handler.digest_path.as_deref())?;
    let digest_interval = cfg.handler.digest_interval;

    // Datagrams are capped by config; a MTU-sized buffer with headroom is
    // allocated once and reused for every read.
    let mut buf = vec![0u8; cfg.feed.max_datagram_bytes.max(65_536)];

    let started = Instant::now();
    let idle_limit = (cfg.handler.idle_timeout_seconds > 0)
        .then(|| Duration::from_secs(cfg.handler.idle_timeout_seconds));
    let stats_interval = Duration::from_millis(cfg.handler.stats_interval_millis.max(1));
    let message_limit = cfg.handler.messages;

    let mut last_data = Instant::now();
    let mut last_report = Instant::now();
    let mut next_expected: Option<u64> = None;

    // Which arm is polled first alternates each pass. Draining A to empty
    // before ever looking at B would make A win every race on a quiet host and
    // make the first-arrival counts meaningless. Proper arbitration with a
    // bounded dedup window is milestone 3; this is just fairness.
    let mut poll_b_first = false;

    'outer: loop {
        let mut got_any = false;
        poll_b_first = !poll_b_first;
        let arms: [(u8, &Receiver); 2] = if poll_b_first {
            [(1u8, &b), (0u8, &a)]
        } else {
            [(0u8, &a), (1u8, &b)]
        };

        for (channel, sock) in arms {
            loop {
                let n = match sock.recv(&mut buf) {
                    Ok(n) => n,
                    Err(e) if is_timeout(&e) => break,
                    Err(e) => return Err(e.into()),
                };
                got_any = true;
                stats.datagrams[usize::from(channel)] += 1;
                stats.bytes += n as u64;

                let reader = match PacketReader::new(&buf[..n]) {
                    Ok(r) => r,
                    Err(e) => {
                        stats.bad_datagrams += 1;
                        eprintln!("  dropping a datagram on arm {channel}: {e}");
                        continue;
                    }
                };

                for m in reader.messages() {
                    let (seq, msg) = match m {
                        Ok(v) => v,
                        Err(e) => {
                            stats.bad_datagrams += 1;
                            eprintln!("  truncated datagram on arm {channel}: {e}");
                            break;
                        }
                    };

                    match next_expected {
                        None => {
                            // First message ever seen. Milestone 4 replaces this
                            // with a snapshot-driven start.
                            if seq != 1 {
                                eprintln!(
                                    "  joined mid-stream at sequence {seq}. The book will be \
                                     incomplete until the snapshot cycle lands in milestone 4 — \
                                     start this handler before the engine for a clean run."
                                );
                                stats.joined_mid_stream = true;
                            }
                            stats.first_sequence = seq;
                        }
                        Some(expected) if seq < expected => {
                            // The other arm already delivered this one.
                            stats.duplicates[usize::from(channel)] += 1;
                            continue;
                        }
                        Some(expected) if seq > expected => {
                            let missing = seq - expected;
                            stats.gaps += 1;
                            stats.messages_missed += missing;
                            eprintln!(
                                "  GAP: expected sequence {expected}, got {seq} ({missing} \
                                 missing). Recovery arrives in milestone 4; continuing with a \
                                 book that is no longer trustworthy."
                            );
                        }
                        Some(_) => {}
                    }

                    // Every path that reaches here has accepted `seq`, so the
                    // expectation advances in exactly one place.
                    next_expected = Some(seq + 1);
                    stats.first_arrivals[usize::from(channel)] += 1;
                    stats.messages += 1;
                    stats.last_sequence = seq;

                    if let Err(e) = apply_message(&mut books, &msg) {
                        // Only worth reporting once the book is supposed to be
                        // complete; after a gap these are the expected fallout.
                        if stats.gaps == 0 && !stats.joined_mid_stream {
                            stats.apply_errors += 1;
                            eprintln!("  sequence {seq} does not apply: {e}");
                        } else {
                            stats.apply_errors_after_gap += 1;
                        }
                    }
                    if let Message::Trade(t) = msg {
                        stats.trades += 1;
                        stats.shares_traded += u64::from(t.quantity());
                    }

                    if digest_interval > 0 && seq.is_multiple_of(digest_interval) {
                        digest_log.write(seq, BookDigest::of(&books))?;
                    }

                    if message_limit > 0 && stats.messages >= message_limit {
                        break 'outer;
                    }
                }
            }
        }

        let now = Instant::now();
        if got_any {
            last_data = now;
        } else {
            if let Some(limit) = idle_limit {
                if now.duration_since(last_data) >= limit {
                    eprintln!("  no data for {}s; stopping", limit.as_secs());
                    break;
                }
            }
            // Nothing to do. Sleeping beats spinning both cores on a 2-core box.
            std::thread::sleep(Duration::from_micros(200));
        }

        if now.duration_since(last_report) >= stats_interval {
            stats.report(started);
            last_report = now;
        }
    }

    digest_log.flush()?;
    stats.report(started);
    let final_digest = BookDigest::of(&books);
    eprintln!("  final book {final_digest}");
    if let Some(path) = args.summary_path.as_deref() {
        stats.write_summary(path, final_digest, started.elapsed().as_secs_f64())?;
    }

    if args.show_book {
        print_books(&books, &cfg, args.depth);
    }

    if let Err(e) = books.check_invariants() {
        eprintln!("  the rebuilt book is internally inconsistent: {e}");
        return Ok(false);
    }

    // A run that saw a gap, dropped a datagram, or failed to apply a message on
    // a complete stream did not do its job, and says so in its exit code.
    let clean = stats.gaps == 0 && stats.bad_datagrams == 0 && stats.apply_errors == 0;
    if !clean {
        eprintln!(
            "  NOT CLEAN: {} gaps, {} bad datagrams, {} messages that did not apply",
            stats.gaps, stats.bad_datagrams, stats.apply_errors
        );
    }
    Ok(clean)
}

fn print_books(books: &Books, cfg: &Config, depth: usize) {
    for (symbol_id, b) in books.iter() {
        let name = cfg.symbol_name(*symbol_id).unwrap_or("?");
        println!("\n{name} (symbol {symbol_id}) — {} orders resting", b.len());
        println!(
            "{:>14}  {:>10}  {:>6}     {:>6}  {:>10}  {:>14}",
            "bid", "qty", "n", "n", "qty", "ask"
        );
        let bids = b.levels(wire::Side::Bid, depth);
        let asks = b.levels(wire::Side::Ask, depth);
        for i in 0..depth.min(bids.len().max(asks.len())) {
            let bid = bids.get(i).map_or_else(
                || " ".repeat(34),
                |l| {
                    format!(
                        "{:>14}  {:>10}  {:>6}",
                        wire::format_price(l.price),
                        l.quantity,
                        l.order_count
                    )
                },
            );
            let ask = asks.get(i).map_or_else(String::new, |l| {
                format!(
                    "{:>6}  {:>10}  {:>14}",
                    l.order_count,
                    l.quantity,
                    wire::format_price(l.price)
                )
            });
            println!("{bid}     {ask}");
        }
    }
    let _ = io::stdout().flush();
}

fn bind_hint(e: io::Error, mode: TransportMode, arm: &str, addr: SocketAddrV4) -> String {
    let mut msg = format!("cannot open channel {arm} on {addr}: {e}");
    if mode == TransportMode::Multicast {
        msg.push_str(
            "\n  Joining a multicast group is the most common thing to fail here, \
             especially under WSL2 or in a container where the group lands on the \
             wrong interface. Set transport.interface in the config, or use \
             `--transport unicast-fanout`, which is a supported mode rather than \
             a workaround.",
        );
    }
    msg
}
