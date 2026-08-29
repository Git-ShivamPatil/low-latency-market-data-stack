//! The feed handler binary.
//!
//! ```text
//! cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001
//! ```
//!
//! Published on the case-study page, so the binary name and the flag names are
//! part of the public surface.
//!
//! # What this does and does not do
//!
//! It arbitrates between the two arms — taking whichever datagram arrives first,
//! holding out-of-order ones in a bounded reorder window until the hole ahead of
//! them fills, and telling *late* from *lost*. A datagram lost on one arm costs
//! nothing. One lost on both is named as a range and the feed moves to `GAPPED`.
//! See `feed_handler::arbitration` for why the window is bounded and why it holds datagrams
//! rather than messages.
//!
//! When a range really is lost on both arms it recovers: live traffic is
//! buffered, the next snapshot cycle replaces the books wholesale, and the
//! buffer is replayed on top of it minus whatever the snapshot already covered.
//! See `feed_handler::recovery` for why that reconciliation is the hard part.

use std::io::{self, Write};
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use alloc_guard::{AllocCounts, AllocGuard, CountingAllocator};
use book::{
    apply_message, BookDigest, BookSet, Books, DigestLog, FastBooks, MboCapacity, OrderBook,
};

/// Installed unconditionally, not only under `--verify-allocations`.
///
/// A flag that changed the allocator would measure a different binary from the
/// one that ships, which is the same mistake as benchmarking a debug build. The
/// cost is one thread-local increment per allocation — and in steady state the
/// whole point is that there are none, so it costs nothing where it matters.
#[global_allocator]
static ALLOC: CountingAllocator<std::alloc::System> = CountingAllocator::new(std::alloc::System);
use clap::Parser;
use mdconfig::Config;
use transport::{is_timeout, Receiver, TransportMode};
use wire::{Message, PacketReader};

use feed_handler::arbitration::{Accepted, Arbitrator, FeedState};
use feed_handler::recovery::RecoveryBuffer;
use feed_handler::stats::HandlerStats;
use replay_service::{protocol::Status, RangeRequest, ReplayResult};

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

    /// Discard this fraction of received datagrams, per arm, independently.
    ///
    /// This is loss on the *consumer's* side of the wire, which is what the
    /// case study's recovery step exercises: it needs no cooperation from the
    /// publisher, so the recovery path can be demonstrated against a feed you do
    /// not control. Each arm decides separately, so at rate p roughly p² of
    /// datagrams are lost on both and become real gaps — which is the point.
    #[arg(long, value_name = "RATE")]
    drop_rate: Option<f64>,

    /// Seed for the input-loss injector, so a demonstration replays exactly.
    #[arg(long, default_value_t = 0x0D_1A_0F_5E_ED_00_00_01)]
    drop_seed: u64,

    /// Which book implementation to rebuild into.
    ///
    /// Both are kept and both are tested. `reference` is the obviously-correct
    /// one the fast book is differentially tested against; the smoke test runs
    /// the reconciliation against each in turn, so a divergence between them
    /// shows up as a cross-process digest mismatch rather than only in a unit
    /// test.
    #[arg(long, value_enum, default_value_t = BookKind::Fast)]
    books: BookKind,

    /// Count heap operations in the steady-state receive loop and report the
    /// total.
    ///
    /// Reports; it does not assert. The assertion lives in
    /// `tests/allocation.rs`, where CI runs it — a claim that depends on
    /// somebody remembering to pass a flag is not a claim.
    #[arg(long)]
    verify_allocations: bool,

    /// Ask a replay service at this address to fill a gap exactly, instead of
    /// waiting for the next snapshot cycle.
    ///
    /// Replay recovers the *messages*; a snapshot recovers only the state they
    /// produced. Falls back to the snapshot cycle when the service is down or
    /// the range has aged out of its history.
    #[arg(long, value_name = "ADDR")]
    replay: Option<String>,
}

/// SplitMix64, for the input-loss injector.
///
/// Deliberately not shared with the engine's: this models the network in front
/// of *this* consumer, and coupling the two would make a handler's losses depend
/// on what the publisher happened to be doing.
struct DropRng(u64);

impl DropRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        ((z >> 11) as f64 / (1u64 << 53) as f64) < p
    }
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
    let cfg = Config::load(&args.config)?;
    match args.books {
        BookKind::Reference => run_with(args, cfg, Books::new()),
        BookKind::Fast => {
            // Sized from the config, per symbol: each has its own reference
            // price and tick, and a single shared window would be wrong for
            // every symbol but one. This is the allocation the milestone puts
            // outside the claim, and it happens here, once, before a byte is
            // received.
            let specs: Vec<(u16, MboCapacity)> = cfg
                .market
                .symbols
                .iter()
                .map(|sym| {
                    (
                        sym.id,
                        MboCapacity {
                            levels: cfg.handler.book_levels,
                            orders: cfg.handler.book_orders,
                            reference_price: sym.reference_price,
                            tick: sym.tick_size,
                        },
                    )
                })
                .collect();
            let fallback = specs
                .first()
                .map(|(_, c)| *c)
                .unwrap_or_else(book::default_capacity);
            let books = FastBooks::with_symbols(&specs, fallback);
            run_with(args, cfg, books)
        }
    }
}

fn run_with<B: BookSet>(
    args: Args,
    cfg: Config,
    mut books: B,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut cfg = cfg;

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

    let mut arbitrator = Arbitrator::new(
        cfg.handler.reorder_window_datagrams,
        cfg.feed.max_datagram_bytes.max(65_536),
    );
    let mut started_stream = false;
    // Whether a snapshot cycle is currently being consumed. A cycle spans
    // several datagrams and replaces every book, so joining it partway is not
    // allowed — see `handle_snapshot`.
    let mut in_snapshot_cycle = false;
    let mut recovery = RecoveryBuffer::new(
        cfg.handler.recovery_buffer_datagrams,
        cfg.feed.max_datagram_bytes.max(65_536),
        Duration::from_millis(cfg.handler.recovery_timeout_millis),
    );

    let input_drop_rate = args.drop_rate.unwrap_or(0.0).clamp(0.0, 1.0);
    let mut drop_rng = DropRng::new(args.drop_seed);
    let mut input_dropped = [0u64; 2];
    if input_drop_rate > 0.0 {
        eprintln!(
            "  discarding {:.2}% of received datagrams per arm (seed {})",
            input_drop_rate * 100.0,
            args.drop_seed
        );
    }
    let mut allocs = AllocProbe::new(args.verify_allocations);
    if args.verify_allocations {
        eprintln!(
            "  counting heap operations in the receive loop after {ALLOC_WARMUP_MESSAGES} \
                messages of warm-up"
        );
    }

    // Replay is optional: without it, recovery waits for the snapshot cycle.
    let replay_addr: Option<std::net::SocketAddr> = args
        .replay
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| Some(cfg.replay.request_connect.clone()).filter(|s| !s.is_empty()))
        .map(|t| {
            t.parse::<std::net::SocketAddr>()
                .map_err(|e| format!("--replay {t:?} is not an address: {e}"))
        })
        .transpose()?;
    if let Some(addr) = replay_addr {
        eprintln!("  replay service -> {addr}");
    }
    let replay_timeout = Duration::from_millis(cfg.replay.request_timeout_millis.max(1));
    // At most one request outstanding: a second would race the first to fill the
    // same hole, and the two answers could disagree about how much they cover.
    let mut pending_replay: Option<std::sync::mpsc::Receiver<io::Result<ReplayResult>>> = None;
    let mut last_replay_from: Option<u64> = None;

    let mut last_data = Instant::now();
    let mut last_report = Instant::now();

    // Which arm is polled first alternates each pass. Draining A to empty before
    // ever looking at B would make A win every race on a quiet host and make the
    // first-arrival counts meaningless.
    let mut poll_b_first = false;
    // ...and each pass reads at most this many datagrams from an arm before
    // switching.
    //
    // Without the cap, a handler that has fallen behind reads everything queued
    // on one arm before touching the other, so the two arms drift apart by the
    // whole socket backlog. Every datagram that arm lost then has to be held
    // until the *other* arm is finally read, and the reorder window fills with
    // traffic that was never actually out of order. Measured on a 2-core box:
    // uncapped, the window peaked at 56 of 64 on an otherwise healthy run —
    // close enough to the bound to start inventing gaps under any extra load.
    const READS_PER_ARM_PER_PASS: usize = 16;
    // How long a hole may stay open on a quiet feed before it is declared lost.
    // Without this a stalled publisher would leave buffered messages held
    // forever, waiting for a datagram that is not coming.
    let gap_timeout = Duration::from_millis(cfg.handler.gap_timeout_millis.max(1));

    'outer: loop {
        let alloc_scope = allocs.begin(arbitrator.messages_delivered());
        let mut got_any = false;
        poll_b_first = !poll_b_first;
        let arms: [(u8, &Receiver); 2] = if poll_b_first {
            [(1u8, &b), (0u8, &a)]
        } else {
            [(0u8, &a), (1u8, &b)]
        };

        for (channel, sock) in arms {
            for _ in 0..READS_PER_ARM_PER_PASS {
                let n = match sock.recv(&mut buf) {
                    Ok(n) => n,
                    Err(e) if is_timeout(&e) => break,
                    Err(e) => return Err(e.into()),
                };
                got_any = true;

                // Simulated loss on this consumer's input, before anything else
                // sees the datagram. Dropping it here rather than after decoding
                // is what makes it indistinguishable from a real network loss.
                if input_drop_rate > 0.0 && drop_rng.chance(input_drop_rate) {
                    input_dropped[usize::from(channel)] += 1;
                    continue;
                }

                // Snapshot datagrams live in their own sequence space and must
                // never reach the arbitrator, which tracks the incremental one.
                // Routing on the flag is what keeps the two streams from being
                // mistaken for each other.
                if is_snapshot_datagram(&buf[..n]) {
                    handle_snapshot(
                        &buf[..n],
                        &mut recovery,
                        &mut books,
                        &mut arbitrator,
                        &mut stats,
                        &mut digest_log,
                        digest_interval,
                        &mut in_snapshot_cycle,
                    )?;
                    continue;
                }

                // Arbitration decides what, if anything, this datagram
                // contributes. Everything downstream sees one ordered stream.
                let outcome = arbitrator.accept(channel, &buf[..n]);
                match outcome {
                    Accepted::Ready {
                        first_sequence,
                        count,
                    } => {
                        if !started_stream {
                            started_stream = true;
                            stats.first_sequence = first_sequence;
                            if first_sequence != 1 {
                                eprintln!(
                                    "  joined mid-stream at sequence {first_sequence}; waiting \
                                     for a snapshot to build a book that can be trusted."
                                );
                                stats.joined_mid_stream = true;
                                // A mid-stream join is a gap by another name:
                                // everything before this point is missing.
                                recovery.begin(1, Instant::now());
                            }
                        }
                        if recovery.is_recovering() {
                            // Live traffic during a recovery is held, not
                            // applied. It will be replayed on top of the
                            // snapshot, minus whatever the snapshot covers.
                            if let Err(e) = recovery.hold(first_sequence, count, &buf[..n]) {
                                eprintln!("  recovery failed: {e}");
                                recovery.fail();
                                stats.recovery_failures += 1;
                            }
                        } else {
                            let gapped = arbitrator.state() == FeedState::Gapped;
                            consume(
                                &buf[..n],
                                &mut books,
                                &mut stats,
                                &mut digest_log,
                                digest_interval,
                                gapped,
                                0,
                            )?;
                        }
                        if reached_limit(&arbitrator, &recovery, message_limit) {
                            break 'outer;
                        }
                    }
                    Accepted::ForcedGap(gap) => {
                        eprintln!(
                            "  GAP: sequence {gap} was lost on both arms. Buffering live \
                             traffic and waiting for a snapshot."
                        );
                        recovery.begin(gap.from, Instant::now());
                    }
                    Accepted::Buffered | Accepted::Duplicate => {}
                    Accepted::Malformed(e) => {
                        stats.bad_datagrams += 1;
                        eprintln!("  dropping a datagram on arm {channel}: {e}");
                    }
                }

                // Whatever the hole filling just unblocked, in sequence order.
                // `books` and `arbitrator` are separate bindings, so the closure
                // can borrow one while the other is borrowed mutably.
                if recovery.is_recovering() {
                    // The arbitrator still has to be drained during a recovery,
                    // or its window fills with traffic nobody is consuming and
                    // the next gap has nowhere to go. What changes is the
                    // destination: held for replay, not applied to the books.
                    let mut overflow = None;
                    arbitrator.drain_ready(|first, count, bytes| {
                        if overflow.is_none() {
                            if let Err(e) = recovery.hold(first, count, bytes) {
                                overflow = Some(e);
                            }
                        }
                    });
                    if let Some(e) = overflow {
                        eprintln!("  recovery failed: {e}");
                        recovery.fail();
                        stats.recovery_failures += 1;
                    }
                } else {
                    drain_into_books(
                        &mut arbitrator,
                        &mut books,
                        &mut stats,
                        &mut digest_log,
                        digest_interval,
                    )?;
                }
                if reached_limit(&arbitrator, &recovery, message_limit) {
                    break 'outer;
                }
            }
        }

        // Poll the outstanding replay request, if there is one. Deliberately off
        // the receive path: a synchronous request would stop this loop reading
        // its sockets, and a handler that stops reading loses datagrams — which
        // is how recovering from one gap manufactures the next.
        if let Some(rx) = pending_replay.as_ref() {
            match rx.try_recv() {
                Ok(Ok(r)) if r.is_ok() => {
                    pending_replay = None;
                    // The answer may have arrived after a snapshot already
                    // closed this gap, in which case applying it would re-apply
                    // messages the book already has. `apply_replay` checks and
                    // says so rather than trusting the caller to remember.
                    if !apply_replay(
                        &r,
                        &mut recovery,
                        &mut arbitrator,
                        &mut books,
                        &mut stats,
                        &mut digest_log,
                        digest_interval,
                    )? {
                        eprintln!(
                            "  a replay of {}..={} arrived after the gap was already closed; \
                                discarded",
                            r.request.from, r.request.through
                        );
                    }
                }
                Ok(Ok(r)) => {
                    pending_replay = None;
                    eprintln!(
                        "  replay refused for {}..={}: {} (the service holds {}..{}); \
                         falling back to the snapshot cycle",
                        r.request.from,
                        r.request.through,
                        r.header.status,
                        r.header.first_available,
                        r.header.last_available
                    );
                    recovery.note_replay_refused();
                }
                Ok(Err(e)) => {
                    pending_replay = None;
                    eprintln!("  replay request failed: {e}; falling back to the snapshot cycle");
                    recovery.note_replay_refused();
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    pending_replay = None;
                    recovery.note_replay_refused();
                }
            }
        }

        // A recovery that has just begun, with a replay service available, asks
        // for the exact range rather than waiting for the next cycle.
        if pending_replay.is_none() {
            if let (Some(addr), Some(from)) = (replay_addr, recovery.gap_from()) {
                let through = arbitrator.next_expected().saturating_sub(1);
                // Keyed on the outstanding hole rather than the attempt number:
                // a recovery that reopened after a partial replay has a new
                // `gap_from` and needs a fresh request, but is still the same
                // attempt.
                if through >= from && last_replay_from != Some(from) {
                    last_replay_from = Some(from);
                    eprintln!("  requesting replay of {from}..={through}");
                    pending_replay = Some(replay_service::request_in_background(
                        addr,
                        RangeRequest { from, through },
                        replay_timeout,
                        cfg.feed.max_datagram_bytes.max(65_536),
                    ));
                }
            }
        }

        let now = Instant::now();
        if got_any {
            last_data = now;
        } else {
            // Drain first. A quiet period with traffic still buffered usually
            // means it is simply waiting to be applied, not that anything is
            // missing — and asking about a stall before draining is how a
            // non-hole gets mistaken for one.
            if !recovery.is_recovering() {
                drain_into_books(
                    &mut arbitrator,
                    &mut books,
                    &mut stats,
                    &mut digest_log,
                    digest_interval,
                )?;
            }

            // A hole that has stayed open through a quiet period is not late
            // any more.
            if now.duration_since(last_data) >= gap_timeout {
                if let Some(gap) = arbitrator.declare_gap_if_stalled() {
                    eprintln!(
                        "  GAP: sequence {gap} did not arrive within {}ms of the feed going \
                            quiet. Buffering live traffic and waiting for a snapshot.",
                        gap_timeout.as_millis()
                    );
                    // A gap found this way is exactly as real as one found by a
                    // full window, and needs recovering the same way. Declaring
                    // it here without starting recovery left the handler stuck
                    // in GAPPED with a stale book for the rest of the run.
                    recovery.begin(gap.from, now);
                }
            }
            // A recovery that has run out of time has failed, and saying so is
            // the point: a handler that waited forever would look healthy while
            // holding a book it knows is wrong.
            if let Err(e) = recovery.check_deadline(now) {
                eprintln!("  recovery failed: {e}");
                recovery.fail();
                stats.recovery_failures += 1;
            }
            if let Some(limit) = idle_limit {
                if now.duration_since(last_data) >= limit {
                    eprintln!("  no data for {}s; stopping", limit.as_secs());
                    break;
                }
            }
            // Nothing to do. Sleeping beats spinning both cores on a 2-core box.
            std::thread::sleep(Duration::from_micros(200));
        }

        // Ended before the periodic report, which writes to stderr and is
        // diagnostic output rather than part of the consume path.
        allocs.end(alloc_scope, arbitrator.messages_delivered());

        if now.duration_since(last_report) >= stats_interval {
            stats.report(&arbitrator, started);
            last_report = now;
        }
    }

    digest_log.flush()?;
    stats.report(&arbitrator, started);
    let final_digest = BookDigest::of(&books);
    eprintln!("  final book {final_digest}");
    if input_drop_rate > 0.0 {
        eprintln!(
            "  discarded {} datagrams on A and {} on B before decoding",
            input_dropped[0], input_dropped[1]
        );
    }
    for gap in arbitrator.gaps() {
        eprintln!("  gap: sequence {gap}");
    }
    if arbitrator.gap_count() > arbitrator.gaps().len() as u64 {
        eprintln!(
            "  ... and {} more not listed",
            arbitrator.gap_count() - arbitrator.gaps().len() as u64
        );
    }
    if let Some(path) = args.summary_path.as_deref() {
        stats.write_summary(
            path,
            &arbitrator,
            final_digest,
            started.elapsed().as_secs_f64(),
        )?;
        stats.write_recovery(path, recovery.stats(), arbitrator.resyncs())?;
        allocs.write_summary(std::path::Path::new(path), args.books)?;
    }
    allocs.report(args.books);

    if args.show_book {
        print_books(&books, &cfg, args.depth);
    }

    if let Err(e) = books.check_invariants() {
        eprintln!("  the rebuilt book is internally inconsistent: {e}");
        return Ok(false);
    }

    // A run that ended GAPPED, dropped a datagram, or failed to apply a message
    // on a complete stream did not do its job, and says so in its exit code.
    let clean = stats.is_clean(&arbitrator, recovery.is_recovering());
    if !clean {
        eprintln!(
            "  NOT CLEAN: state {}, {} gaps ({} recovered, {} failed), still recovering: {}, \
                {} bad datagrams, {} messages that did not apply",
            arbitrator.state(),
            arbitrator.gap_count(),
            stats.recoveries,
            stats.recovery_failures,
            recovery.is_recovering(),
            stats.bad_datagrams,
            stats.apply_errors
        );
    }
    Ok(clean)
}

/// Whether the run has consumed as much of the stream as it was asked to.
///
/// Counts what the arbitrator *accounted for*, not what reached the books. While
/// a recovery is in progress nothing reaches the books, so a limit on applied
/// messages would never fire and a bounded run would only end on its idle
/// timeout — which looks like a hang and reports the wrong thing.
fn reached_limit(arbitrator: &Arbitrator, recovery: &RecoveryBuffer, limit: u64) -> bool {
    // Never stop mid-recovery. Finishing it is part of consuming the stream, and
    // a run that stopped here would report GAPPED for a hole it was about to
    // close. The idle timeout is the backstop for a recovery that never lands.
    limit > 0 && !recovery.is_recovering() && arbitrator.messages_delivered() >= limit
}

/// Applies a served replay: the missing messages themselves, in order, followed
/// by the live traffic held while waiting.
///
/// Unlike a snapshot this does **not** discard the book. The hole is filled where
/// it is, so everything before it stays, and everything held after it is replayed
/// in full because none of it was covered by anything.
fn apply_replay<B: BookSet>(
    result: &ReplayResult,
    recovery: &mut RecoveryBuffer,
    arbitrator: &mut Arbitrator,
    books: &mut B,
    stats: &mut HandlerStats,
    digest_log: &mut DigestLog,
    digest_interval: u64,
) -> io::Result<bool> {
    debug_assert_eq!(result.header.status, Status::Ok);
    // Nothing is applied until the recovery is confirmed still outstanding: the
    // request ran on another thread and a snapshot may have closed the gap while
    // it was in flight.
    if !recovery.is_recovering() {
        return Ok(false);
    }
    let mut messages = 0u64;
    // The highest sequence the replay actually delivered, which is NOT the same
    // as the range that was asked for.
    //
    // The service serves whole datagrams, so the first can begin before the hole
    // did and the last can end past it. `skip_below` handles the leading
    // overlap. The trailing overlap is subtler and was a real bug: those
    // messages get applied here *and* sit in the held buffer, so reconciling
    // from the requested `through` replayed them a second time and left the book
    // with orders the publisher never added twice.
    let mut covered_through = result.request.through;
    for datagram in &result.datagrams {
        consume(
            datagram,
            books,
            stats,
            digest_log,
            digest_interval,
            false,
            result.request.from,
        )?;
        if let Ok(h) = wire::PacketHeaderDecoder::wrap(datagram) {
            messages += u64::from(h.message_count());
            let end = h.first_sequence() + u64::from(h.message_count());
            covered_through = covered_through.max(end.saturating_sub(1));
        }
    }

    // A replay covers the range that was asked for, and that range was fixed
    // when the request went out. If another gap opened while it was in flight,
    // the held traffic does not start where the replay ended — and completing
    // here would leave that hole in the book while reporting success. This is
    // the bug that produced "order N is not on the book" two hundred messages
    // after a recovery that looked clean.
    //
    // The check is for a hole *anywhere* in the held traffic, not just at its
    // front. An earlier version compared only the first held sequence, which
    // catches the case where the new gap opens immediately and misses the one
    // where it opens a few datagrams later — and the second is the common one,
    // because a replay that is slow enough for another gap to open is usually
    // slow enough for some traffic to arrive first.
    if let Some((from, through)) = recovery.held_discontinuity(covered_through + 1) {
        eprintln!(
            "  replay closed {}..={covered_through} but {from}..={through} opened while it \
             was in flight; staying in recovery",
            result.request.from
        );
        // The held traffic before the new hole is applied and discarded now.
        // Leaving it buffered loses it: the next adoption reconciles from the
        // far side of the hole and skips every slot below that as covered.
        let mut failure: Option<io::Error> = None;
        recovery.drain_contiguous(covered_through + 1, |skip_below, bytes| {
            if failure.is_some() {
                return;
            }
            if let Err(e) = consume(
                bytes,
                books,
                stats,
                digest_log,
                digest_interval,
                false,
                skip_below,
            ) {
                failure = Some(e);
            }
        });
        if let Some(e) = failure {
            return Err(e);
        }
        recovery.reopen(from);
        return Ok(false);
    }

    if !recovery.adopt_replay(covered_through, messages) {
        return Ok(false);
    }

    let mut failure: Option<io::Error> = None;
    recovery.replay(|skip_below, bytes| {
        if failure.is_some() {
            return;
        }
        if let Err(e) = consume(
            bytes,
            books,
            stats,
            digest_log,
            digest_interval,
            false,
            skip_below,
        ) {
            failure = Some(e);
        }
    });
    if let Some(e) = failure {
        return Err(e);
    }

    // The hole was filled where it was, so the frontier and the window are
    // already right — but the arbitrator is still flagged Gapped from when it
    // reported the range, and nothing else will clear it.
    arbitrator.clear_gapped();

    let elapsed = recovery.complete(Instant::now());
    stats.recoveries += 1;
    eprintln!(
        "  RECOVERED in {}ms by replaying {}..={}; the book was never discarded",
        elapsed.as_millis(),
        result.request.from,
        result.request.through
    );
    Ok(true)
}

/// True when this datagram belongs to a snapshot cycle rather than the
/// incremental stream.
fn is_snapshot_datagram(datagram: &[u8]) -> bool {
    wire::PacketHeaderDecoder::wrap(datagram)
        .map(|h| h.is_snapshot())
        .unwrap_or(false)
}

/// Adopts a snapshot if it can close the outstanding gap, then replays the live
/// traffic held during recovery.
#[allow(clippy::too_many_arguments)]
fn handle_snapshot<B: BookSet>(
    datagram: &[u8],
    recovery: &mut RecoveryBuffer,
    books: &mut B,
    arbitrator: &mut Arbitrator,
    stats: &mut HandlerStats,
    digest_log: &mut DigestLog,
    digest_interval: u64,
    in_cycle: &mut bool,
) -> io::Result<()> {
    if !recovery.is_recovering() {
        // Nothing is wrong, so there is nothing to recover. Adopting a snapshot
        // here would replace a correct book with an older one.
        return Ok(());
    }
    let reader = match wire::PacketReader::new(datagram) {
        Ok(r) => r,
        Err(e) => {
            stats.bad_datagrams += 1;
            eprintln!("  dropping a snapshot datagram: {e}");
            return Ok(());
        }
    };
    for m in reader.messages() {
        let Ok((_seq, msg)) = m else {
            stats.bad_datagrams += 1;
            return Ok(());
        };
        let Message::Snapshot(d) = msg else {
            continue;
        };
        if !recovery.snapshot_is_usable(d.last_sequence()) {
            // Reflects a point before the messages we are missing, so adopting
            // it would discard good state and still leave the hole.
            stats.snapshots_discarded += 1;
            continue;
        }

        let flags = d.flags();
        let cycle_start = flags & wire::SNAPSHOT_FLAG_CYCLE_START != 0;

        // A cycle replaces every book, so it has to be joined at the start.
        // Adopting from the middle leaves the symbols whose fragments already
        // went past holding stale state while reporting a successful recovery —
        // which is worse than not recovering at all, because it looks fine.
        if !cycle_start && !*in_cycle {
            stats.snapshots_discarded += 1;
            continue;
        }
        if cycle_start {
            // The whole set is about to be replaced, not merged into.
            books.clear_all();
            *in_cycle = true;
        }

        // Every fragment appends; the clear happened once, at the cycle start.
        if let Err(e) = apply_message(books, &msg) {
            eprintln!("  a snapshot fragment did not apply: {e}");
            stats.apply_errors += 1;
            *in_cycle = false;
            return Ok(());
        }

        // A cycle covers every symbol. The last fragment of *a symbol* completes
        // one book, not the set, so recovery waits for the cycle-end marker.
        // Ending on the first symbol would leave every other book stale while
        // reporting a successful recovery — and the snapshots that would have
        // fixed them get discarded as unsolicited.
        if d.flags() & wire::SNAPSHOT_FLAG_CYCLE_END == 0 {
            continue;
        }

        let last_sequence = d.last_sequence();

        // The same trap as on the replay path: a gap declared while the snapshot
        // cycle was arriving leaves a hole *inside* the held traffic, and
        // replaying it would fill the original hole, leave the new one, and
        // report success. The snapshot is good as of `last_sequence`, so the
        // held traffic up to the hole is applied and discarded, and the recovery
        // stays open for the rest.
        if let Some((from, through)) = recovery.held_discontinuity(last_sequence + 1) {
            eprintln!(
                "  the snapshot is consistent as of {last_sequence} but {from}..={through} \
                 opened while the cycle was arriving; staying in recovery"
            );
            let mut failure: Option<io::Error> = None;
            recovery.drain_contiguous(last_sequence + 1, |skip_below, bytes| {
                if failure.is_some() {
                    return;
                }
                if let Err(e) = consume(
                    bytes,
                    books,
                    stats,
                    digest_log,
                    digest_interval,
                    false,
                    skip_below,
                ) {
                    failure = Some(e);
                }
            });
            if let Some(e) = failure {
                return Err(e);
            }
            *in_cycle = false;
            recovery.reopen(from);
            return Ok(());
        }

        recovery.adopt_snapshot(last_sequence);
        arbitrator.resync_to(last_sequence + 1);

        // Replay whatever arrived while we were waiting, minus what the
        // snapshot already covers.
        let mut failure: Option<io::Error> = None;
        recovery.replay(|skip_below, bytes| {
            if failure.is_some() {
                return;
            }
            if let Err(e) = consume(
                bytes,
                books,
                stats,
                digest_log,
                digest_interval,
                false,
                skip_below,
            ) {
                failure = Some(e);
            }
        });
        if let Some(e) = failure {
            return Err(e);
        }

        *in_cycle = false;
        let elapsed = recovery.complete(Instant::now());
        stats.recoveries += 1;
        eprintln!(
            "  RECOVERED in {}ms from a snapshot consistent as of sequence {last_sequence}; \
             back to LIVE",
            elapsed.as_millis()
        );
    }
    Ok(())
}

/// Decodes one datagram and applies every message in it.
///
/// Returns whether anything was consumed, so the caller can check its message
/// limit without re-deriving the count.
#[allow(clippy::too_many_arguments)]
fn consume<B: BookSet>(
    datagram: &[u8],
    books: &mut B,
    stats: &mut HandlerStats,
    digest_log: &mut DigestLog,
    digest_interval: u64,
    gapped: bool,
    // Messages below this sequence are already in the book and must be skipped.
    // Only ever non-zero when replaying a datagram that straddles a snapshot
    // boundary; applying those twice is what puts a book quietly and permanently
    // wrong.
    skip_below: u64,
) -> io::Result<bool> {
    let reader = match PacketReader::new(datagram) {
        Ok(r) => r,
        Err(e) => {
            stats.bad_datagrams += 1;
            eprintln!("  dropping a datagram: {e}");
            return Ok(false);
        }
    };
    let mut any = false;
    for m in reader.messages() {
        let (seq, msg) = match m {
            Ok(v) => v,
            Err(e) => {
                stats.bad_datagrams += 1;
                eprintln!("  truncated datagram: {e}");
                break;
            }
        };
        if seq < skip_below {
            continue;
        }
        any = true;
        stats.messages += 1;
        stats.last_sequence = seq;

        if let Err(e) = apply_message(books, &msg) {
            // Only a bug while the stream is supposed to be complete. After a
            // real gap these are the expected fallout of the missing messages,
            // and counting them together would let a genuine bug hide behind an
            // explained one.
            if gapped || stats.joined_mid_stream {
                stats.apply_errors_after_gap += 1;
            } else {
                stats.apply_errors += 1;
                eprintln!("  sequence {seq} does not apply: {e}");
            }
        }
        if let Message::Trade(t) = msg {
            stats.trades += 1;
            stats.shares_traded += u64::from(t.quantity());
        }
        if digest_interval > 0 && seq.is_multiple_of(digest_interval) {
            digest_log.write(seq, BookDigest::of(books))?;
        }
    }
    Ok(any)
}

/// Releases everything the arbitrator can now deliver, in sequence order.
fn drain_into_books<B: BookSet>(
    arbitrator: &mut Arbitrator,
    books: &mut B,
    stats: &mut HandlerStats,
    digest_log: &mut DigestLog,
    digest_interval: u64,
) -> io::Result<()> {
    let gapped = arbitrator.state() == FeedState::Gapped;
    // The closure cannot return a Result, so a failure is parked and raised
    // once the drain is done. Losing the rest of a ready batch on a write error
    // would be worse than finishing it.
    let mut failure: Option<io::Error> = None;
    arbitrator.drain_ready(|_first, _count, bytes| {
        if failure.is_some() {
            return;
        }
        if let Err(e) = consume(bytes, books, stats, digest_log, digest_interval, gapped, 0) {
            failure = Some(e);
        }
    });
    match failure {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn print_books<B: BookSet>(books: &B, cfg: &Config, depth: usize) {
    // Built through the callback interface rather than `levels()`, so this works
    // for either book. The `Vec`s here are fine: printing is a one-off at the
    // end of a run, not the steady-state path.
    let mut bids: Vec<book::Level> = Vec::new();
    let mut asks: Vec<book::Level> = Vec::new();
    books.for_each_symbol(&mut |symbol_id, b| {
        let name = cfg.symbol_name(symbol_id).unwrap_or("?");
        println!("\n{name} (symbol {symbol_id}) — {} orders resting", b.len());
        println!(
            "{:>14}  {:>10}  {:>6}     {:>6}  {:>10}  {:>14}",
            "bid", "qty", "n", "n", "qty", "ask"
        );
        bids.clear();
        asks.clear();
        b.for_each_level(wire::Side::Bid, depth, &mut |l| bids.push(l));
        b.for_each_level(wire::Side::Ask, depth, &mut |l| asks.push(l));
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
    });
    let _ = io::stdout().flush();
}

/// Which book implementation the handler rebuilds into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum BookKind {
    /// `BTreeMap` of `VecDeque`. Obviously correct, allocates freely, and stays
    /// in the tree as the oracle the fast book is tested against.
    Reference,
    /// Dense tick-indexed levels, a slab of order nodes, an open-addressed
    /// order-id map. Allocation-free per message.
    Fast,
}

impl std::fmt::Display for BookKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reference => write!(f, "reference"),
            Self::Fast => write!(f, "fast"),
        }
    }
}

/// Measures the steady-state receive loop when `--verify-allocations` is on.
///
/// Per outer pass rather than per run: a single delta across a whole session
/// cannot tell "allocated once during setup" from "allocates on every message",
/// and the first of those is fine while the second is the bug. Recording where
/// the first dirty pass happened turns a number into something diagnosable.
///
/// Measuring starts only after [`ALLOC_WARMUP_MESSAGES`], because the first
/// datagrams through a fresh process legitimately allocate: a book for a symbol
/// it has not seen, the digest log's buffer, whatever `std` initialises lazily.
/// Counting those would make the claim unachievable rather than meaningful — see
/// the module docs of `alloc-guard` for where that boundary sits and why.
///
/// This reports; `crates/feed-handler/tests/allocation.rs` asserts. A flag a
/// human has to run and read is a demonstration, not a proof.
struct AllocProbe {
    enabled: bool,
    armed: bool,
    total: AllocCounts,
    passes: u64,
    first_dirty: Option<(u64, AllocCounts)>,
}

/// Messages that must be delivered before allocation counting starts.
const ALLOC_WARMUP_MESSAGES: u64 = 50_000;

impl AllocProbe {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            armed: false,
            total: AllocCounts::default(),
            passes: 0,
            first_dirty: None,
        }
    }

    fn begin(&mut self, delivered: u64) -> Option<AllocGuard> {
        if !self.enabled {
            return None;
        }
        if !self.armed {
            if delivered < ALLOC_WARMUP_MESSAGES {
                return None;
            }
            self.armed = true;
        }
        Some(AllocGuard::start())
    }

    fn end(&mut self, guard: Option<AllocGuard>, delivered: u64) {
        let Some(guard) = guard else { return };
        let d = guard.finish();
        self.passes += 1;
        self.total.allocations += d.allocations;
        self.total.deallocations += d.deallocations;
        self.total.reallocations += d.reallocations;
        self.total.bytes += d.bytes;
        if !d.is_clean() && self.first_dirty.is_none() {
            self.first_dirty = Some((delivered, d));
        }
    }

    fn report(&self, kind: BookKind) {
        if !self.enabled {
            return;
        }
        if !self.armed {
            eprintln!(
                "  allocations: not measured — the run ended before \
                 {ALLOC_WARMUP_MESSAGES} messages, which is the warm-up"
            );
            return;
        }
        eprintln!(
            "  allocations over {} steady-state passes with --books {kind}: {}",
            self.passes, self.total
        );
        if let Some((at, d)) = self.first_dirty {
            eprintln!("  first allocated after {at} messages: {d}");
            if kind == BookKind::Reference {
                eprintln!(
                    "  that is expected here: the reference book is a BTreeMap of VecDeque \
                     and allocates per level. --books fast is what the claim is about."
                );
            }
        }
    }

    fn write_summary(&self, path: &std::path::Path, kind: BookKind) -> io::Result<()> {
        use std::fs::OpenOptions;
        let mut f = OpenOptions::new().append(true).open(path)?;
        writeln!(f, "books={kind}")?;
        writeln!(f, "alloc_measured={}", self.armed)?;
        writeln!(f, "alloc_passes={}", self.passes)?;
        writeln!(f, "allocations={}", self.total.allocations)?;
        writeln!(f, "deallocations={}", self.total.deallocations)?;
        writeln!(f, "reallocations={}", self.total.reallocations)?;
        writeln!(f, "alloc_bytes={}", self.total.bytes)?;
        f.flush()
    }
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
