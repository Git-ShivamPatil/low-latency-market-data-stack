//! The matching engine binary.
//!
//! ```text
//! cargo run --release --bin matching-engine -- --config configs/local.toml
//! ```
//!
//! That command is published on the project's case-study page, so the binary
//! name and the config path are part of the public surface and are not free to
//! drift.

mod engine;
mod feed;
mod generator;
mod rng;
mod uplink;

use std::io::{self, Write};
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Parser;
use mdconfig::Config;
use transport::{Publisher, TransportMode};

use crate::engine::Engine;
use crate::feed::{DropMode, FeedPublisher};
use crate::generator::{Generator, Intent, Shape};
use book::DigestLog;

#[derive(Parser, Debug)]
#[command(
    name = "matching-engine",
    about = "Price-time-priority matching engine publishing a batched binary feed over A/B channels",
    long_about = None,
)]
struct Args {
    /// Path to the configuration file.
    #[arg(long, default_value = "configs/local.toml")]
    config: PathBuf,

    /// Override `transport.mode`: `multicast` or `unicast-fanout`.
    ///
    /// The fallback exists because multicast over a Docker bridge under WSL2 is
    /// the likeliest infrastructure blocker in this project. Same binary, same
    /// framing, same everything else.
    #[arg(long)]
    transport: Option<String>,

    /// Override the A channel address.
    #[arg(long, value_name = "ADDR")]
    feed_a: Option<SocketAddrV4>,

    /// Override the B channel address.
    #[arg(long, value_name = "ADDR")]
    feed_b: Option<SocketAddrV4>,

    /// Messages per datagram. This is the number a throughput figure has to be
    /// published next to; see docs/WIRE.md.
    #[arg(long)]
    batch_size: Option<u16>,

    /// Stop after at least this many messages. 0 runs until interrupted.
    ///
    /// A **floor, not an exact count.** One order intent can publish several
    /// messages — a `Submit` that crosses emits a `Trade` and the `ModifyOrder`
    /// or `DeleteOrder` describing the fill — and the loop finishes the intent
    /// it started. Stopping mid-intent would publish a trade whose fill never
    /// arrives, which is precisely the inconsistency the rest of this project
    /// exists to avoid, so the overshoot is deliberate. It is at most a couple
    /// of messages.
    #[arg(long)]
    messages: Option<u64>,

    /// Stop after this many seconds. 0 runs until interrupted.
    #[arg(long)]
    duration: Option<u64>,

    /// Throttle to roughly this many messages per second. 0 runs flat out.
    #[arg(long)]
    rate: Option<u64>,

    /// Seed for the order-flow generator. The same seed replays the same run.
    #[arg(long)]
    seed: Option<u64>,

    /// Write `sequence digest` checkpoints here for the smoke test to compare.
    #[arg(long, value_name = "PATH")]
    digest_path: Option<PathBuf>,

    /// Checkpoint every N sequences. Must match the handler's interval.
    #[arg(long)]
    digest_interval: Option<u64>,

    /// Decode every datagram before sending it and rebuild a shadow book from
    /// the result, then compare it against the engine's own book at shutdown.
    /// Catches an encoding bug here rather than as a mystery digest mismatch in
    /// another process.
    #[arg(long)]
    self_check: bool,

    /// Fraction of datagrams to drop per arm, 0.0 to 1.0.
    ///
    /// Injected at the publisher rather than with `tc qdisc`: it needs no
    /// privileges, no second machine, and replays exactly from a seed. What the
    /// handler has to survive is a datagram that never arrives, and this
    /// produces exactly that.
    #[arg(long, value_name = "RATE")]
    drop_rate: Option<f64>,

    /// How loss is correlated between the arms.
    ///
    /// `exclusive` drops on exactly one arm, which is the model under which
    /// "single-arm loss costs nothing" is a theorem rather than a probability.
    /// `independent` is what a real network does and unavoidably loses some
    /// datagrams on both. `correlated` drops the same ones on both arms and is
    /// how the GAPPED path gets exercised.
    #[arg(long, value_name = "MODE")]
    drop_mode: Option<String>,

    /// Seed for the loss injector. Separate from `--seed` so that turning loss
    /// on does not change which orders are generated.
    #[arg(long)]
    drop_seed: Option<u64>,

    /// How often to publish a full snapshot of every book, in milliseconds.
    /// 0 disables the cycle.
    #[arg(long, value_name = "MS")]
    snapshot_interval: Option<u64>,

    /// Stream every published datagram to a replay service at this address.
    ///
    /// Optional infrastructure: if the service is down the engine runs normally
    /// and says so. Never on the publish path — see `uplink`.
    #[arg(long, value_name = "ADDR")]
    replay_uplink: Option<String>,

    /// Print the resolved configuration and exit without sending anything.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("matching-engine: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut cfg = Config::load(&args.config)?;
    apply_overrides(&mut cfg, &args);

    let mode: TransportMode = match &args.transport {
        Some(s) => s
            .parse()
            .map_err(|e: String| -> Box<dyn std::error::Error> { e.into() })?,
        None => cfg.transport.mode()?,
    };
    let opts = cfg.socket_options();

    let (a_targets, b_targets) = match mode {
        TransportMode::Multicast => (vec![cfg.feed.a.group], vec![cfg.feed.b.group]),
        TransportMode::UnicastFanout => (
            cfg.feed.a.unicast_targets.clone(),
            cfg.feed.b.unicast_targets.clone(),
        ),
    };

    if args.dry_run {
        println!("transport      {mode}");
        println!("channel A      {a_targets:?}");
        println!("channel B      {b_targets:?}");
        println!("batch size     {}", cfg.feed.batch_size);
        println!("datagram cap   {} bytes", cfg.feed.max_datagram_bytes);
        println!("symbols        {}", cfg.market.symbols.len());
        println!("seed           {}", cfg.engine.seed);
        println!("digest every   {} sequences", cfg.engine.digest_interval);
        println!(
            "loss           {} ({})",
            cfg.engine.drop_rate, cfg.engine.drop_mode
        );
        return Ok(());
    }

    let a = Publisher::bind(mode, &a_targets, opts).map_err(|e| bind_hint(e, mode, "A"))?;
    let b = Publisher::bind(mode, &b_targets, opts).map_err(|e| bind_hint(e, mode, "B"))?;

    let mut feed = FeedPublisher::new(a, b, cfg.feed.batch_size, cfg.feed.max_datagram_bytes);
    let mut engine = Engine::new(&cfg.market.symbols);
    let mut generator = Generator::new(cfg.engine.seed, Shape::from(&cfg.engine));

    let mut digest_log = DigestLog::open(cfg.engine.digest_path.as_deref())?;
    let digest_interval = cfg.engine.digest_interval;

    let self_check = args.self_check || cfg.engine.self_check;
    if self_check {
        feed.enable_self_check();
    }

    let uplink_target = args
        .replay_uplink
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| Some(cfg.replay.uplink_connect.clone()).filter(|s| !s.is_empty()));
    if let Some(target) = uplink_target {
        match target.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                // 4096 datagrams of slack. Enough to ride out a service restart
                // at the rates this engine publishes; beyond that the store
                // notices the hole rather than serving around it.
                feed.set_uplink(uplink::Uplink::connect(addr, 4096));
                eprintln!("  replay uplink -> {addr}");
            }
            Err(e) => {
                return Err(format!("--replay-uplink {target:?} is not an address: {e}").into())
            }
        }
    }

    let drop_mode: DropMode = cfg
        .engine
        .drop_mode
        .parse()
        .map_err(|e: String| -> Box<dyn std::error::Error> { e.into() })?;
    if cfg.engine.drop_rate > 0.0 {
        feed.set_loss(cfg.engine.drop_rate, drop_mode, cfg.engine.drop_seed);
    }

    eprintln!("matching-engine");
    eprintln!("  {}", feed.describe());
    eprintln!(
        "  {} symbols, seed {}, digest every {} sequences{}",
        cfg.market.symbols.len(),
        cfg.engine.seed,
        digest_interval,
        if self_check { ", self-check on" } else { "" }
    );
    if let Some(p) = cfg.engine.digest_path.as_deref() {
        eprintln!("  checkpoints -> {}", p.display());
    }
    if feed.loss_enabled() {
        eprintln!(
            "  injecting {:.2}% loss per arm, {drop_mode} (seed {})",
            cfg.engine.drop_rate * 100.0,
            cfg.engine.drop_seed
        );
    }

    let started = Instant::now();
    let deadline = (cfg.engine.duration_seconds > 0)
        .then(|| started + Duration::from_secs(cfg.engine.duration_seconds));
    let message_limit = cfg.engine.messages;
    let rate = cfg.engine.rate_per_second;
    let flush_interval = Duration::from_micros(cfg.feed.flush_interval_micros.max(1));
    let heartbeat_interval = Duration::from_millis(cfg.feed.heartbeat_millis.max(1));
    // 0 disables the cycle entirely, which is what the tests that want a bare
    // incremental feed use.
    let snapshot_interval = (cfg.feed.snapshot_interval_millis > 0)
        .then(|| Duration::from_millis(cfg.feed.snapshot_interval_millis));

    let mut last_flush = Instant::now();
    let mut last_heartbeat = Instant::now();
    let mut last_report = Instant::now();
    let mut last_snapshot = Instant::now();
    let mut snapshot_cycles = 0u64;

    // Bounded by --messages or --duration. There is deliberately no signal
    // handler: installing one needs a crate or `unsafe`, and this workspace
    // denies `unsafe_code`. Ctrl-C therefore kills the process outright, which
    // is why DigestLog flushes every line as it is written rather than relying
    // on a clean exit — an interrupted demo still leaves usable evidence.
    loop {
        if let Some(d) = deadline {
            if Instant::now() >= d {
                break;
            }
        }
        // Checked between intents, never inside one. See `Args::messages`: a
        // crossing order publishes a trade and the fill that follows it, and
        // splitting that pair to hit an exact count would put the feed in a
        // state no consumer could reconcile.
        if message_limit > 0 && feed.stats().messages >= message_limit {
            break;
        }

        let idx = generator.pick_symbol(engine.symbols.len());
        generator.drift_mid(&mut engine.symbols[idx]);
        let intent = generator.next(&engine.symbols[idx]);

        // Checkpoints are taken inside the engine, immediately after each
        // message is given its sequence — that is the only point where the
        // engine's book is exactly "messages 1..=seq".
        let mut checkpoint = |eng: &Engine, seq: u64| -> io::Result<()> {
            if digest_interval > 0 && seq.is_multiple_of(digest_interval) {
                digest_log.write(seq, eng.digest())?;
            }
            Ok(())
        };

        match intent {
            Intent::Submit {
                symbol_id,
                side,
                price,
                quantity,
            } => engine.submit(&mut feed, symbol_id, side, price, quantity, &mut checkpoint)?,
            Intent::Cancel {
                symbol_id,
                order_id,
            } => engine.cancel(&mut feed, symbol_id, order_id, &mut checkpoint)?,
            Intent::Amend {
                symbol_id,
                order_id,
                new_price,
                new_quantity,
            } => engine.amend(
                &mut feed,
                symbol_id,
                order_id,
                new_price,
                new_quantity,
                &mut checkpoint,
            )?,
        }

        let now = Instant::now();
        if now.duration_since(last_flush) >= flush_interval {
            feed.flush_on_timer()?;
            last_flush = now;
        }
        if now.duration_since(last_heartbeat) >= heartbeat_interval {
            feed.heartbeat(feed.last_sequence())?;
            last_heartbeat = now;
        }
        if let Some(interval) = snapshot_interval {
            if now.duration_since(last_snapshot) >= interval {
                let cycle = feed.publish_snapshot(engine.books())?;
                snapshot_cycles += 1;
                if snapshot_cycles == 1 {
                    eprintln!(
                        "  snapshot cycle every {}ms; first cycle at snapshot sequence {} \
                            covered {} symbols and {} orders in {} datagrams, consistent as of \
                            incremental sequence {}",
                        interval.as_millis(),
                        cycle.sequence,
                        cycle.symbols,
                        cycle.orders,
                        cycle.datagrams,
                        cycle.last_sequence
                    );
                }
                last_snapshot = now;
            }
        }
        if now.duration_since(last_report) >= Duration::from_secs(5) {
            report(&feed, &engine, started);
            last_report = now;
        }

        if rate > 0 {
            throttle(rate, feed.stats().messages, started);
        }
    }

    feed.flush()?;
    // Keep publishing snapshots for a short while after the last message.
    //
    // A gap in the final moments of a run is only detectable once the feed goes
    // quiet, which by definition happens after the engine has stopped. A single
    // parting cycle races that detection and usually loses, leaving a consumer
    // stuck in GAPPED holding a book it knows is wrong, with nothing further
    // coming that could fix it.
    if let Some(interval) = snapshot_interval {
        let linger_until = Instant::now() + Duration::from_millis(cfg.feed.snapshot_linger_millis);
        loop {
            feed.publish_snapshot(engine.books())?;
            snapshot_cycles += 1;
            if Instant::now() >= linger_until {
                break;
            }
            std::thread::sleep(interval);
        }
    }
    if let Some(shadow) = feed.shadow() {
        // Everything has been flushed, so the shadow covers exactly the same
        // sequence range as the engine's own book. This is the only point where
        // the two are guaranteed comparable.
        let engine_digest = engine.digest();
        let shadow_digest = book::BookDigest::of(shadow);
        if engine_digest != shadow_digest {
            return Err(format!(
                "self-check failed: the feed does not rebuild the engine's own book\n  \
                 engine {engine_digest}\n  replay {shadow_digest}"
            )
            .into());
        }
        eprintln!("  self-check ok: the feed rebuilds the engine's book exactly");
    }
    digest_log.flush()?;

    report(&feed, &engine, started);
    let s = engine.stats();
    eprintln!(
        "  {} orders, {} trades ({} shares), {} cancels, {} amends",
        s.orders_submitted, s.trades, s.shares_traded, s.cancels, s.modifies
    );
    let f = feed.stats();
    if let Some(uplink) = feed.uplink() {
        eprintln!(
            "  replay uplink: {} datagrams sent, {} dropped, {}",
            uplink.sent(),
            uplink.dropped(),
            if uplink.is_connected() {
                "connected"
            } else {
                "not connected"
            }
        );
        if uplink.dropped() > 0 {
            eprintln!(
                "  note: dropped uplink datagrams shorten the replay horizon. The store \
                    discards its history at the discontinuity rather than serving around it, \
                    so nothing is served wrongly - but a consumer that needed that range will \
                    be told it is too old."
            );
        }
    }
    if snapshot_cycles > 0 {
        eprintln!(
            "  {snapshot_cycles} snapshot cycles, {} snapshot datagrams",
            f.snapshot_datagrams
        );
    }
    if feed.loss_enabled() {
        eprintln!(
            "  dropped {} datagrams on A and {} on B of {} sent ({} on both)",
            f.dropped[0], f.dropped[1], f.datagrams, f.dropped_both
        );
    }
    eprintln!("  final book {}", engine.digest());
    engine
        .books()
        .check_invariants()
        .map_err(|e| format!("the engine's own book is inconsistent: {e}"))?;
    Ok(())
}

fn apply_overrides(cfg: &mut Config, args: &Args) {
    if let Some(a) = args.feed_a {
        cfg.feed.a.group = a;
        cfg.feed.a.unicast_targets = vec![a];
    }
    if let Some(b) = args.feed_b {
        cfg.feed.b.group = b;
        cfg.feed.b.unicast_targets = vec![b];
    }
    if let Some(v) = args.batch_size {
        cfg.feed.batch_size = v.max(1);
    }
    if let Some(v) = args.messages {
        cfg.engine.messages = v;
    }
    if let Some(v) = args.duration {
        cfg.engine.duration_seconds = v;
    }
    if let Some(v) = args.rate {
        cfg.engine.rate_per_second = v;
    }
    if let Some(v) = args.seed {
        cfg.engine.seed = v;
    }
    if let Some(v) = args.digest_interval {
        cfg.engine.digest_interval = v;
    }
    if let Some(v) = args.drop_rate {
        cfg.engine.drop_rate = v.clamp(0.0, 1.0);
    }
    if let Some(v) = args.drop_mode.clone() {
        cfg.engine.drop_mode = v;
    }
    if let Some(v) = args.drop_seed {
        cfg.engine.drop_seed = v;
    }
    if let Some(v) = args.snapshot_interval {
        cfg.feed.snapshot_interval_millis = v;
    }
    if args.digest_path.is_some() {
        cfg.engine.digest_path = args.digest_path.clone();
    }
}

/// Turns a bind failure into something actionable rather than an errno.
fn bind_hint(e: io::Error, mode: TransportMode, arm: &str) -> String {
    let mut msg = format!("cannot open channel {arm}: {e}");
    if mode == TransportMode::Multicast {
        msg.push_str(
            "\n  Multicast setup is the most common thing to fail here. Try \
             `--transport unicast-fanout`, which uses the same framing over \
             plain UDP and is a supported mode rather than a workaround.",
        );
    }
    msg
}

fn report(feed: &FeedPublisher, engine: &Engine, started: Instant) {
    let s = feed.stats();
    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    eprintln!(
        "  {:>10} msgs  {:>8} datagrams  {:.1} msg/datagram  {:.0} msg/s  {} orders resting",
        s.messages,
        s.datagrams,
        s.messages_per_datagram(),
        s.messages as f64 / elapsed,
        engine.books().total_orders(),
    );
    let _ = io::stderr().flush();
}

/// Coarse pacing: sleep when the run is ahead of the requested rate.
///
/// Deliberately not a precise scheduler. Rate limiting here exists so a demo
/// does not saturate a laptop, and any real throughput number comes from an
/// unthrottled run measured receiver-side on a quiet host — not from this.
fn throttle(rate: u64, messages: u64, started: Instant) {
    let target = Duration::from_secs_f64(messages as f64 / rate as f64);
    let elapsed = started.elapsed();
    if target > elapsed {
        std::thread::sleep(target - elapsed);
    }
}
