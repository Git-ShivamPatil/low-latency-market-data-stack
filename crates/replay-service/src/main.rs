//! The replay service binary.
//!
//! ```text
//! cargo run --release --bin replay-service -- --config configs/local.toml
//! ```
//!
//! Two listeners and one shared history:
//!
//! - the **uplink** port, where the engine connects and streams every datagram
//!   it publishes;
//! - the **request** port, where consumers ask for a sequence range.
//!
//! # Threads, and why the lock is not a problem
//!
//! One thread per connection, sharing the store behind a `Mutex`. That is an
//! unfashionable design for a project about latency, and it is the right one
//! here: the store is written by exactly one uplink and read by consumers only
//! when something has already gone wrong. Contention on this lock means a
//! consumer is recovering, and a recovering consumer is not in a hurry measured
//! in microseconds — it is in a hurry measured against the snapshot interval,
//! which is two seconds.
//!
//! The publisher's own hot path never touches this process at all.

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use clap::Parser;
use mdconfig::Config;
use replay_service::protocol::{self, ResponseHeader, Status};
use replay_service::store::DatagramStore;

#[derive(Parser, Debug)]
#[command(
    name = "replay-service",
    about = "Serves ranges of the market-data stream over TCP so a consumer can fill a gap exactly",
    long_about = None,
)]
struct Args {
    /// Path to the configuration file.
    #[arg(long, default_value = "configs/local.toml")]
    config: PathBuf,

    /// Where the engine connects to feed this service.
    #[arg(long, value_name = "ADDR")]
    uplink_bind: Option<SocketAddrV4>,

    /// Where consumers connect to ask for a range.
    #[arg(long, value_name = "ADDR")]
    request_bind: Option<SocketAddrV4>,

    /// How many datagrams of history to keep. This is the recovery horizon.
    #[arg(long)]
    history: Option<usize>,

    /// Stop after this many seconds. 0 runs until interrupted.
    #[arg(long, default_value_t = 0)]
    duration: u64,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("replay-service: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Shared {
    store: Mutex<DatagramStore>,
    served: AtomicU64,
    refused: AtomicU64,
    uplinks: AtomicU64,
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;

    let uplink_bind = args.uplink_bind.unwrap_or(cfg.replay.uplink_bind);
    let request_bind = args.request_bind.unwrap_or(cfg.replay.request_bind);
    let history = args.history.unwrap_or(cfg.replay.history_datagrams);
    let max_datagram = cfg.feed.max_datagram_bytes.max(65_536);

    let shared = Arc::new(Shared {
        store: Mutex::new(DatagramStore::new(history, max_datagram)),
        served: AtomicU64::new(0),
        refused: AtomicU64::new(0),
        uplinks: AtomicU64::new(0),
    });

    let uplink = TcpListener::bind(uplink_bind)?;
    let requests = TcpListener::bind(request_bind)?;

    eprintln!("replay-service");
    eprintln!("  uplink   <- {uplink_bind}");
    eprintln!("  requests <- {request_bind}");
    eprintln!(
        "  history    {history} datagrams (~{} messages at a batch of {})",
        history * usize::from(cfg.feed.batch_size),
        cfg.feed.batch_size
    );

    {
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            for conn in uplink.incoming() {
                match conn {
                    Ok(stream) => {
                        let shared = Arc::clone(&shared);
                        shared.uplinks.fetch_add(1, Ordering::Relaxed);
                        std::thread::spawn(move || {
                            if let Err(e) = serve_uplink(stream, &shared, max_datagram) {
                                eprintln!("  uplink ended: {e}");
                            }
                        });
                    }
                    Err(e) => eprintln!("  uplink accept failed: {e}"),
                }
            }
        });
    }

    {
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            for conn in requests.incoming() {
                match conn {
                    Ok(stream) => {
                        let shared = Arc::clone(&shared);
                        std::thread::spawn(move || {
                            if let Err(e) = serve_request(stream, &shared) {
                                eprintln!("  request failed: {e}");
                            }
                        });
                    }
                    Err(e) => eprintln!("  request accept failed: {e}"),
                }
            }
        });
    }

    let started = std::time::Instant::now();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(2));
        {
            let store = shared.store.lock().expect("store lock");
            eprintln!(
                "  {}  served {} refused {}  uplinks {}",
                store.describe(),
                shared.served.load(Ordering::Relaxed),
                shared.refused.load(Ordering::Relaxed),
                shared.uplinks.load(Ordering::Relaxed),
            );
        }
        if args.duration > 0 && started.elapsed().as_secs() >= args.duration {
            break;
        }
    }
    Ok(())
}

/// Reads the engine's stream of published datagrams into the store.
fn serve_uplink(stream: TcpStream, shared: &Shared, max_datagram: usize) -> io::Result<()> {
    let peer = stream.peer_addr().ok();
    let mut r = BufReader::new(stream);
    protocol::read_hello(&mut r)?;
    eprintln!("  uplink connected from {peer:?}");

    let mut buf = vec![0u8; max_datagram];
    let mut count = 0u64;
    while let Some(n) = protocol::read_datagram(&mut r, &mut buf)? {
        let mut store = shared.store.lock().expect("store lock");
        // A datagram the store refuses is a wiring problem, not a stream
        // problem: snapshots on the incremental uplink, or something that does
        // not decode. Say so once rather than per datagram.
        if let Err(why) = store.push(&buf[..n]) {
            if count == 0 {
                eprintln!("  uplink is sending datagrams the store cannot hold: {why}");
            }
        }
        count += 1;
    }
    eprintln!("  uplink closed after {count} datagrams");
    Ok(())
}

/// Answers one range request.
fn serve_request(stream: TcpStream, shared: &Shared) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut r = BufReader::new(stream.try_clone()?);
    let mut w = BufWriter::new(stream);

    let req = protocol::read_request(&mut r)?;

    // Everything that touches the store happens inside this block, and the
    // datagrams are COPIED out before the lock is released.
    //
    // Writing them to the socket under the lock would be the obvious thing and
    // the wrong one: a `BufWriter` flushes when it fills, so a consumer that
    // stopped reading would block the write, which would hold the lock, which
    // would stall the uplink — and a stalled uplink means the store starts
    // missing the very datagrams the next consumer will ask for. One slow
    // reader would take out recovery for everyone.
    //
    // The copy is per request, and requests only happen when something has
    // already gone wrong.
    let (header, payload) = {
        let store = shared.store.lock().expect("store lock");
        let found = store.locate(req.from, req.through);
        let header = ResponseHeader {
            status: found.status,
            datagrams: if found.status == Status::Ok {
                (found.end - found.start) as u32
            } else {
                0
            },
            first_available: store.first_sequence(),
            last_available: store.next_sequence(),
        };
        let mut payload: Vec<Vec<u8>> = Vec::new();
        if found.status == Status::Ok {
            payload.reserve(found.end - found.start);
            for i in found.start..found.end {
                let d = store
                    .datagram_at(i)
                    .expect("locate returned an index the store does not hold");
                payload.push(d.to_vec());
            }
        } else {
            eprintln!(
                "  refused {}..={}: {} (holding {}..{})",
                req.from,
                req.through,
                found.status,
                store.first_sequence(),
                store.next_sequence()
            );
        }
        (header, payload)
    };

    protocol::write_response_header(&mut w, header)?;
    for d in &payload {
        protocol::write_datagram(&mut w, d)?;
    }
    if header.status == Status::Ok {
        shared.served.fetch_add(1, Ordering::Relaxed);
    } else {
        shared.refused.fetch_add(1, Ordering::Relaxed);
    }
    w.flush()
}
