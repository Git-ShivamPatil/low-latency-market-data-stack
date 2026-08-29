//! Feeding published datagrams to the replay service.
//!
//! # The publish path must not depend on this
//!
//! The engine's job is to publish. A replay service that is down, slow, or being
//! restarted must not slow the feed down by one microsecond, and must certainly
//! not stall it — a publisher that blocks on a downstream consumer is a publisher
//! that has handed that consumer control of the market data.
//!
//! So the uplink is a bounded queue and a writer thread. [`Uplink::offer`] copies
//! a datagram into the queue and returns; if the queue is full it **drops the
//! datagram and counts it**, and never waits.
//!
//! # Dropping is safe, because the store notices
//!
//! A dropped datagram means the replay service's history has a hole. That would
//! be dangerous if the service served around it, so it does not: every datagram
//! carries its own `firstSequence`, the store detects the discontinuity on
//! arrival, and discards everything before it rather than hold a history with a
//! lie in the middle. The cost of a drop here is a shortened recovery horizon,
//! not a corrupt replay.
//!
//! `datagrams_dropped` being non-zero is still worth knowing — it means the
//! queue is undersized for the publish rate — so it is reported at shutdown.

use std::io::{self, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct UplinkStats {
    pub datagrams_sent: AtomicU64,
    pub datagrams_dropped: AtomicU64,
    pub reconnects: AtomicU64,
    pub connected: AtomicU64,
}

/// A bounded, non-blocking feed into the replay service.
#[derive(Debug)]
pub struct Uplink {
    tx: SyncSender<Vec<u8>>,
    stats: Arc<UplinkStats>,
}

impl Uplink {
    /// Connects in the background and keeps reconnecting.
    ///
    /// Returns immediately whether or not the service is up: replay is optional
    /// infrastructure, and the engine runs perfectly well without it.
    pub fn connect(addr: SocketAddr, queue_depth: usize) -> Self {
        let (tx, rx) = sync_channel::<Vec<u8>>(queue_depth.max(1));
        let stats = Arc::new(UplinkStats::default());
        let writer_stats = Arc::clone(&stats);

        std::thread::spawn(move || {
            let mut stream: Option<BufWriter<TcpStream>> = None;
            for datagram in rx {
                // Reconnect lazily, when there is something to send. Retrying on
                // a timer while idle would just churn.
                if stream.is_none() {
                    match TcpStream::connect_timeout(&addr, Duration::from_millis(500)) {
                        Ok(s) => {
                            let mut w = BufWriter::new(s);
                            if hello(&mut w).is_ok() {
                                writer_stats.connected.store(1, Ordering::Relaxed);
                                writer_stats.reconnects.fetch_add(1, Ordering::Relaxed);
                                stream = Some(w);
                            }
                        }
                        Err(_) => {
                            // Nothing to be done about it here. The datagram is
                            // lost to the service, the store will notice the
                            // hole, and the engine carries on.
                            writer_stats
                                .datagrams_dropped
                                .fetch_add(1, Ordering::Relaxed);
                            writer_stats.connected.store(0, Ordering::Relaxed);
                            continue;
                        }
                    }
                }
                if let Some(w) = stream.as_mut() {
                    let ok = replay_service::protocol::write_datagram(w, &datagram)
                        .and_then(|()| w.flush())
                        .is_ok();
                    if ok {
                        writer_stats.datagrams_sent.fetch_add(1, Ordering::Relaxed);
                    } else {
                        writer_stats.connected.store(0, Ordering::Relaxed);
                        stream = None;
                    }
                }
            }
        });

        Self { tx, stats }
    }

    /// Hands a datagram to the writer thread. Never blocks.
    #[inline]
    pub fn offer(&self, datagram: &[u8]) {
        // The copy is the price of not blocking the publish path on a socket.
        match self.tx.try_send(datagram.to_vec()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.stats.datagrams_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn sent(&self) -> u64 {
        self.stats.datagrams_sent.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.stats.datagrams_dropped.load(Ordering::Relaxed)
    }

    pub fn is_connected(&self) -> bool {
        self.stats.connected.load(Ordering::Relaxed) == 1
    }
}

/// Writes the uplink handshake. Split out so `Uplink` does not need to name the
/// protocol module in two places.
pub fn hello(w: &mut impl Write) -> io::Result<()> {
    replay_service::protocol::write_hello(w)
}
