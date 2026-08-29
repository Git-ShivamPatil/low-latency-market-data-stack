//! The configuration file both ends of the stack read.
//!
//! `configs/local.toml` is named directly by the portfolio's second copyable
//! command, so its shape is part of the public surface of this project:
//!
//! ```text
//! cargo run --release --bin matching-engine -- --config configs/local.toml
//! ```
//!
//! Everything has a default. A missing file is an error (silently running on
//! defaults when someone mistyped a path is worse than stopping), but a partial
//! file is fine — an operator overriding one rate should not have to restate the
//! symbol table.

use std::fs;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use transport::{SocketOptions, TransportMode};

#[derive(Debug)]
pub enum ConfigError {
    Read {
        path: PathBuf,
        error: std::io::Error,
    },
    Parse {
        path: PathBuf,
        error: toml::de::Error,
    },
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, error } => write!(f, "cannot read {}: {error}", path.display()),
            Self::Parse { path, error } => write!(f, "cannot parse {}: {error}", path.display()),
            Self::Invalid(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub transport: Transport,
    pub feed: Feed,
    pub market: Market,
    pub engine: Engine,
    pub handler: Handler,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|error| ConfigError::Read {
            path: path.to_path_buf(),
            error,
        })?;
        let cfg: Self = toml::from_str(&text).map_err(|error| ConfigError::Parse {
            path: path.to_path_buf(),
            error,
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.market.symbols.is_empty() {
            return Err(ConfigError::Invalid(
                "market.symbols is empty; the engine would have nothing to trade".into(),
            ));
        }
        let mut ids: Vec<u16> = self.market.symbols.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.market.symbols.len() {
            return Err(ConfigError::Invalid(
                "market.symbols contains a duplicate id".into(),
            ));
        }
        for s in &self.market.symbols {
            if s.tick_size <= 0 {
                return Err(ConfigError::Invalid(format!(
                    "symbol {} has tick_size {}; it must be positive",
                    s.name, s.tick_size
                )));
            }
        }
        if self.feed.batch_size == 0 {
            return Err(ConfigError::Invalid(
                "feed.batch_size must be at least 1".into(),
            ));
        }
        // 24 bytes of packet header plus the largest fixed message (Trade at
        // 8 + 40) has to fit, or no datagram could ever be produced.
        const FLOOR: usize = 24 + 8 + 40;
        if self.feed.max_datagram_bytes < FLOOR {
            return Err(ConfigError::Invalid(format!(
                "feed.max_datagram_bytes is {}; it must be at least {FLOOR} to hold \
                 a packet header and one Trade",
                self.feed.max_datagram_bytes
            )));
        }
        // A recovery has to outlive at least two cycles: the one that may already
        // have been in flight when the gap opened and is therefore too old, and
        // the next one, which is the first that can actually close it.
        if self.feed.snapshot_interval_millis > 0
            && self.handler.recovery_timeout_millis <= self.feed.snapshot_interval_millis * 2
        {
            return Err(ConfigError::Invalid(format!(
                "handler.recovery_timeout_millis is {} but the snapshot cycle is every {}ms;                  recovery must outlive two cycles or it can time out before the first                  snapshot that could close the gap was ever due",
                self.handler.recovery_timeout_millis, self.feed.snapshot_interval_millis
            )));
        }
        Ok(())
    }

    pub fn socket_options(&self) -> SocketOptions {
        SocketOptions {
            interface: self.transport.interface,
            ttl: self.transport.ttl,
            loopback: self.transport.loopback,
            buffer_bytes: self.transport.buffer_bytes,
        }
    }

    pub fn symbol_name(&self, id: u16) -> Option<&str> {
        self.market
            .symbols
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.name.as_str())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Transport {
    /// `multicast` or `unicast-fanout`. `--transport` on either binary wins.
    pub mode: String,
    /// Which local interface carries multicast. `0.0.0.0` lets the routing
    /// table choose — usually right, and occasionally the entire problem.
    pub interface: Ipv4Addr,
    pub ttl: u32,
    pub loopback: bool,
    pub buffer_bytes: usize,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            mode: "multicast".into(),
            interface: Ipv4Addr::UNSPECIFIED,
            ttl: 1,
            loopback: true,
            buffer_bytes: 4 * 1024 * 1024,
        }
    }
}

impl Transport {
    pub fn mode(&self) -> Result<TransportMode, ConfigError> {
        self.mode.parse().map_err(ConfigError::Invalid)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Feed {
    pub a: Channel,
    pub b: Channel,
    /// Messages per datagram.
    ///
    /// This is the number the throughput claim depends on, which is why it is a
    /// visible knob rather than a constant buried in the publisher. At one
    /// message per datagram the ceiling is the kernel UDP path — somewhere
    /// around 300-600K packets per second per core — not anything this code
    /// does. See `docs/WIRE.md`.
    pub batch_size: u16,
    /// Hard cap on datagram size. 1400 stays under a 1500-byte MTU with room
    /// for IP and UDP headers; raise it only where the whole path is known to
    /// carry larger frames.
    pub max_datagram_bytes: usize,
    /// A partial batch is sent after this long rather than waiting for it to
    /// fill. Without it a quiet symbol would sit in the buffer indefinitely.
    pub flush_interval_micros: u64,
    /// How often an idle channel emits a `Heartbeat`.
    pub heartbeat_millis: u64,
    /// How often a full snapshot of every book is published. 0 disables it.
    ///
    /// Snapshots ride the same two channels marked with `PACKET_FLAG_SNAPSHOT`
    /// and carry their own sequence space, so a consumer routes on the flag and
    /// they never look like a gap in the incremental stream.
    ///
    /// The interval is a recovery-time budget, not a tuning knob: a handler that
    /// cannot replay waits at most this long for a book it can trust.
    pub snapshot_interval_millis: u64,
    /// How long to keep publishing snapshots after the last message.
    ///
    /// A gap in the final moments of a run is only detectable once the feed goes
    /// quiet, which is after the engine has stopped. Without a linger, a single
    /// parting cycle races that detection and usually loses, leaving a consumer
    /// stuck holding a book it knows is wrong.
    pub snapshot_linger_millis: u64,
}

impl Default for Feed {
    fn default() -> Self {
        Self {
            a: Channel {
                group: SocketAddrV4::new(Ipv4Addr::new(239, 1, 1, 1), 30001),
                unicast_targets: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001)],
                unicast_bind: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001),
            },
            b: Channel {
                group: SocketAddrV4::new(Ipv4Addr::new(239, 1, 1, 2), 30001),
                unicast_targets: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31002)],
                unicast_bind: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31002),
            },
            batch_size: 32,
            max_datagram_bytes: 1400,
            flush_interval_micros: 500,
            heartbeat_millis: 1000,
            snapshot_interval_millis: 2000,
            snapshot_linger_millis: 3000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Channel {
    /// Multicast group and port. The A and B groups differ while the port is
    /// shared, so a receiver binds the group address itself to keep the two
    /// arms independent.
    pub group: SocketAddrV4,
    /// Where the publisher sends in unicast-fanout mode.
    pub unicast_targets: Vec<SocketAddrV4>,
    /// Where a handler binds in unicast-fanout mode.
    pub unicast_bind: SocketAddrV4,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            group: SocketAddrV4::new(Ipv4Addr::new(239, 1, 1, 1), 30001),
            unicast_targets: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001)],
            unicast_bind: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Market {
    pub symbols: Vec<Symbol>,
}

impl Default for Market {
    fn default() -> Self {
        Self {
            symbols: vec![Symbol {
                id: 1,
                name: "ACME".into(),
                reference_price: 1_000_000,
                tick_size: 100,
            }],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Symbol {
    /// The `symbolId` on the wire. A dense integer rather than a ticker string,
    /// because the tick-indexed book in milestone 5 indexes on it directly.
    pub id: u16,
    pub name: String,
    /// Starting mid, in wire price units (10^-4).
    pub reference_price: i64,
    /// Minimum price increment, in the same units.
    pub tick_size: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Engine {
    /// Seeds the order-flow generator. The same seed produces the same stream,
    /// which is what makes the smoke test reproducible rather than flaky.
    pub seed: u64,
    /// Stop after this many published messages. 0 runs until interrupted.
    pub messages: u64,
    /// Stop after this long. 0 runs until interrupted.
    pub duration_seconds: u64,
    /// Throttle to roughly this many messages per second. 0 runs flat out.
    pub rate_per_second: u64,
    /// Write a book digest every this many sequences. Both ends use the same
    /// interval so their checkpoints line up.
    pub digest_interval: u64,
    pub digest_path: Option<PathBuf>,
    /// Run a shadow book through the same apply path the handler uses and stop
    /// on any disagreement. Costs roughly a second book update per message, so
    /// it is off unless asked for — and on in the smoke test.
    pub self_check: bool,
    /// Roughly how many orders to keep resting per side per symbol.
    pub target_depth: usize,
    /// How far from the touch new orders are placed, in ticks.
    pub price_spread_ticks: i64,
    /// Chance that a new order is priced to cross the opposite side.
    pub aggressive_chance: f64,
    /// Chance of cancelling a resting order instead of adding one.
    pub cancel_chance: f64,
    /// Chance of amending a resting order instead of adding one.
    pub modify_chance: f64,
    pub min_quantity: u32,
    pub max_quantity: u32,
    /// Fraction of datagrams deliberately not sent, per arm. 0 disables it.
    ///
    /// Loss is injected at the publisher rather than with `tc qdisc` because it
    /// needs no privileges, no second machine, and reproduces exactly from a
    /// seed — and what the handler must survive is simply a datagram that never
    /// arrives.
    pub drop_rate: f64,
    /// `independent`, `exclusive` or `correlated`. See `DropMode` in the engine:
    /// only `exclusive` makes "zero arbitrated gaps" a theorem rather than a
    /// probability, and `correlated` is what forces the GAPPED path.
    pub drop_mode: String,
    /// Seeded separately from `seed` so that turning loss on does not change
    /// which orders are generated.
    pub drop_seed: u64,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            seed: 0x5EED_1234_ABCD_0001,
            messages: 0,
            duration_seconds: 0,
            rate_per_second: 0,
            digest_interval: 1000,
            digest_path: None,
            self_check: false,
            target_depth: 40,
            price_spread_ticks: 8,
            aggressive_chance: 0.18,
            cancel_chance: 0.25,
            modify_chance: 0.15,
            min_quantity: 1,
            max_quantity: 500,
            drop_rate: 0.0,
            drop_mode: "independent".into(),
            drop_seed: 0x0105_0B10_5510_0000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Handler {
    pub digest_interval: u64,
    pub digest_path: Option<PathBuf>,
    pub stats_interval_millis: u64,
    /// Stop after consuming this many messages. 0 runs until interrupted.
    pub messages: u64,
    /// Give up if no datagram arrives for this long. 0 waits forever.
    pub idle_timeout_seconds: u64,
    /// How many out-of-order datagrams may be held before the hole ahead of
    /// them is declared lost.
    ///
    /// This is the bound that turns "wait for the other arm" into something with
    /// a worst case, capping both memory and the delay a single loss can add.
    ///
    /// 256 datagrams is ~8000 messages of slack at a batch of 32, and costs
    /// about 350KB. That is deliberately generous: the amount of reordering the
    /// window has to absorb is set by how far the two arms drift apart, which on
    /// a host where the handler cannot keep up with the publisher is far larger
    /// than any real network reordering. A window that is too small does not
    /// fail safe — it invents gaps that never happened.
    pub reorder_window_datagrams: usize,
    /// Declare an outstanding hole lost after this long with nothing arriving.
    /// Without it a stalled feed would hold buffered messages forever.
    pub gap_timeout_millis: u64,
    /// How many live datagrams may be held while waiting for a snapshot to
    /// recover from a gap.
    ///
    /// At a 2-second snapshot cycle and a batched feed, this is the recovery
    /// path's memory bound. It has to cover a full cycle of live traffic or
    /// recovery fails for want of buffer rather than for want of a snapshot.
    pub recovery_buffer_datagrams: usize,
    /// Give up on a recovery attempt after this long.
    ///
    /// Must comfortably exceed `feed.snapshot_interval_millis`, or a recovery
    /// will time out before the snapshot it is waiting for was ever due.
    pub recovery_timeout_millis: u64,
}

impl Default for Handler {
    fn default() -> Self {
        Self {
            digest_interval: 1000,
            digest_path: None,
            stats_interval_millis: 1000,
            messages: 0,
            idle_timeout_seconds: 0,
            reorder_window_datagrams: 256,
            gap_timeout_millis: 250,
            recovery_buffer_datagrams: 4096,
            recovery_timeout_millis: 10_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_file_yields_working_defaults() {
        let cfg: Config = toml::from_str("").expect("empty is valid");
        cfg.validate().expect("defaults are usable");
        assert_eq!(cfg.transport.mode().unwrap(), TransportMode::Multicast);
        assert_eq!(cfg.feed.a.group.port(), 30001);
        assert_eq!(cfg.feed.batch_size, 32);
    }

    #[test]
    fn a_partial_file_only_overrides_what_it_names() {
        let cfg: Config = toml::from_str(
            r#"
            [feed]
            batch_size = 4
            "#,
        )
        .expect("partial is valid");
        assert_eq!(cfg.feed.batch_size, 4);
        assert_eq!(
            cfg.feed.max_datagram_bytes, 1400,
            "untouched fields keep their defaults"
        );
    }

    #[test]
    fn a_typo_in_a_key_is_an_error_rather_than_a_silent_default() {
        // deny_unknown_fields exists for exactly this: a misspelled batch_size
        // that silently ran at 32 would quietly invalidate a throughput number.
        let err = toml::from_str::<Config>(
            r#"
            [feed]
            batch_sixe = 4
            "#,
        );
        assert!(err.is_err(), "an unknown key must be rejected");
    }

    #[test]
    fn transport_mode_round_trips_through_the_file() {
        let cfg: Config = toml::from_str(
            r#"
            [transport]
            mode = "unicast-fanout"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.transport.mode().unwrap(), TransportMode::UnicastFanout);
    }

    #[test]
    fn validation_rejects_configurations_that_could_not_run() {
        let mut cfg = Config::default();
        cfg.market.symbols.clear();
        assert!(cfg.validate().is_err(), "no symbols means nothing to trade");

        let mut cfg = Config::default();
        cfg.feed.batch_size = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = Config::default();
        cfg.feed.max_datagram_bytes = 32;
        assert!(
            cfg.validate().is_err(),
            "a datagram that cannot hold one Trade is unusable"
        );

        let mut cfg = Config::default();
        cfg.market.symbols[0].tick_size = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = Config::default();
        cfg.market.symbols.push(cfg.market.symbols[0].clone());
        assert!(cfg.validate().is_err(), "duplicate symbol ids");
    }

    #[test]
    fn a_missing_file_is_an_error_not_a_fallback_to_defaults() {
        let err = Config::load(Path::new("/nonexistent/configs/local.toml"));
        assert!(matches!(err, Err(ConfigError::Read { .. })));
    }
}
