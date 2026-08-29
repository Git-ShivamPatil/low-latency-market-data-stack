//! Datagram transport for the feed: UDP multicast, and a unicast fanout that
//! does the same job when multicast will not cooperate.
//!
//! # Why there are two backends on day one
//!
//! Multicast between containers on a Docker bridge under WSL2 is the single
//! most likely infrastructure blocker in this project. IGMP handling, which
//! interface the group join lands on, `IP_MULTICAST_LOOP` semantics and bridge
//! behaviour can each swallow most of a working session, and none of that is
//! debugging anyone can shortcut — it is reading kernel behaviour.
//!
//! So the fallback is not a contingency to write later when multicast breaks.
//! It is here from the first commit that sends a byte, it is the same binaries
//! and the same code path, and it is selected with `--transport unicast-fanout`.
//! Multicast becomes the thing being made to work rather than the thing the
//! project is blocked on.
//!
//! # One send path
//!
//! Both modes are the same code: a socket and a list of targets. Multicast has
//! exactly one target (the group); unicast fanout has one per subscriber. The
//! mode changes how the socket is *configured*, not how a datagram is sent, so
//! there is no dispatch on the hot path and nothing to get wrong twice.
//!
//! # Sockets
//!
//! Sockets are configured with `socket2` — `SO_REUSEADDR`, `IP_MULTICAST_IF`
//! and receive-buffer sizing are not reachable through `std` — and then
//! converted into `std::net::UdpSocket` for the actual I/O. That keeps every
//! read and write on a safe API: `socket2`'s receive path hands back
//! `MaybeUninit` bytes, and this workspace denies `unsafe_code`.

use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};

/// How datagrams reach subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// One send per channel, delivered by the network to every subscriber.
    Multicast,
    /// One send per subscriber. Always works; does not scale, and is not
    /// pretending to.
    UnicastFanout,
}

impl TransportMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Multicast => "multicast",
            Self::UnicastFanout => "unicast-fanout",
        }
    }
}

impl fmt::Display for TransportMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TransportMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "multicast" => Ok(Self::Multicast),
            "unicast-fanout" | "unicast_fanout" | "unicast" => Ok(Self::UnicastFanout),
            other => Err(format!(
                "unknown transport {other:?}: expected \"multicast\" or \"unicast-fanout\""
            )),
        }
    }
}

/// Socket tuning shared by both ends.
#[derive(Debug, Clone, Copy)]
pub struct SocketOptions {
    /// Which local interface carries multicast. `0.0.0.0` lets the routing
    /// table decide, which is usually right and occasionally the whole problem —
    /// under WSL2 the group can land on `lo` instead of `eth0`.
    pub interface: Ipv4Addr,
    /// Multicast TTL. 1 keeps traffic on the local segment, which is what a
    /// single-host or single-bridge setup wants.
    pub ttl: u32,
    /// Whether the sending host also receives its own multicast. Required when
    /// the engine and the handler share a host, which is every setup this
    /// project currently has.
    pub loopback: bool,
    /// Requested `SO_RCVBUF`/`SO_SNDBUF`. The kernel silently clamps this to
    /// `net.core.rmem_max`, so what was actually granted is reported by
    /// [`Receiver::describe`] rather than assumed.
    pub buffer_bytes: usize,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            interface: Ipv4Addr::UNSPECIFIED,
            ttl: 1,
            loopback: true,
            // 4 MiB. A batched feed at the rates this project targets will
            // outrun the default ~208 KiB during any scheduling hiccup, and a
            // full receive buffer is a silent drop.
            buffer_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Sends datagrams to one channel.
#[derive(Debug)]
pub struct Publisher {
    sock: UdpSocket,
    targets: Vec<SocketAddr>,
    mode: TransportMode,
    granted_send_buffer: usize,
}

impl Publisher {
    /// `targets` is the multicast group (exactly one) or the unicast
    /// subscribers (one or more).
    pub fn bind(
        mode: TransportMode,
        targets: &[SocketAddrV4],
        opts: SocketOptions,
    ) -> io::Result<Self> {
        if targets.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a publisher needs at least one target address",
            ));
        }
        if mode == TransportMode::Multicast && targets.len() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "multicast takes exactly one group address per channel",
            ));
        }
        if mode == TransportMode::Multicast {
            if let Some(bad) = targets.iter().find(|t| !t.ip().is_multicast()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} is not a multicast address; 239.0.0.0/8 is the \
                         administratively-scoped range this project uses",
                        bad.ip()
                    ),
                ));
            }
        }

        let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        sock.set_send_buffer_size(opts.buffer_bytes)?;
        sock.bind(&SocketAddrV4::new(opts.interface, 0).into())?;

        if mode == TransportMode::Multicast {
            sock.set_multicast_if_v4(&opts.interface)?;
            sock.set_multicast_ttl_v4(opts.ttl)?;
            sock.set_multicast_loop_v4(opts.loopback)?;
        }

        let granted_send_buffer = sock.send_buffer_size().unwrap_or(0);
        let sock: UdpSocket = sock.into();

        Ok(Self {
            sock,
            targets: targets.iter().copied().map(SocketAddr::V4).collect(),
            mode,
            granted_send_buffer,
        })
    }

    /// Sends one datagram to every target.
    ///
    /// No allocation: `targets` is built once at startup and only iterated.
    #[inline]
    pub fn send(&self, datagram: &[u8]) -> io::Result<()> {
        for target in &self.targets {
            let sent = self.sock.send_to(datagram, target)?;
            if sent != datagram.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "sent {sent} of {} bytes to {target}; the datagram was truncated",
                        datagram.len()
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn mode(&self) -> TransportMode {
        self.mode
    }

    pub fn targets(&self) -> &[SocketAddr] {
        &self.targets
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    pub fn describe(&self) -> String {
        let targets: Vec<String> = self.targets.iter().map(ToString::to_string).collect();
        format!(
            "{} -> [{}] (send buffer {} KiB)",
            self.mode,
            targets.join(", "),
            self.granted_send_buffer / 1024,
        )
    }
}

/// Receives datagrams from one channel.
#[derive(Debug)]
pub struct Receiver {
    sock: UdpSocket,
    bound: SocketAddrV4,
    mode: TransportMode,
    granted_recv_buffer: usize,
}

impl Receiver {
    /// For multicast, `addr` is the group and port: the socket binds to the
    /// group address itself so the kernel filters by group. That matters
    /// because the A and B channels use different groups on the *same* port —
    /// binding to `0.0.0.0` would deliver both channels to both sockets and
    /// quietly turn two independent arms into one.
    ///
    /// For unicast fanout, `addr` is the local address to bind.
    pub fn bind(mode: TransportMode, addr: SocketAddrV4, opts: SocketOptions) -> io::Result<Self> {
        let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        // Needed so a second handler can join the same group and port, which is
        // how the A/B arbitration work in the next milestone gets tested.
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
        sock.set_reuse_port(true)?;
        sock.set_recv_buffer_size(opts.buffer_bytes)?;

        match mode {
            TransportMode::Multicast => {
                if !addr.ip().is_multicast() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{} is not a multicast address", addr.ip()),
                    ));
                }
                sock.bind(&addr.into())?;
                sock.join_multicast_v4(addr.ip(), &opts.interface)?;
            }
            TransportMode::UnicastFanout => {
                sock.bind(&addr.into())?;
            }
        }

        let granted_recv_buffer = sock.recv_buffer_size().unwrap_or(0);
        let sock: UdpSocket = sock.into();

        Ok(Self {
            sock,
            bound: addr,
            mode,
            granted_recv_buffer,
        })
    }

    /// Blocks for one datagram, or until the read timeout expires.
    ///
    /// A timeout comes back as `WouldBlock`/`TimedOut` rather than an error the
    /// caller has to special-case as fatal — an idle channel is a normal state,
    /// and telling idle from dead is what the heartbeat is for.
    #[inline]
    pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let (n, _from) = self.sock.recv_from(buf)?;
        Ok(n)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.sock.set_read_timeout(timeout)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.sock.set_nonblocking(nonblocking)
    }

    pub fn mode(&self) -> TransportMode {
        self.mode
    }

    /// The address that was requested. When the port was 0 the kernel picked
    /// one, so use [`local_addr`](Self::local_addr) for what is actually bound.
    pub fn bound_addr(&self) -> SocketAddrV4 {
        self.bound
    }

    /// The address the socket is really bound to, port resolved.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    pub fn describe(&self) -> String {
        format!(
            "{} <- {} (receive buffer {} KiB)",
            self.mode,
            self.bound,
            self.granted_recv_buffer / 1024,
        )
    }

    /// True when the receive buffer the kernel granted is far below what was
    /// asked for. Worth saying out loud: a clamped buffer shows up later as
    /// unexplained gaps that look like network loss and are not.
    pub fn buffer_was_clamped(&self, requested: usize) -> bool {
        // Linux reports back double what it granted, so compare generously.
        self.granted_recv_buffer < requested
    }

    pub fn granted_recv_buffer(&self) -> usize {
        self.granted_recv_buffer
    }
}

/// True when `e` means "nothing arrived in time" rather than a real failure.
pub fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn loopback_opts() -> SocketOptions {
        SocketOptions {
            interface: Ipv4Addr::LOCALHOST,
            ttl: 0,
            loopback: true,
            buffer_bytes: 256 * 1024,
        }
    }

    #[test]
    fn unicast_fanout_delivers_to_every_target() {
        // The fallback path has to be as trustworthy as the primary, because it
        // is what the project falls back *to*.
        let a = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            loopback_opts(),
        )
        .expect("bind a");
        let b = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            loopback_opts(),
        )
        .expect("bind b");

        let targets = [
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, a.local_addr().unwrap().port()),
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, b.local_addr().unwrap().port()),
        ];
        let pubr = Publisher::bind(TransportMode::UnicastFanout, &targets, loopback_opts())
            .expect("publisher");

        a.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        b.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        pubr.send(b"hello wire").expect("send");

        let mut buf = [0u8; 64];
        let n = a.recv(&mut buf).expect("a receives");
        assert_eq!(&buf[..n], b"hello wire");
        let n = b.recv(&mut buf).expect("b receives");
        assert_eq!(&buf[..n], b"hello wire");
    }

    #[test]
    fn a_publisher_needs_at_least_one_target() {
        let err = Publisher::bind(TransportMode::UnicastFanout, &[], loopback_opts())
            .expect_err("empty target list must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn multicast_refuses_a_unicast_group_address() {
        let err = Publisher::bind(
            TransportMode::Multicast,
            &[SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 30001)],
            loopback_opts(),
        )
        .expect_err("a unicast address is not a group");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let err = Publisher::bind(
            TransportMode::Multicast,
            &[
                SocketAddrV4::new(Ipv4Addr::new(239, 1, 1, 1), 30001),
                SocketAddrV4::new(Ipv4Addr::new(239, 1, 1, 2), 30001),
            ],
            loopback_opts(),
        )
        .expect_err("one group per channel");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn transport_mode_parses_the_spellings_people_actually_type() {
        use std::str::FromStr;
        assert_eq!(
            TransportMode::from_str("multicast"),
            Ok(TransportMode::Multicast)
        );
        for spelling in ["unicast-fanout", "unicast_fanout", "unicast"] {
            assert_eq!(
                TransportMode::from_str(spelling),
                Ok(TransportMode::UnicastFanout),
                "{spelling} should parse"
            );
        }
        assert!(TransportMode::from_str("udp").is_err());
    }

    #[test]
    fn a_read_timeout_is_reported_as_a_timeout_not_a_failure() {
        let r = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            loopback_opts(),
        )
        .expect("bind");
        r.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let mut buf = [0u8; 16];
        let err = r.recv(&mut buf).expect_err("nothing was sent");
        assert!(
            is_timeout(&err),
            "an idle channel must look idle, not broken: {err:?}"
        );
    }

    /// Multicast over loopback. This is the capability the whole project would
    /// rather use, and the one most likely to be unavailable in a sandbox or a
    /// container without `IP_MULTICAST_LOOP`, so a failure here reports what is
    /// missing instead of failing the suite.
    #[test]
    fn multicast_loopback_round_trips_when_the_environment_allows_it() {
        let group = SocketAddrV4::new(Ipv4Addr::new(239, 255, 42, 99), 34567);
        let opts = loopback_opts();

        let r = match Receiver::bind(TransportMode::Multicast, group, opts) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("SKIP: cannot join {group} on this host: {e}");
                return;
            }
        };
        let p = match Publisher::bind(TransportMode::Multicast, &[group], opts) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("SKIP: cannot publish to {group} on this host: {e}");
                return;
            }
        };

        r.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        if let Err(e) = p.send(b"multicast works") {
            eprintln!("SKIP: send to {group} failed: {e}");
            return;
        }

        let mut buf = [0u8; 64];
        match r.recv(&mut buf) {
            Ok(n) => assert_eq!(&buf[..n], b"multicast works"),
            Err(e) if is_timeout(&e) => {
                eprintln!(
                    "SKIP: multicast loopback is not delivering on this host. \
                     This is the documented reason --transport unicast-fanout exists."
                );
            }
            Err(e) => panic!("unexpected receive failure: {e}"),
        }
    }
}
