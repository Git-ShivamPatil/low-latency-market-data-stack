//! The two small TCP protocols the replay service speaks.
//!
//! Both are binary and fixed-layout, for the same reason the feed is: this is a
//! systems project and a text protocol here would be a different project. They
//! are hand-written rather than generated, because unlike the market-data wire
//! format they are spoken only by Rust and there is no second implementation to
//! drift from.
//!
//! # Uplink — the engine feeding the service
//!
//! ```text
//! hello    := magic(u32) , kind(u8)=1 , reserved(u8) , schemaId(u16) , schemaVersion(u16) , reserved(u16)
//! datagram := length(u32) , bytes[length]
//! ```
//!
//! A stream of length-prefixed datagrams, exactly as published. The service does
//! not need to be told sequence numbers: every datagram already carries
//! `firstSequence` and `messageCount` in its header, so the store indexes itself.
//!
//! **This link must be lossless**, which is why it is TCP rather than another
//! multicast subscriber. A replay service that had itself missed the datagrams a
//! consumer is asking for would be worse than no replay service at all: it would
//! answer confidently and wrongly.
//!
//! # Request — a consumer asking for a range
//!
//! ```text
//! request  := magic(u32) , kind(u8)=2 , reserved(u8) , reserved(u16) , from(u64) , through(u64)
//! response := magic(u32) , status(u8) , reserved(u8) , reserved(u16) ,
//!             datagrams(u32) , firstAvailable(u64) , lastAvailable(u64)
//!             , (length(u32) , bytes[length])*
//! ```
//!
//! The response reports what the service *does* hold even when it cannot serve
//! the request. A consumer that asked too late needs to know it asked too late,
//! not merely that it failed — the difference decides whether it falls back to a
//! snapshot or gives up.

use std::io::{self, Read, Write};

/// "MDRP". Present on both protocols so a mis-wired port fails immediately
/// rather than at the first field that happens to look wrong.
pub const MAGIC: u32 = 0x4D44_5250;

pub const KIND_UPLINK_HELLO: u8 = 1;
pub const KIND_RANGE_REQUEST: u8 = 2;

pub const HELLO_LEN: usize = 12;
pub const REQUEST_LEN: usize = 24;
pub const RESPONSE_HEADER_LEN: usize = 28;

/// The most a single response may carry, to bound the work one request can ask
/// for. A consumer with a bigger hole than this is better served by a snapshot.
pub const MAX_RESPONSE_DATAGRAMS: u32 = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    /// The full range follows.
    Ok = 0,
    /// The range starts before anything still held. The consumer asked too late
    /// and must recover from a snapshot instead.
    TooOld = 1,
    /// The range extends past what the service has seen. Usually means the
    /// consumer is ahead of the service, which is a wiring problem.
    NotYet = 2,
    /// The service is missing datagrams inside the range, so it cannot serve it
    /// honestly. See `DatagramStore` for how a hole gets there.
    Incomplete = 3,
    /// Malformed, or `from > through`.
    BadRequest = 4,
}

impl Status {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Ok,
            1 => Self::TooOld,
            2 => Self::NotYet,
            3 => Self::Incomplete,
            4 => Self::BadRequest,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::TooOld => "the range starts before anything still held",
            Self::NotYet => "the range extends past what the service has seen",
            Self::Incomplete => "the service is missing datagrams inside the range",
            Self::BadRequest => "malformed request",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeRequest {
    pub from: u64,
    pub through: u64,
}

impl RangeRequest {
    pub fn messages(&self) -> u64 {
        self.through.saturating_sub(self.from) + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub status: Status,
    pub datagrams: u32,
    /// The oldest sequence the service still holds, for diagnostics.
    pub first_available: u64,
    /// One past the newest sequence the service holds.
    pub last_available: u64,
}

fn put_u16(buf: &mut [u8], at: usize, v: u16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(buf: &mut [u8], at: usize, v: u64) {
    buf[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([buf[at], buf[at + 1]])
}

fn get_u32(buf: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&buf[at..at + 4]);
    u32::from_le_bytes(a)
}

fn get_u64(buf: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[at..at + 8]);
    u64::from_le_bytes(a)
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

pub fn write_hello(w: &mut impl Write) -> io::Result<()> {
    let mut buf = [0u8; HELLO_LEN];
    put_u32(&mut buf, 0, MAGIC);
    buf[4] = KIND_UPLINK_HELLO;
    put_u16(&mut buf, 6, wire::SCHEMA_ID);
    put_u16(&mut buf, 8, wire::SCHEMA_VERSION);
    w.write_all(&buf)
}

pub fn read_hello(r: &mut impl Read) -> io::Result<()> {
    let mut buf = [0u8; HELLO_LEN];
    r.read_exact(&mut buf)?;
    if get_u32(&buf, 0) != MAGIC {
        return Err(invalid("not a replay uplink: bad magic"));
    }
    if buf[4] != KIND_UPLINK_HELLO {
        return Err(invalid(format!(
            "expected an uplink hello, got kind {}",
            buf[4]
        )));
    }
    let (id, version) = (get_u16(&buf, 6), get_u16(&buf, 8));
    if id != wire::SCHEMA_ID {
        return Err(invalid(format!(
            "uplink speaks schema {id}, this build speaks {}",
            wire::SCHEMA_ID
        )));
    }
    if version != wire::SCHEMA_VERSION {
        return Err(invalid(format!(
            "uplink speaks schema version {version}, this build speaks {}",
            wire::SCHEMA_VERSION
        )));
    }
    Ok(())
}

pub fn write_datagram(w: &mut impl Write, datagram: &[u8]) -> io::Result<()> {
    let len = u32::try_from(datagram.len())
        .map_err(|_| invalid("datagram longer than u32 can express"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(datagram)
}

/// Reads one length-prefixed datagram into `buf`, returning its length.
///
/// `Ok(None)` means the peer closed cleanly between datagrams.
pub fn read_datagram(r: &mut impl Read, buf: &mut [u8]) -> io::Result<Option<usize>> {
    let mut len_bytes = [0u8; 4];
    match r.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > buf.len() {
        return Err(invalid(format!(
            "a {len}-byte datagram does not fit in a {}-byte buffer",
            buf.len()
        )));
    }
    r.read_exact(&mut buf[..len])?;
    Ok(Some(len))
}

pub fn write_request(w: &mut impl Write, req: RangeRequest) -> io::Result<()> {
    let mut buf = [0u8; REQUEST_LEN];
    put_u32(&mut buf, 0, MAGIC);
    buf[4] = KIND_RANGE_REQUEST;
    put_u64(&mut buf, 8, req.from);
    put_u64(&mut buf, 16, req.through);
    w.write_all(&buf)
}

pub fn read_request(r: &mut impl Read) -> io::Result<RangeRequest> {
    let mut buf = [0u8; REQUEST_LEN];
    r.read_exact(&mut buf)?;
    if get_u32(&buf, 0) != MAGIC {
        return Err(invalid("not a replay request: bad magic"));
    }
    if buf[4] != KIND_RANGE_REQUEST {
        return Err(invalid(format!(
            "expected a range request, got kind {}",
            buf[4]
        )));
    }
    Ok(RangeRequest {
        from: get_u64(&buf, 8),
        through: get_u64(&buf, 16),
    })
}

pub fn write_response_header(w: &mut impl Write, h: ResponseHeader) -> io::Result<()> {
    let mut buf = [0u8; RESPONSE_HEADER_LEN];
    put_u32(&mut buf, 0, MAGIC);
    buf[4] = h.status as u8;
    put_u32(&mut buf, 8, h.datagrams);
    put_u64(&mut buf, 12, h.first_available);
    put_u64(&mut buf, 20, h.last_available);
    w.write_all(&buf)
}

pub fn read_response_header(r: &mut impl Read) -> io::Result<ResponseHeader> {
    let mut buf = [0u8; RESPONSE_HEADER_LEN];
    r.read_exact(&mut buf)?;
    if get_u32(&buf, 0) != MAGIC {
        return Err(invalid("not a replay response: bad magic"));
    }
    let status = Status::from_u8(buf[4])
        .ok_or_else(|| invalid(format!("unknown response status {}", buf[4])))?;
    Ok(ResponseHeader {
        status,
        datagrams: get_u32(&buf, 8),
        first_available: get_u64(&buf, 12),
        last_available: get_u64(&buf, 20),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hello_round_trips() {
        let mut buf = Vec::new();
        write_hello(&mut buf).unwrap();
        assert_eq!(buf.len(), HELLO_LEN);
        read_hello(&mut buf.as_slice()).unwrap();
    }

    #[test]
    fn a_hello_on_the_wrong_port_is_rejected_immediately() {
        // Pointing the engine at the request port instead of the uplink port is
        // the obvious wiring mistake; it must fail at the handshake rather than
        // by misparsing a datagram much later.
        let mut buf = Vec::new();
        write_request(
            &mut buf,
            RangeRequest {
                from: 1,
                through: 2,
            },
        )
        .unwrap();
        let err = read_hello(&mut buf.as_slice()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn garbage_is_rejected_rather_than_interpreted() {
        let junk = [0xFFu8; 32];
        assert!(read_hello(&mut junk.as_slice()).is_err());
        assert!(read_request(&mut junk.as_slice()).is_err());
        assert!(read_response_header(&mut junk.as_slice()).is_err());
    }

    #[test]
    fn a_request_round_trips() {
        let req = RangeRequest {
            from: 0xDEAD_BEEF,
            through: 0xDEAD_BEEF + 99,
        };
        let mut buf = Vec::new();
        write_request(&mut buf, req).unwrap();
        assert_eq!(buf.len(), REQUEST_LEN);
        assert_eq!(read_request(&mut buf.as_slice()).unwrap(), req);
        assert_eq!(req.messages(), 100);
    }

    #[test]
    fn a_response_header_round_trips_every_status() {
        for status in [
            Status::Ok,
            Status::TooOld,
            Status::NotYet,
            Status::Incomplete,
            Status::BadRequest,
        ] {
            let h = ResponseHeader {
                status,
                datagrams: 7,
                first_available: 1_000,
                last_available: 2_000,
            };
            let mut buf = Vec::new();
            write_response_header(&mut buf, h).unwrap();
            assert_eq!(buf.len(), RESPONSE_HEADER_LEN);
            assert_eq!(read_response_header(&mut buf.as_slice()).unwrap(), h);
        }
    }

    #[test]
    fn datagrams_round_trip_and_a_clean_close_is_not_an_error() {
        let mut stream = Vec::new();
        write_datagram(&mut stream, &[1, 2, 3]).unwrap();
        write_datagram(&mut stream, &[4, 5]).unwrap();

        let mut r = stream.as_slice();
        let mut buf = [0u8; 64];
        assert_eq!(read_datagram(&mut r, &mut buf).unwrap(), Some(3));
        assert_eq!(&buf[..3], &[1, 2, 3]);
        assert_eq!(read_datagram(&mut r, &mut buf).unwrap(), Some(2));
        assert_eq!(&buf[..2], &[4, 5]);
        assert_eq!(
            read_datagram(&mut r, &mut buf).unwrap(),
            None,
            "a peer closing between datagrams is normal, not a failure"
        );
    }

    #[test]
    fn a_datagram_too_large_for_the_buffer_is_an_error_not_an_overrun() {
        let mut stream = Vec::new();
        write_datagram(&mut stream, &[0u8; 100]).unwrap();
        let mut small = [0u8; 16];
        assert!(read_datagram(&mut stream.as_slice(), &mut small).is_err());
    }

    #[test]
    fn a_truncated_datagram_is_an_error_not_a_short_read() {
        let mut stream = Vec::new();
        write_datagram(&mut stream, &[1, 2, 3, 4, 5]).unwrap();
        stream.truncate(stream.len() - 2);
        let mut buf = [0u8; 64];
        assert!(read_datagram(&mut stream.as_slice(), &mut buf).is_err());
    }
}
