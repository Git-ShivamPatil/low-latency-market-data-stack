//! Asking the replay service for a range, from a consumer.
//!
//! # Why this runs on its own thread
//!
//! A replay request is a TCP connect, a round trip, and however long it takes to
//! stream back the datagrams. Doing that inline on a feed handler's receive loop
//! would stop it reading its sockets for the duration — and a handler that stops
//! reading loses datagrams, which is how a recovery from one gap manufactures the
//! next. So [`request_in_background`] spawns a thread and hands back a receiver
//! the caller polls.
//!
//! The handler keeps one request outstanding at a time and continues buffering
//! live traffic while it waits, exactly as it does when waiting for a snapshot.
//! Replay is simply a faster and more complete way to reach the same state.

use std::io::{self, BufReader, BufWriter};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use crate::protocol::{self, RangeRequest, ResponseHeader, Status, MAX_RESPONSE_DATAGRAMS};

/// What came back from a replay request.
#[derive(Debug)]
pub struct ReplayResult {
    pub request: RangeRequest,
    pub header: ResponseHeader,
    /// The datagrams covering the range, in sequence order. Empty unless
    /// `header.status` is [`Status::Ok`].
    pub datagrams: Vec<Vec<u8>>,
}

impl ReplayResult {
    pub fn is_ok(&self) -> bool {
        self.header.status == Status::Ok
    }
}

/// Requests `range` from `addr`, blocking until it is served or fails.
pub fn request(
    addr: SocketAddr,
    range: RangeRequest,
    timeout: Duration,
    max_datagram_bytes: usize,
) -> io::Result<ReplayResult> {
    let stream = TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    // The request is 24 bytes and the response starts with a 28-byte header;
    // batching them into one segment each keeps the exchange to two round trips.
    stream.set_nodelay(true)?;

    let mut w = BufWriter::new(stream.try_clone()?);
    protocol::write_request(&mut w, range)?;
    io::Write::flush(&mut w)?;

    let mut r = BufReader::new(stream);
    let header = protocol::read_response_header(&mut r)?;

    let mut datagrams = Vec::new();
    if header.status == Status::Ok {
        if header.datagrams > MAX_RESPONSE_DATAGRAMS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the service offered {} datagrams, over the {MAX_RESPONSE_DATAGRAMS} cap",
                    header.datagrams
                ),
            ));
        }
        let mut buf = vec![0u8; max_datagram_bytes];
        for i in 0..header.datagrams {
            match protocol::read_datagram(&mut r, &mut buf)? {
                Some(n) => datagrams.push(buf[..n].to_vec()),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "the service promised {} datagrams and closed after {i}",
                            header.datagrams
                        ),
                    ))
                }
            }
        }
    }

    Ok(ReplayResult {
        request: range,
        header,
        datagrams,
    })
}

/// Runs [`request`] on its own thread. The caller polls the receiver.
///
/// Errors are delivered through the channel rather than panicking the thread: a
/// replay service being down is an ordinary condition, and the consumer's answer
/// to it is to fall back to the snapshot cycle rather than to stop.
pub fn request_in_background(
    addr: SocketAddr,
    range: RangeRequest,
    timeout: Duration,
    max_datagram_bytes: usize,
) -> Receiver<io::Result<ReplayResult>> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(request(addr, range, timeout, max_datagram_bytes));
    });
    rx
}
