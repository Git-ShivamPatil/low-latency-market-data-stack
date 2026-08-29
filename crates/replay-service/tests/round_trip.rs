//! The replay service over a real TCP socket, end to end.
//!
//! The store and the protocol have their own unit tests. This covers the thing
//! neither can: that a client and a server built from them actually agree once a
//! socket is between them — framing, ordering, and the failure answers.

use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use replay_service::protocol::{self, ResponseHeader, Status};
use replay_service::store::DatagramStore;
use replay_service::{request, RangeRequest};
use wire::{PacketWriter, Side};

const BATCH: u16 = 10;

fn datagram(first: u64) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let mut w = PacketWriter::new(&mut buf, 0, 0, first, 0).unwrap();
    for i in 0..BATCH {
        w.add_order(first + u64::from(i), 1_000_000, 1, 1, Side::Bid)
            .unwrap();
    }
    let n = w.finish();
    buf.truncate(n);
    buf
}

/// A store holding `datagrams` datagrams starting at sequence 1.
fn filled(capacity: usize, datagrams: usize) -> DatagramStore {
    let mut s = DatagramStore::new(capacity, 4096);
    for i in 0..datagrams {
        s.push(&datagram(1 + (i as u64) * u64::from(BATCH)))
            .unwrap();
    }
    s
}

/// Serves exactly one request from `store`, then closes.
///
/// This is the server binary's `serve_request` in miniature. Duplicated rather
/// than shared because the binary owns its threading and this test owns its
/// determinism; what is being checked is the protocol, and both sides of it are
/// here.
fn serve_once(store: DatagramStore) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut r = BufReader::new(stream.try_clone().unwrap());
        let mut w = BufWriter::new(stream);
        let req = protocol::read_request(&mut r).unwrap();
        let found = store.locate(req.from, req.through);
        protocol::write_response_header(
            &mut w,
            ResponseHeader {
                status: found.status,
                datagrams: if found.status == Status::Ok {
                    (found.end - found.start) as u32
                } else {
                    0
                },
                first_available: store.first_sequence(),
                last_available: store.next_sequence(),
            },
        )
        .unwrap();
        if found.status == Status::Ok {
            for i in found.start..found.end {
                protocol::write_datagram(&mut w, store.datagram_at(i).unwrap()).unwrap();
            }
        }
        w.flush().unwrap();
    });
    addr
}

fn ask(addr: SocketAddr, from: u64, through: u64) -> replay_service::ReplayResult {
    request(
        addr,
        RangeRequest { from, through },
        Duration::from_secs(5),
        4096,
    )
    .expect("the request should reach the service")
}

#[test]
fn a_served_range_arrives_complete_and_in_order() {
    let addr = serve_once(filled(64, 20)); // sequences 1..=200
    let result = ask(addr, 55, 84);

    assert_eq!(result.header.status, Status::Ok);
    assert!(result.is_ok());
    assert_eq!(result.header.first_available, 1);
    assert_eq!(result.header.last_available, 201);
    assert!(!result.datagrams.is_empty());

    // Every sequence in the requested range must be present, exactly once, in
    // order. That is the whole contract: a replay with a hole in it would be
    // worse than a refusal, because the consumer asked for help.
    let mut covered = Vec::new();
    for d in &result.datagrams {
        let h = wire::PacketHeaderDecoder::wrap(d).unwrap();
        for k in 0..u64::from(h.message_count()) {
            covered.push(h.first_sequence() + k);
        }
    }
    for w in covered.windows(2) {
        assert_eq!(w[1], w[0] + 1, "the replay must be contiguous");
    }
    assert!(covered.first().is_some_and(|&f| f <= 55));
    assert!(covered.last().is_some_and(|&l| l >= 84));
}

#[test]
fn a_range_that_aged_out_is_refused_with_the_horizon_reported() {
    // capacity 4 keeps only the last 4 datagrams of 20: sequences 161..=200.
    let addr = serve_once(filled(4, 20));
    let result = ask(addr, 1, 50);

    assert_eq!(result.header.status, Status::TooOld);
    assert!(result.datagrams.is_empty());
    // The consumer needs to know it asked too late, not merely that it failed:
    // the difference decides whether it falls back to a snapshot or gives up.
    assert_eq!(result.header.first_available, 161);
    assert_eq!(result.header.last_available, 201);
}

#[test]
fn a_range_past_the_end_is_refused_rather_than_partly_served() {
    let addr = serve_once(filled(64, 5)); // 1..=50
    let result = ask(addr, 40, 90);
    assert_eq!(result.header.status, Status::NotYet);
    assert!(
        result.datagrams.is_empty(),
        "a partial answer to a range request is a hole the consumer would not know about"
    );
}

#[test]
fn an_inverted_range_is_a_bad_request() {
    let addr = serve_once(filled(64, 5));
    let result = ask(addr, 40, 10);
    assert_eq!(result.header.status, Status::BadRequest);
}

#[test]
fn a_single_message_range_is_served() {
    let addr = serve_once(filled(64, 20));
    let result = ask(addr, 77, 77);
    assert_eq!(result.header.status, Status::Ok);
    assert_eq!(result.datagrams.len(), 1);
}

#[test]
fn a_service_that_is_not_listening_is_an_error_not_a_hang() {
    // The consumer's answer to this is to fall back to the snapshot cycle, so it
    // has to come back promptly rather than block the recovery.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let result = request(
        dead,
        RangeRequest {
            from: 1,
            through: 10,
        },
        Duration::from_millis(250),
        4096,
    );
    assert!(result.is_err());
}

/// The uplink half: datagrams streamed in come back out of the store intact.
#[test]
fn the_uplink_handshake_and_stream_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut r = BufReader::new(stream);
        protocol::read_hello(&mut r).unwrap();
        let mut store = DatagramStore::new(64, 4096);
        let mut buf = vec![0u8; 4096];
        while let Some(n) = protocol::read_datagram(&mut r, &mut buf).unwrap() {
            store.push(&buf[..n]).unwrap();
        }
        store
    });

    {
        let stream = TcpStream::connect(addr).unwrap();
        let mut w = BufWriter::new(stream);
        protocol::write_hello(&mut w).unwrap();
        for i in 0..10 {
            protocol::write_datagram(&mut w, &datagram(1 + i * u64::from(BATCH))).unwrap();
        }
        w.flush().unwrap();
        // Dropping the writer closes the connection, which the server reads as a
        // clean end of stream rather than an error.
    }

    let store = server.join().unwrap();
    assert_eq!(store.len(), 10);
    assert_eq!(store.first_sequence(), 1);
    assert_eq!(store.next_sequence(), 101);
    assert_eq!(store.stats().discontinuities, 0);
}

/// A consumer pointed at the uplink port instead of the request port must fail
/// at the handshake, not by misreading a datagram much later.
#[test]
fn a_mis_wired_port_fails_at_the_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut r = BufReader::new(stream);
        // This side expects an uplink hello; the client will send a request.
        let _ = protocol::read_hello(&mut r);
    });

    let result = request(
        addr,
        RangeRequest {
            from: 1,
            through: 10,
        },
        Duration::from_millis(500),
        4096,
    );
    assert!(
        result.is_err(),
        "the server closes without a response, which the client must report"
    );
}
