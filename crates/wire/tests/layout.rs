//! Layout and rejection tests for the generated Rust codec.
//!
//! The golden suite proves the two languages agree on well-formed input. This
//! covers the other half: what the decoder does with input that is wrong. A feed
//! handler sees malformed datagrams as a matter of routine — a truncated packet,
//! a stale schema, a byte flipped in flight — and every one of those has to
//! produce a rejection rather than a panic or a read past the end of the buffer.
//!
//! `cpp/wire/tests/layout_test.cpp` asserts the same properties against the
//! generated C++ header.

use wire::*;

fn one_add_order() -> Vec<u8> {
    let mut buf = vec![0u8; 256];
    let mut w = PacketWriter::new(&mut buf, 0, 0, 100, 7).expect("header");
    w.add_order(1, 1000, 5, 2, Side::Bid).expect("add order");
    let n = w.finish();
    buf.truncate(n);
    buf
}

fn snapshot_with_levels(levels: u16) -> Vec<u8> {
    let mut buf = vec![0u8; 1024];
    let mut w = PacketWriter::new(&mut buf, 0, PACKET_FLAG_SNAPSHOT, 1, 2).expect("header");
    let n = {
        let mut e = SnapshotEncoder::start(w.tail(), 9, 4, 1).expect("snapshot start");
        for i in 0..levels {
            e.push_level(1000 + i64::from(i), 10, 1, Side::Bid)
                .expect("push level");
        }
        e.finish()
    };
    w.commit(n).expect("commit");
    let total = w.finish();
    buf.truncate(total);
    buf
}

#[test]
fn constants_match_the_schema() {
    assert_eq!(SCHEMA_ID, 1);
    assert_eq!(SCHEMA_VERSION, 1);
    assert_eq!(PRICE_EXPONENT, -4);
    assert_eq!(PACKET_HEADER_LEN, 24);
    assert_eq!(MESSAGE_HEADER_LEN, 8);
    assert_eq!(GROUP_SIZE_ENCODING_LEN, 4);
    assert_eq!(AddOrderDecoder::BLOCK_LENGTH, 24);
    assert_eq!(ModifyOrderDecoder::BLOCK_LENGTH, 24);
    assert_eq!(DeleteOrderDecoder::BLOCK_LENGTH, 12);
    assert_eq!(TradeDecoder::BLOCK_LENGTH, 40);
    assert_eq!(SnapshotDecoder::BLOCK_LENGTH, 12);
    assert_eq!(HeartbeatDecoder::BLOCK_LENGTH, 8);
    assert_eq!(SequenceResetDecoder::BLOCK_LENGTH, 8);
}

#[test]
fn a_written_packet_reads_back() {
    let buf = one_add_order();
    let r = PacketReader::new(&buf).expect("reader");
    assert_eq!(r.header().message_count(), 1);
    assert_eq!(r.header().first_sequence(), 100);
    assert!(!r.header().is_snapshot());

    let msgs: Vec<_> = r
        .messages()
        .collect::<Result<Vec<_>, _>>()
        .expect("messages");
    assert_eq!(msgs.len(), 1);
    let (seq, msg) = msgs[0];
    assert_eq!(seq, 100);
    let Message::AddOrder(d) = msg else {
        panic!("expected AddOrder")
    };
    assert_eq!(d.order_id(), 1);
    assert_eq!(d.price(), 1000);
    assert_eq!(d.side().expect("side"), Side::Bid);
    assert_eq!(d.total_len(), MESSAGE_HEADER_LEN + 24);
}

/// Sequence is derived, not carried, so this is the property the whole batching
/// decision rests on.
#[test]
fn sequences_are_derived_from_the_packet_header() {
    let mut buf = vec![0u8; 512];
    let mut w = PacketWriter::new(&mut buf, 0, 0, 1_000, 0).expect("header");
    for i in 0..5u64 {
        w.add_order(i, 10, 1, 0, Side::Bid).expect("add order");
    }
    let n = w.finish();

    let r = PacketReader::new(&buf[..n]).expect("reader");
    let seqs: Vec<u64> = r.messages().map(|m| m.expect("message").0).collect();
    assert_eq!(seqs, vec![1_000, 1_001, 1_002, 1_003, 1_004]);
}

#[test]
fn every_truncation_is_rejected_rather_than_panicking() {
    let buf = one_add_order();
    for n in 0..PACKET_HEADER_LEN {
        assert!(
            PacketReader::new(&buf[..n]).is_err(),
            "a {n}-byte buffer must not wrap as a packet header"
        );
    }
    let msg = &buf[PACKET_HEADER_LEN..];
    for n in 0..msg.len() {
        assert!(
            AddOrderDecoder::wrap(&msg[..n]).is_err(),
            "a {n}-byte buffer must not wrap as an AddOrder"
        );
    }
}

/// The group case reads the group header to learn how long the message is, so it
/// has to prove that header is present before reading it. Getting this wrong is
/// a panic on a truncated datagram, not a decode error.
#[test]
fn every_truncation_of_a_group_message_is_rejected() {
    let buf = snapshot_with_levels(3);
    let msg = &buf[PACKET_HEADER_LEN..];
    for n in 0..msg.len() {
        assert!(
            SnapshotDecoder::wrap(&msg[..n]).is_err(),
            "a {n}-byte buffer must not wrap as a Snapshot"
        );
    }
    let full = SnapshotDecoder::wrap(msg).expect("full snapshot");
    assert_eq!(full.levels_count(), 3);
    assert_eq!(full.total_len(), msg.len());
    assert_eq!(full.levels().count(), 3);
}

#[test]
fn an_empty_group_still_carries_its_header() {
    let buf = snapshot_with_levels(0);
    let msg = &buf[PACKET_HEADER_LEN..];
    let d = SnapshotDecoder::wrap(msg).expect("empty snapshot");
    assert_eq!(d.levels_count(), 0);
    assert_eq!(d.levels().count(), 0);
    assert_eq!(
        d.total_len(),
        MESSAGE_HEADER_LEN + 12 + GROUP_SIZE_ENCODING_LEN
    );
}

#[test]
fn a_message_does_not_decode_as_the_wrong_template() {
    let buf = one_add_order();
    let msg = &buf[PACKET_HEADER_LEN..];
    assert!(matches!(
        TradeDecoder::wrap(msg),
        Err(WireError::TemplateMismatch {
            expected: 4,
            got: 1
        })
    ));
}

/// A publisher on a later schema version sends a larger root block. This build
/// must read the fields it knows and skip the rest.
#[test]
fn a_larger_root_block_is_skipped_not_misparsed() {
    let mut buf = vec![0u8; 256];
    let n = encode_add_order(&mut buf, 42, 1234, 9, 3, Side::Ask).expect("encode");
    // Widen the root block by 8 bytes, as a future schema version would.
    buf[0..2].copy_from_slice(&(24u16 + 8).to_le_bytes());
    let d = AddOrderDecoder::wrap(&buf[..n + 8]).expect("wider block still wraps");
    assert_eq!(d.order_id(), 42);
    assert_eq!(d.price(), 1234);
    assert_eq!(
        d.total_len(),
        MESSAGE_HEADER_LEN + 32,
        "total_len must follow the wire block length, not the constant"
    );
}

#[test]
fn a_root_block_smaller_than_this_build_needs_is_rejected() {
    let mut buf = vec![0u8; 256];
    let n = encode_add_order(&mut buf, 42, 1234, 9, 3, Side::Ask).expect("encode");
    buf[0..2].copy_from_slice(&16u16.to_le_bytes());
    assert!(matches!(
        AddOrderDecoder::wrap(&buf[..n]),
        Err(WireError::BlockTooSmall {
            message: "AddOrder",
            needed: 24,
            got: 16
        })
    ));
}

#[test]
fn an_undefined_enum_value_is_rejected_by_the_validating_accessor() {
    let mut buf = vec![0u8; 256];
    let n = encode_add_order(&mut buf, 1, 1, 1, 1, Side::Bid).expect("encode");
    // side sits at root offset 22.
    buf[MESSAGE_HEADER_LEN + 22] = 7;
    let d = AddOrderDecoder::wrap(&buf[..n]).expect("still wraps");
    assert_eq!(
        d.side_raw(),
        7,
        "the raw accessor reports what is on the wire"
    );
    assert!(matches!(
        d.side(),
        Err(WireError::InvalidEnum {
            name: "Side",
            value: 7
        })
    ));
}

#[test]
fn a_foreign_schema_id_is_rejected_at_the_packet_header() {
    let mut buf = one_add_order();
    buf[0..2].copy_from_slice(&99u16.to_le_bytes());
    assert!(matches!(
        PacketReader::new(&buf),
        Err(WireError::SchemaMismatch {
            expected: 1,
            got: 99
        })
    ));
}

/// A datagram claiming more messages than it carries must report an error, not
/// silently yield fewer. Undercounting a batch is how a feed handler loses
/// messages without ever noticing a gap.
#[test]
fn a_lying_message_count_produces_an_error_not_a_short_read() {
    let mut buf = one_add_order();
    buf[4..6].copy_from_slice(&3u16.to_le_bytes());
    let r = PacketReader::new(&buf).expect("header still valid");
    let results: Vec<_> = r.messages().collect();
    assert_eq!(results.len(), 2, "one good message, then one error");
    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(WireError::ShortBuffer { .. })));
}

#[test]
fn encoders_refuse_to_write_past_the_end_of_the_buffer() {
    let mut small = [0u8; 8];
    assert!(matches!(
        encode_add_order(&mut small, 1, 1, 1, 1, Side::Bid),
        Err(WireError::ShortBuffer { needed: 32, got: 8 })
    ));

    let mut tiny = [0u8; PACKET_HEADER_LEN + 8];
    let mut w = PacketWriter::new(&mut tiny, 0, 0, 0, 0).expect("header fits");
    assert!(w.add_order(1, 1, 1, 1, Side::Bid).is_err());
    assert_eq!(w.message_count(), 0, "a failed append must not be counted");
}

#[test]
fn a_group_encoder_refuses_to_overrun_its_buffer() {
    let mut buf = vec![0u8; MESSAGE_HEADER_LEN + 12 + GROUP_SIZE_ENCODING_LEN + 16];
    let mut e = SnapshotEncoder::start(&mut buf, 1, 1, 0).expect("start");
    assert!(
        e.push_level(1, 1, 1, Side::Bid).is_ok(),
        "the first entry fits"
    );
    assert!(
        e.push_level(2, 1, 1, Side::Bid).is_err(),
        "the second must not run past the buffer"
    );
    let n = e.finish();
    let d = SnapshotDecoder::wrap(&buf[..n]).expect("wrap");
    assert_eq!(
        d.levels_count(),
        1,
        "the rejected entry must not be counted"
    );
}
