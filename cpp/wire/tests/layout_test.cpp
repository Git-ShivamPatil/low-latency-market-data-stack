// Layout and rejection tests for the generated C++ codec.
//
// The golden test proves the two languages agree on well-formed input. This one
// covers the other half: what the decoder does with input that is wrong. A feed
// handler sees malformed datagrams as a matter of routine -- a truncated packet,
// a stale schema, a byte flipped in flight -- and every one of those has to
// produce a rejection rather than a read past the end of the buffer.

#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "wire/generated.hpp"

using namespace mdstack::wire;

namespace {

int failures = 0;

void check(bool ok, const std::string& what) {
    if (ok) {
        std::cout << "ok   " << what << "\n";
    } else {
        std::cerr << "FAIL " << what << "\n";
        ++failures;
    }
}

std::vector<std::byte> encode_one_add_order() {
    std::vector<std::byte> buf(256);
    auto ph = encode_packet_header(buf.data(), buf.size(),
                                   static_cast<std::uint8_t>(0),
                                   static_cast<std::uint8_t>(0), 100ULL, 7ULL);
    const std::size_t pos = *ph;
    auto n = encode_add_order(buf.data() + pos, buf.size() - pos, 1ULL, 1000LL,
                              static_cast<std::uint32_t>(5),
                              static_cast<std::uint16_t>(2), Side::kBid);
    buf.resize(pos + *n);
    (void)patch_message_count(buf.data(), buf.size(), 1);
    return buf;
}

std::vector<std::byte> encode_snapshot_with_levels(std::uint16_t levels) {
    std::vector<std::byte> buf(1024);
    auto ph = encode_packet_header(buf.data(), buf.size(),
                                   static_cast<std::uint8_t>(0),
                                   static_cast<std::uint8_t>(kPacketFlagSnapshot),
                                   1ULL, 2ULL);
    std::size_t pos = *ph;
    auto e = SnapshotEncoder::start(buf.data() + pos, buf.size() - pos, 9ULL,
                                   static_cast<std::uint16_t>(4),
                                   static_cast<std::uint8_t>(1));
    for (std::uint16_t i = 0; i < levels; ++i) {
        (void)e->push_level(static_cast<std::int64_t>(1000 + i),
                            static_cast<std::uint32_t>(10),
                            static_cast<std::uint16_t>(1), Side::kBid);
    }
    pos += e->finish();
    buf.resize(pos);
    (void)patch_message_count(buf.data(), buf.size(), 1);
    return buf;
}

}  // namespace

int main() {
    // --- constants -------------------------------------------------------
    check(kSchemaId == 1, "schema id is 1");
    check(kSchemaVersion == 1, "schema version is 1");
    check(kPriceExponent == -4, "price exponent is -4");
    check(kPacketHeaderLen == 24, "packet header is 24 bytes");
    check(kMessageHeaderLen == 8, "message header is 8 bytes");
    check(kGroupSizeEncodingLen == 4, "group size encoding is 4 bytes");
    check(AddOrderDecoder::kBlockLength == 24, "AddOrder block is 24 bytes");
    check(ModifyOrderDecoder::kBlockLength == 24, "ModifyOrder block is 24 bytes");
    check(DeleteOrderDecoder::kBlockLength == 12, "DeleteOrder block is 12 bytes");
    check(TradeDecoder::kBlockLength == 40, "Trade block is 40 bytes");
    check(SnapshotDecoder::kBlockLength == 12, "Snapshot root block is 12 bytes");
    check(HeartbeatDecoder::kBlockLength == 8, "Heartbeat block is 8 bytes");
    check(SequenceResetDecoder::kBlockLength == 8, "SequenceReset block is 8 bytes");

    // --- round trip ------------------------------------------------------
    {
        auto buf = encode_one_add_order();
        auto h = PacketHeaderDecoder::wrap(buf.data(), buf.size());
        check(h.has_value(), "well-formed packet header wraps");
        check(h && h->message_count() == 1, "messageCount survives the patch");
        auto d = AddOrderDecoder::wrap(buf.data() + kPacketHeaderLen,
                                       buf.size() - kPacketHeaderLen);
        check(d.has_value(), "well-formed AddOrder wraps");
        check(d && d->order_id() == 1, "orderId round-trips");
        check(d && d->price() == 1000, "price round-trips");
        check(d && d->side() == Side::kBid, "side round-trips");
        check(d && d->total_len() == kMessageHeaderLen + 24, "AddOrder total_len");
    }

    // --- rejection: short buffers ---------------------------------------
    {
        auto buf = encode_one_add_order();
        bool all_rejected = true;
        for (std::size_t n = 0; n < kPacketHeaderLen; ++n) {
            if (PacketHeaderDecoder::wrap(buf.data(), n).has_value()) {
                all_rejected = false;
            }
        }
        check(all_rejected, "every truncation of a packet header is rejected");

        const std::byte* msg = buf.data() + kPacketHeaderLen;
        const std::size_t msg_len = buf.size() - kPacketHeaderLen;
        all_rejected = true;
        for (std::size_t n = 0; n < msg_len; ++n) {
            if (AddOrderDecoder::wrap(msg, n).has_value()) all_rejected = false;
        }
        check(all_rejected, "every truncation of an AddOrder is rejected");
    }

    // --- rejection: a truncated group -----------------------------------
    //
    // This is the case that reads the group header to find out how long the
    // message is, so it has to prove the header is present before reading it.
    {
        auto buf = encode_snapshot_with_levels(3);
        const std::byte* msg = buf.data() + kPacketHeaderLen;
        const std::size_t msg_len = buf.size() - kPacketHeaderLen;
        bool all_rejected = true;
        for (std::size_t n = 0; n < msg_len; ++n) {
            if (SnapshotDecoder::wrap(msg, n).has_value()) all_rejected = false;
        }
        check(all_rejected, "every truncation of a Snapshot is rejected");

        auto full = SnapshotDecoder::wrap(msg, msg_len);
        check(full.has_value(), "the untruncated Snapshot still wraps");
        check(full && full->levels_count() == 3, "levels count survives");
        check(full && full->total_len() == msg_len, "Snapshot total_len is exact");
    }

    // --- an empty group still carries its header ------------------------
    {
        auto buf = encode_snapshot_with_levels(0);
        const std::byte* msg = buf.data() + kPacketHeaderLen;
        auto d = SnapshotDecoder::wrap(msg, buf.size() - kPacketHeaderLen);
        check(d.has_value(), "a Snapshot with no levels wraps");
        check(d && d->levels_count() == 0, "empty group reports zero entries");
        check(d && d->total_len() == kMessageHeaderLen + 12 + kGroupSizeEncodingLen,
              "an empty group still occupies its 4-byte header");
    }

    // --- rejection: wrong template --------------------------------------
    {
        auto buf = encode_one_add_order();
        const std::byte* msg = buf.data() + kPacketHeaderLen;
        const std::size_t msg_len = buf.size() - kPacketHeaderLen;
        check(!TradeDecoder::wrap(msg, msg_len).has_value(),
              "an AddOrder does not decode as a Trade");
    }

    // --- forward compatibility ------------------------------------------
    //
    // A publisher on a later schema version sends a larger root block. This
    // build must read the fields it knows and skip the rest, not misparse.
    {
        std::vector<std::byte> buf(256);
        auto n = encode_add_order(buf.data(), buf.size(), 42ULL, 1234LL,
                                  static_cast<std::uint32_t>(9),
                                  static_cast<std::uint16_t>(3), Side::kAsk);
        // Widen the root block by 8 bytes, as a future version would.
        detail::store_le<std::uint16_t>(buf.data(), static_cast<std::uint16_t>(24 + 8));
        auto d = AddOrderDecoder::wrap(buf.data(), *n + 8);
        check(d.has_value(), "a larger root block still wraps");
        check(d && d->order_id() == 42, "known fields still read correctly");
        check(d && d->total_len() == kMessageHeaderLen + 32,
              "total_len follows the wire block length, not the constant");

        // A smaller root block is the opposite case and must be rejected:
        // fields this build needs are simply not present.
        detail::store_le<std::uint16_t>(buf.data(), static_cast<std::uint16_t>(16));
        check(!AddOrderDecoder::wrap(buf.data(), *n).has_value(),
              "a root block smaller than this build needs is rejected");
    }

    // --- enum validation -------------------------------------------------
    {
        std::vector<std::byte> buf(256);
        auto n = encode_add_order(buf.data(), buf.size(), 1ULL, 1LL,
                                  static_cast<std::uint32_t>(1),
                                  static_cast<std::uint16_t>(1), Side::kBid);
        (void)n;
        // side sits at root offset 22.
        buf[kMessageHeaderLen + 22] = std::byte{7};
        auto d = AddOrderDecoder::wrap(buf.data(), buf.size());
        check(d.has_value(), "an undefined enum does not stop the message wrapping");
        check(d && d->side_raw() == 7, "the raw accessor reports what is on the wire");
        check(d && !d->side().has_value(), "the validating accessor rejects it");
    }

    // --- schema rejection ------------------------------------------------
    {
        auto buf = encode_one_add_order();
        detail::store_le<std::uint16_t>(buf.data(), static_cast<std::uint16_t>(99));
        check(!PacketHeaderDecoder::wrap(buf.data(), buf.size()).has_value(),
              "a foreign schema id is rejected at the packet header");
    }

    std::cout << (failures == 0 ? "layout tests passed\n" : "layout tests FAILED\n");
    return failures == 0 ? 0 : 1;
}
