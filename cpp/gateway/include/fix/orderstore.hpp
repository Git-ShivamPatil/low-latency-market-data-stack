// Durable order state: a write-ahead log the gateway can rebuild from.
//
// # What this protects
//
// `SeqStore` keeps the two numbers a FIX session cannot reconstruct. This keeps
// the other thing it cannot reconstruct: which orders were live when the process
// died. Sequence numbers let the session resume; order state is what lets the
// session resume *meaning something*, because a gateway that comes back not
// knowing what it has working cannot answer a cancel, cannot report a position,
// and cannot tell whether the fill it just received belongs to anything.
//
// # The ordering rule, again
//
// A record is appended and **`fsync`'d before the message it describes leaves
// for the ring**. Same rule as `SeqStore`, same reason: if the process dies
// between sending and persisting, it comes back believing it sent less than it
// did. For sequence numbers that produces a reversal; for orders it produces an
// order live at the exchange that nothing here knows about, which is worse
// because nothing will ever notice on its own.
//
// The fsync policy is *every transition, synchronously*. That is the expensive
// choice and it is the correct one for order state. It is written down here and
// in docs/ORDER-PATH.md so that no future benchmark quietly relaxes it and
// measures a different guarantee.
//
// # A torn tail is not corruption
//
// `SIGKILL` during a write leaves a partial record at the end of the file. That
// is the normal outcome, not damage: the record was never `fsync`'d, so the
// message it describes was never sent, so discarding it is exactly right. Every
// record carries a checksum and a monotonic number, and replay stops at the
// first record that fails either. The bytes discarded are counted rather than
// silently dropped.
//
// A bad record with good records *after* it is a different thing entirely and
// gets a different answer: the file is refused. Stopping there quietly would
// drop every order past the damage while reporting success, and those orders
// are live at the exchange whether this process can read them or not.
//
// # It compacts at startup
//
// An append-only log that never compacts is a disk-space bug with a delay
// fuse. After replay the live set is known exactly, so the log is rewritten to
// contain only it: temp file, fsync, rename, fsync the directory. Only at
// startup -- compacting a running log needs a story about what happens to a
// record written during the rewrite, and nothing here needs one.

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "fix/seqstore.hpp"
#include "wire/generated.hpp"

namespace fix {

/// Where an order has got to. Ordered so that a terminal state is anything
/// greater than `Live`, which is the only comparison replay needs.
enum class OrderState : std::uint8_t {
    /// Written before the order goes to the ring. It may or may not have
    /// arrived; that is the point of writing it first.
    Pending = 0,
    /// The engine acknowledged it, or filled part of it.
    Live = 1,
    Filled = 2,
    Canceled = 3,
    Rejected = 4,
};

std::string_view describe(OrderState s);

/// One line of the log.
struct OrderRecord {
    std::uint64_t client_order_id{0};
    std::uint64_t exchange_order_id{0};
    std::int64_t price{0};
    /// What was originally asked for; never changes.
    std::uint32_t quantity{0};
    /// What is still working. Zero on any terminal state.
    std::uint32_t leaves_quantity{0};
    std::uint16_t symbol_id{0};
    mdstack::wire::Side side{mdstack::wire::Side::kBid};
    OrderState state{OrderState::Pending};

    friend bool operator==(const OrderRecord&, const OrderRecord&) = default;

    [[nodiscard]] bool is_terminal() const noexcept { return state > OrderState::Live; }
};

/// Bytes one record occupies. Fixed, so a torn tail is detectable by length as
/// well as by checksum.
inline constexpr std::size_t kOrderRecordSize = 64;

/// The gateway's durable view of what it has working.
class OrderStore {
  public:
    OrderStore() = default;
    ~OrderStore();

    OrderStore(const OrderStore&) = delete;
    OrderStore& operator=(const OrderStore&) = delete;

    /// Opens or creates `path` and replays it.
    ///
    /// `on_live` is called once per order that is still working, in no
    /// particular order. A file that does not exist replays as nothing, which
    /// is what a first run looks like.
    std::optional<StoreError> open(const std::string& path,
                                   const std::function<void(const OrderRecord&)>& on_live);

    /// Appends `r` and `fsync`s it.
    ///
    /// **The caller must not send the message this record describes until this
    /// returns success.** An error here means the record is not durable, and a
    /// message whose record is not durable is one the gateway will not know
    /// about after a crash.
    std::optional<StoreError> record(const OrderRecord& r);

    /// Orders still working, as of the last thing recorded.
    [[nodiscard]] std::size_t live_count() const noexcept { return live_.size(); }

    /// The live set, for reconciliation.
    [[nodiscard]] const std::vector<OrderRecord>& live() const noexcept { return live_; }

    /// Records read at open.
    [[nodiscard]] std::uint64_t records_replayed() const noexcept { return replayed_; }

    /// Bytes discarded from the end of the file at open. Non-zero means the
    /// previous run was killed mid-write, which is the case this design is for.
    [[nodiscard]] std::uint64_t torn_tail_bytes() const noexcept { return torn_tail_; }

    /// Records written since open.
    [[nodiscard]] std::uint64_t records_written() const noexcept { return written_; }

    /// `fsync` calls made since open. One per record; if this ever stops
    /// tracking `records_written`, the durability claim has quietly changed.
    [[nodiscard]] std::uint64_t syncs() const noexcept { return syncs_; }

    /// Records the compaction at open removed.
    [[nodiscard]] std::uint64_t compacted_away() const noexcept { return compacted_; }

  private:
    std::optional<StoreError> replay();
    std::optional<StoreError> compact();
    void apply(const OrderRecord& r);

    std::string path_;
    int fd_{-1};
    std::vector<OrderRecord> live_;
    std::uint64_t replayed_{0};
    std::uint64_t torn_tail_{0};
    std::uint64_t written_{0};
    std::uint64_t syncs_{0};
    std::uint64_t compacted_{0};
};

}  // namespace fix
