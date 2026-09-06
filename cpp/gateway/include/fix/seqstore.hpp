// Durable FIX sequence numbers.
//
// # Why this is the hard part
//
// A FIX session's two sequence numbers are the only state that cannot be
// reconstructed. Lose them and there is no safe move: come back too low and the
// counterparty sees a sequence reversal, which the spec says is fatal; come back
// too high and every message in between is silently skipped.
//
// So they are written and **fsync'd before the message they describe reaches the
// socket**. That ordering is the whole point and it is expensive on purpose. If
// the process dies between sending and persisting, it restarts believing it sent
// less than it did, reuses a number, and the counterparty — correctly — drops
// the session. The fsync is what makes "survives a hard kill" true rather than
// usually true.
//
// # Surviving a torn write
//
// An fsync that is interrupted mid-write can leave a record half old and half
// new. A single record with a checksum detects that but leaves nothing to fall
// back to.
//
// So there are **two slots**, on separate sectors, each with a generation number
// and a checksum. A write goes to the older slot; a read takes the valid slot
// with the higher generation. A torn write damages exactly one slot and the
// other is intact and one generation behind — which is a sequence number that
// was genuinely durable at some point, and therefore safe.
//
// Coming back one message behind is recoverable: the counterparty asks for a
// resend and gets it, or receives a duplicate flagged `PossDupFlag=Y`. Coming
// back with a number that was never durable is not recoverable at all. The
// design trades a little redundancy for that difference.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace fix {

/// The two numbers a FIX session cannot rebuild.
struct SeqPair {
    /// The sequence number the next outbound message will carry.
    std::uint64_t outbound{1};
    /// The sequence number the next inbound message is expected to carry.
    std::uint64_t inbound{1};

    friend bool operator==(const SeqPair&, const SeqPair&) = default;
};

/// Why a store operation failed.
enum class StoreError {
    CannotOpen,
    CannotRead,
    CannotWrite,
    CannotSync,
    /// Both slots were unreadable or failed their checksum. A fresh file reads
    /// as a clean 1/1 rather than as this; this means a file exists and is
    /// damaged beyond both slots, which is not something to guess past.
    Corrupt,
};

std::string_view describe(StoreError e);

/// Sequence numbers persisted across a hard kill.
class SeqStore {
  public:
    SeqStore() = default;
    ~SeqStore();

    SeqStore(const SeqStore&) = delete;
    SeqStore& operator=(const SeqStore&) = delete;

    /// Opens or creates `path`. A file that does not exist starts at 1/1, which
    /// is what FIX means by a new session.
    std::optional<StoreError> open(const std::string& path);

    /// What was last durably recorded.
    [[nodiscard]] SeqPair current() const noexcept { return seq_; }

    /// Records `next` and does not return until it is on the disk.
    ///
    /// Call this **before** the message it describes is written to the socket.
    /// The reverse order is the bug this class exists to prevent.
    std::optional<StoreError> store(SeqPair next);

    /// Convenience for the common path: claim the next outbound number, persist
    /// it, and return the number the message should carry.
    ///
    /// Returns nothing if the store failed, in which case **the message must not
    /// be sent**. Sending it would consume a number that was never durable.
    std::optional<std::uint64_t> claim_outbound();

    /// Records that an inbound message with `seq` was accepted.
    std::optional<StoreError> accept_inbound(std::uint64_t seq);

    /// Resets both directions to 1, as `ResetSeqNumFlag=Y` requires.
    std::optional<StoreError> reset();

    /// How many times a slot failed its checksum on open. Non-zero means a
    /// previous run was killed mid-write and the fallback did its job — worth
    /// reporting rather than swallowing, because it is evidence the durability
    /// path is real.
    [[nodiscard]] int torn_slots_recovered() const noexcept { return torn_recovered_; }

    /// Number of fsyncs performed. The cost of the guarantee, made visible.
    [[nodiscard]] std::uint64_t syncs() const noexcept { return syncs_; }

  private:
    std::optional<StoreError> write_slot(int slot, std::uint64_t generation, SeqPair v);

    int fd_{-1};
    SeqPair seq_{};
    std::uint64_t generation_{};
    int torn_recovered_{};
    std::uint64_t syncs_{};
};

}  // namespace fix
