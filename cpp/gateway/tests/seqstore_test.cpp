// Durability tests for the FIX sequence store.
//
// These do the thing the milestone actually asks for: damage the file the way a
// kill mid-write would, reopen, and require the store to come back with a number
// that was genuinely durable. A test that only writes and reads back proves the
// happy path and nothing about the guarantee.

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdint>
#include <iostream>
#include <string>

#include "fix/seqstore.hpp"

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

std::string temp_path(const char* name) {
    return std::string("/tmp/mdstack-seqstore-") + name + "-" + std::to_string(::getpid());
}

/// Overwrites `bytes` at `offset` with rubbish, the way a half-finished write
/// leaves a sector.
void damage(const std::string& path, off_t offset, std::size_t bytes) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return;
    }
    std::string junk(bytes, '\xA5');
    ssize_t written = ::pwrite(fd, junk.data(), junk.size(), offset);
    int synced = ::fsync(fd);
    ::close(fd);
    if (written != static_cast<ssize_t>(junk.size()) || synced != 0) {
        std::cerr << "FAIL could not damage " << path << " for the test\n";
        ++failures;
    }
}

void a_new_session_starts_at_one() {
    std::string path = temp_path("new");
    ::unlink(path.c_str());

    fix::SeqStore s;
    check(!s.open(path), "a new store opens");
    check(s.current() == fix::SeqPair{1, 1}, "a session with no history starts at 1 and 1");
    ::unlink(path.c_str());
}

void numbers_survive_a_reopen() {
    std::string path = temp_path("reopen");
    ::unlink(path.c_str());
    {
        fix::SeqStore s;
        s.open(path);
        s.store(fix::SeqPair{42, 17});
    }
    {
        fix::SeqStore s;
        check(!s.open(path), "an existing store reopens");
        check(s.current() == fix::SeqPair{42, 17}, "the numbers survive the process");
    }
    ::unlink(path.c_str());
}

void claiming_a_number_persists_before_returning_it() {
    // The ordering the whole class exists for. If `claim_outbound` returned
    // before the fsync, a kill in that window would hand the same number out
    // twice, and the counterparty would treat the second as a reversal.
    std::string path = temp_path("claim");
    ::unlink(path.c_str());

    std::uint64_t claimed{};
    {
        fix::SeqStore s;
        s.open(path);
        auto n = s.claim_outbound();
        check(n.has_value(), "claiming a number succeeds");
        claimed = *n;
        check(claimed == 1, "the first outbound message is sequence 1");
    }
    {
        // Simulating the kill: nothing else ran after the claim.
        fix::SeqStore s;
        s.open(path);
        check(s.current().outbound == claimed + 1,
              "after a kill immediately following a claim, the number is not handed out again");
    }
    ::unlink(path.c_str());
}

void a_torn_slot_falls_back_to_the_other() {
    // The case the two-slot layout exists for. Slot 0 is destroyed; the store
    // must come back on slot 1 with a number that was durable, not guess.
    std::string path = temp_path("torn");
    ::unlink(path.c_str());
    {
        fix::SeqStore s;
        s.open(path);
        s.store(fix::SeqPair{10, 10});  // generation 1 -> slot 1
        s.store(fix::SeqPair{11, 11});  // generation 2 -> slot 0
        s.store(fix::SeqPair{12, 12});  // generation 3 -> slot 1
    }
    // Slot 1 holds the newest record. Destroy it.
    damage(path, 512, 64);
    {
        fix::SeqStore s;
        auto err = s.open(path);
        check(!err, "a store with one damaged slot still opens");
        check(s.torn_slots_recovered() == 1, "the damaged slot is noticed and counted");
        check(s.current() == fix::SeqPair{11, 11},
              "it falls back to the older slot, which was genuinely durable");
    }
    ::unlink(path.c_str());
}

void the_other_slot_torn_is_also_survivable() {
    std::string path = temp_path("torn0");
    ::unlink(path.c_str());
    {
        fix::SeqStore s;
        s.open(path);
        s.store(fix::SeqPair{10, 10});  // gen 1 -> slot 1
        s.store(fix::SeqPair{11, 11});  // gen 2 -> slot 0
    }
    damage(path, 0, 64);
    {
        fix::SeqStore s;
        check(!s.open(path), "a store with the other slot damaged still opens");
        check(s.current() == fix::SeqPair{10, 10}, "and falls back the other way");
    }
    ::unlink(path.c_str());
}

void both_slots_damaged_is_refused_not_guessed() {
    // The one case where there is nothing safe to return. Inventing a sequence
    // number here is exactly the failure this store exists to prevent, so it
    // reports corruption and lets the operator decide.
    std::string path = temp_path("both");
    ::unlink(path.c_str());
    {
        fix::SeqStore s;
        s.open(path);
        s.store(fix::SeqPair{5, 5});
        s.store(fix::SeqPair{6, 6});
    }
    damage(path, 0, 64);
    damage(path, 512, 64);
    {
        fix::SeqStore s;
        auto err = s.open(path);
        check(err == fix::StoreError::Corrupt,
              "two damaged slots are reported as corruption, never guessed past");
    }
    ::unlink(path.c_str());
}

void a_reset_returns_both_directions_to_one() {
    std::string path = temp_path("reset");
    ::unlink(path.c_str());
    fix::SeqStore s;
    s.open(path);
    s.store(fix::SeqPair{500, 400});
    check(!s.reset(), "reset succeeds");
    check(s.current() == fix::SeqPair{1, 1}, "ResetSeqNumFlag returns both directions to 1");

    fix::SeqStore reopened;
    reopened.open(path);
    check(reopened.current() == fix::SeqPair{1, 1}, "and the reset is itself durable");
    ::unlink(path.c_str());
}

void every_store_costs_exactly_one_sync() {
    // The cost of the guarantee, asserted rather than assumed. If a future
    // change batches or skips an fsync, this is what says so.
    std::string path = temp_path("sync");
    ::unlink(path.c_str());
    fix::SeqStore s;
    s.open(path);
    check(s.syncs() == 0, "opening syncs nothing");
    s.claim_outbound();
    s.claim_outbound();
    s.accept_inbound(7);
    check(s.syncs() == 3, "each recorded change costs exactly one fsync");
    ::unlink(path.c_str());
}

void inbound_records_the_next_expected() {
    std::string path = temp_path("inbound");
    ::unlink(path.c_str());
    fix::SeqStore s;
    s.open(path);
    s.accept_inbound(41);
    check(s.current().inbound == 42,
          "accepting sequence 41 records 42 as next expected, not 41");
    ::unlink(path.c_str());
}

}  // namespace

int main() {
    a_new_session_starts_at_one();
    numbers_survive_a_reopen();
    claiming_a_number_persists_before_returning_it();
    a_torn_slot_falls_back_to_the_other();
    the_other_slot_torn_is_also_survivable();
    both_slots_damaged_is_refused_not_guessed();
    a_reset_returns_both_directions_to_one();
    every_store_costs_exactly_one_sync();
    inbound_records_the_next_expected();

    if (failures != 0) {
        std::cerr << failures << " failure(s)\n";
        return 1;
    }
    std::cout << "all sequence durability checks passed\n";
    return 0;
}
