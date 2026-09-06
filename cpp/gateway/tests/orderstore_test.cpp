// What the order log has to survive.
//
// The interesting cases are all failures: a kill in the middle of a write, a
// file that is a whole record short, a file whose middle is wrong. The happy
// path is one append and one read-back, and it is the least of what this has to
// do.

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "fix/orderstore.hpp"

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

using fix::OrderRecord;
using fix::OrderState;
using fix::OrderStore;
using fix::StoreError;
using mdstack::wire::Side;

std::string temp_path(const char* name) {
    return std::string("/tmp/mdstack-orders-") + name + "-" + std::to_string(::getpid());
}

struct Temp {
    std::string path;
    explicit Temp(const char* name) : path(temp_path(name)) { ::unlink(path.c_str()); }
    ~Temp() {
        ::unlink(path.c_str());
        ::unlink((path + ".compact").c_str());
    }
    Temp(const Temp&) = delete;
    Temp& operator=(const Temp&) = delete;
};

OrderRecord pending(std::uint64_t id, std::uint32_t qty = 100) {
    return OrderRecord{id, 0, 1'000'000, qty, qty, 7, Side::kBid, OrderState::Pending};
}

std::size_t file_size(const std::string& p) {
    const int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    const off_t n = ::lseek(fd, 0, SEEK_END);
    ::close(fd);
    return static_cast<std::size_t>(n);
}

// --- the basics ------------------------------------------------------------

void a_fresh_file_replays_as_nothing() {
    Temp t("fresh");
    OrderStore s;
    check(!s.open(t.path, nullptr).has_value(), "opening a path that does not exist works");
    check(s.live_count() == 0 && s.records_replayed() == 0,
          "and a first run has no orders and no records");
}

void an_order_survives_a_reopen() {
    Temp t("reopen");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        check(!s.record(pending(1)).has_value(), "recorded");
        check(s.live_count() == 1, "and it is live in this process");
        check(s.syncs() == s.records_written(),
              "one fsync per record, which is the durability policy stated as a number");
    }
    OrderStore s;
    std::vector<OrderRecord> seen;
    s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); });
    check(seen.size() == 1 && seen[0].client_order_id == 1 && seen[0].quantity == 100,
          "and it comes back after a reopen");
}

void a_terminal_record_removes_the_order() {
    Temp t("terminal");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        s.record(pending(1));
        s.record(pending(2));
        OrderRecord done = pending(1);
        done.state = OrderState::Filled;
        done.leaves_quantity = 0;
        s.record(done);
        check(s.live_count() == 1, "a filled order stops being live immediately");
    }
    OrderStore s;
    std::vector<OrderRecord> seen;
    s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); });
    check(seen.size() == 1 && seen[0].client_order_id == 2,
          "and stays gone across a reopen, with the other order still there");
}

void the_last_record_for_an_order_wins() {
    Temp t("update");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        s.record(pending(1, 100));
        OrderRecord partial = pending(1, 100);
        partial.state = OrderState::Live;
        partial.exchange_order_id = 555;
        partial.leaves_quantity = 60;
        s.record(partial);
    }
    OrderStore s;
    std::vector<OrderRecord> seen;
    s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); });
    check(seen.size() == 1 && seen[0].leaves_quantity == 60 && seen[0].exchange_order_id == 555,
          "a later record for the same order replaces the earlier one");
    check(seen[0].quantity == 100, "and the original quantity is still there to compare against");
}

// --- the failures ----------------------------------------------------------

void a_torn_tail_is_discarded_and_counted() {
    Temp t("torn");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        s.record(pending(1));
        s.record(pending(2));
    }
    // Half a record, which is what SIGKILL during a write leaves behind. The
    // record was never fsync'd, so the message it describes was never sent.
    const int fd = ::open(t.path.c_str(), O_RDWR);
    const auto partial = std::string(30, '\xEE');
    ::lseek(fd, 0, SEEK_END);
    ssize_t w = ::write(fd, partial.data(), partial.size());
    (void)w;
    ::close(fd);

    OrderStore s;
    std::vector<OrderRecord> seen;
    check(!s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); }).has_value(),
          "a torn tail is not an error");
    check(seen.size() == 2, "the whole records before it are intact");
    check(s.torn_tail_bytes() == 30, "and the partial bytes are counted rather than ignored");
}

void a_whole_record_that_fails_its_checksum_ends_the_replay() {
    Temp t("checksum");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        s.record(pending(1));
        s.record(pending(2));
    }
    // A full-length record of rubbish. An interrupted write can produce one, so
    // it gets the same treatment as a short tail -- but it is counted, because
    // a checksum failure anywhere else would mean something quite different.
    const int fd = ::open(t.path.c_str(), O_RDWR);
    const auto junk = std::string(fix::kOrderRecordSize, '\x5A');
    ::lseek(fd, 0, SEEK_END);
    ssize_t w = ::write(fd, junk.data(), junk.size());
    (void)w;
    ::close(fd);

    OrderStore s;
    std::vector<OrderRecord> seen;
    check(!s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); }).has_value(),
          "a bad record at the end is not an error");
    check(seen.size() == 2, "the good records before it survive");
    check(s.torn_tail_bytes() == fix::kOrderRecordSize, "and the bad one is counted");
}

void a_flipped_bit_in_the_middle_is_refused_rather_than_guessed_past() {
    Temp t("middle");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        for (std::uint64_t i = 1; i <= 5; ++i) {
            s.record(pending(i));
        }
    }
    // Damage record 2's price. Its checksum now fails, and the records after it
    // are still fine -- so a store that stopped quietly would come back with
    // one order instead of five and never say why.
    const int fd = ::open(t.path.c_str(), O_RDWR);
    unsigned char b = 0;
    ::lseek(fd, static_cast<off_t>(fix::kOrderRecordSize + 24), SEEK_SET);
    ssize_t r = ::read(fd, &b, 1);
    (void)r;
    b = static_cast<unsigned char>(b ^ 0x01);
    ::lseek(fd, static_cast<off_t>(fix::kOrderRecordSize + 24), SEEK_SET);
    ssize_t w = ::write(fd, &b, 1);
    (void)w;
    ::close(fd);

    OrderStore s;
    // Stopping at the damage would silently drop three orders that are live at
    // the exchange. Refusing to open is the only honest answer.
    const auto err = s.open(t.path, nullptr);
    check(err.has_value(), "a file damaged in the middle is refused rather than truncated");
}

// --- compaction ------------------------------------------------------------

void the_log_is_compacted_at_startup() {
    Temp t("compact");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        // A thousand orders opened and closed, and two left working. An
        // append-only log that never compacts is a disk-space bug with a delay
        // fuse.
        for (std::uint64_t i = 1; i <= 1'000; ++i) {
            s.record(pending(i));
            OrderRecord done = pending(i);
            done.state = OrderState::Canceled;
            done.leaves_quantity = 0;
            s.record(done);
        }
        s.record(pending(10'001));
        s.record(pending(10'002));
    }
    const std::size_t before = file_size(t.path);
    check(before == 2002 * fix::kOrderRecordSize, "the log holds every transition before restart");

    OrderStore s;
    std::vector<OrderRecord> seen;
    s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); });
    check(seen.size() == 2, "restart rebuilds exactly the two live orders");
    check(s.compacted_away() == 2000, "and says how many records it removed");
    check(file_size(t.path) == 2 * fix::kOrderRecordSize,
          "the file is now two records, not two thousand");

    // And the compacted file is itself replayable, which is the part a
    // rewrite-in-place gets wrong.
    OrderStore again;
    std::vector<OrderRecord> twice;
    check(!again.open(t.path, [&twice](const OrderRecord& r) { twice.push_back(r); }).has_value(),
          "the compacted log reopens cleanly");
    check(twice.size() == 2, "with the same two orders");
    check(again.compacted_away() == 0, "and nothing left to compact");
}

void appending_after_a_compaction_still_replays() {
    Temp t("append-after");
    {
        OrderStore s;
        s.open(t.path, nullptr);
        s.record(pending(1));
        OrderRecord done = pending(1);
        done.state = OrderState::Filled;
        done.leaves_quantity = 0;
        s.record(done);
        s.record(pending(2));
    }
    {
        OrderStore s;
        s.open(t.path, nullptr);  // compacts to one record
        check(s.compacted_away() == 2, "the finished order is compacted away");
        // The record numbering has to continue from the compacted file rather
        // than from where the old one had got to, or replay sees a gap and
        // refuses the file.
        s.record(pending(3));
    }
    OrderStore s;
    std::vector<OrderRecord> seen;
    check(!s.open(t.path, [&seen](const OrderRecord& r) { seen.push_back(r); }).has_value(),
          "a log appended to after compaction still replays");
    check(seen.size() == 2, "and holds both orders");
}

}  // namespace

int main() {
    a_fresh_file_replays_as_nothing();
    an_order_survives_a_reopen();
    a_terminal_record_removes_the_order();
    the_last_record_for_an_order_wins();

    a_torn_tail_is_discarded_and_counted();
    a_whole_record_that_fails_its_checksum_ends_the_replay();
    a_flipped_bit_in_the_middle_is_refused_rather_than_guessed_past();

    the_log_is_compacted_at_startup();
    appending_after_a_compaction_still_replays();

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all order store checks passed\n";
    return 0;
}
