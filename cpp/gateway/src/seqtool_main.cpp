// Read and set a FIX session's persisted sequence numbers.
//
//   fix-seqtool --store /tmp/gw.seq
//   fix-seqtool --store /tmp/gw.seq --set-outbound 100 --set-inbound 50
//
// This is operational tooling, not a test hook. Setting sequence numbers by hand
// is a real and routine FIX operations task: a counterparty resets over the
// weekend, a session is reprovisioned, two sides disagree after an outage and
// somebody has to decide what the truth is. Every FIX engine ships something
// like it.
//
// It happens to be what lets the interop test create a gap on purpose, which is
// the only way to watch an independent implementation react to one.

#include <cstdlib>
#include <iostream>
#include <string>

#include "fix/seqstore.hpp"

int main(int argc, char** argv) {
    std::string path;
    long long set_out = -1;
    long long set_in = -1;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
        if (a == "--store") {
            path = next();
        } else if (a == "--set-outbound") {
            set_out = std::atoll(next().c_str());
        } else if (a == "--set-inbound") {
            set_in = std::atoll(next().c_str());
        } else {
            std::cerr << "usage: fix-seqtool --store PATH [--set-outbound N] [--set-inbound N]\n";
            return 2;
        }
    }
    if (path.empty()) {
        std::cerr << "fix-seqtool: --store is required\n";
        return 2;
    }

    fix::SeqStore store;
    if (auto err = store.open(path)) {
        std::cerr << "fix-seqtool: " << fix::describe(*err) << "\n";
        return 1;
    }

    if (set_out >= 0 || set_in >= 0) {
        fix::SeqPair next = store.current();
        if (set_out >= 0) {
            next.outbound = static_cast<std::uint64_t>(set_out);
        }
        if (set_in >= 0) {
            next.inbound = static_cast<std::uint64_t>(set_in);
        }
        if (auto err = store.store(next)) {
            std::cerr << "fix-seqtool: " << fix::describe(*err) << "\n";
            return 1;
        }
    }

    std::cout << "outbound=" << store.current().outbound << "\n"
              << "inbound=" << store.current().inbound << "\n"
              << "torn_slots_recovered=" << store.torn_slots_recovered() << "\n";
    return 0;
}
