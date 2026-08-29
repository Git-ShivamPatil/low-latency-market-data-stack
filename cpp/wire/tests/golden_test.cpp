// The C++ half of milestone 1's verification.
//
// This runs against the same schema/golden/*.bin files as `cargo test -p wire`,
// asserts the same field values, and re-encodes each vector expecting the bytes
// back. If the Rust codec and this header ever disagree about a single byte,
// one of the two suites fails here rather than surfacing as a corrupted field
// under load six milestones later.

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

#include "golden_generated.hpp"
#include "golden_support.hpp"

int main() {
    using namespace mdstack::golden;

    const std::string dir = golden_dir();
    std::cout << "golden dir: " << dir << "\n";

    int failures = 0;
    int checked = 0;

    for (const auto& v : kVectors) {
        const std::string path = dir + "/" + v.file;
        std::vector<std::byte> expected;
        if (!read_file(path, expected)) {
            std::cerr << "FAIL " << v.name << ": cannot read " << path << "\n";
            ++failures;
            continue;
        }

        const std::string decode_err = v.check(expected.data(), expected.size());
        if (!decode_err.empty()) {
            std::cerr << "FAIL " << v.name << " (decode): " << decode_err << "\n";
            ++failures;
            continue;
        }

        // Re-encode from the same literal values and require the file back.
        // This is what makes every byte of the vector load-bearing, padding and
        // reserved fields included -- a decode-only test would happily ignore
        // them and let a one-byte corruption through.
        std::vector<std::byte> actual(expected.size() + 64);
        std::string build_err;
        const std::size_t n = v.build(actual.data(), actual.size(), build_err);
        if (n == 0) {
            std::cerr << "FAIL " << v.name << " (encode): "
                      << (build_err.empty() ? "encoder returned 0" : build_err) << "\n";
            ++failures;
            continue;
        }
        const std::string cmp = compare_bytes(expected, actual.data(), n);
        if (!cmp.empty()) {
            std::cerr << "FAIL " << v.name << " (re-encode): " << cmp << "\n";
            std::cerr << "  expected: " << hex(expected.data(), expected.size()) << "\n";
            std::cerr << "  actual:   " << hex(actual.data(), n) << "\n";
            ++failures;
            continue;
        }

        std::cout << "ok   " << v.name << " (" << expected.size() << " bytes)\n";
        ++checked;
    }

    if (checked == 0) {
        std::cerr << "FAIL: no vectors were checked -- is " << dir << " populated?\n";
        return 1;
    }

    std::cout << checked << " vectors ok, " << failures << " failed\n";
    return failures == 0 ? 0 : 1;
}
