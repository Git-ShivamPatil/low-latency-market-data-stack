// Minimal test scaffolding. No framework on purpose: the C++ side of milestone 1
// exists to prove the generated header agrees with the Rust codec byte for byte,
// and pulling in GoogleTest to compare two byte arrays would add a submodule and
// a build dependency to every CI run for no extra evidence.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <type_traits>
#include <vector>

namespace mdstack::golden {

/// Returns "" on success or a description of the first mismatch.
///
/// Note the deliberate lack of short-circuit cleverness: every check names the
/// field, because a golden-vector failure that says only "mismatch" costs more
/// time than the test saved.
// The expected value is cast to the accessor's own type before comparing. The
// build runs with -Wsign-compare -Wconversion -Werror, and an unsigned accessor
// against a plain integer literal would otherwise be a build failure rather than
// a test.
#define CHECK_EQ(label, actual, expected)                                    \
    do {                                                                     \
        const auto mdstack_actual_ = (actual);                               \
        const auto mdstack_expected_ =                                       \
            static_cast<std::decay_t<decltype(mdstack_actual_)>>(expected);  \
        if (mdstack_actual_ != mdstack_expected_) {                          \
            return std::string(label) + ": expected " +                      \
                   std::to_string(mdstack_expected_) + ", got " +            \
                   std::to_string(mdstack_actual_);                          \
        }                                                                    \
    } while (0)

inline std::string golden_dir() {
    if (const char* env = std::getenv("MDSTACK_GOLDEN_DIR")) {
        return env;
    }
    return MDSTACK_DEFAULT_GOLDEN_DIR;
}

inline bool read_file(const std::string& path, std::vector<std::byte>& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    if (size < 0) return false;
    in.seekg(0, std::ios::beg);
    out.resize(static_cast<std::size_t>(size));
    if (!out.empty()) {
        in.read(reinterpret_cast<char*>(out.data()), size);
    }
    return static_cast<bool>(in);
}

inline std::string hex(const std::byte* p, std::size_t len) {
    static const char* digits = "0123456789abcdef";
    std::string s;
    s.reserve(len * 3);
    for (std::size_t i = 0; i < len; ++i) {
        const auto v = static_cast<unsigned>(p[i]);
        s += digits[(v >> 4) & 0xF];
        s += digits[v & 0xF];
        s += ' ';
    }
    if (!s.empty()) s.pop_back();
    return s;
}

/// Reports the first differing byte rather than dumping both buffers, so a
/// wire-drift failure names an offset that can be looked up in the .txt dump.
inline std::string compare_bytes(const std::vector<std::byte>& expected,
                                 const std::byte* actual, std::size_t actual_len) {
    if (expected.size() != actual_len) {
        return "length: expected " + std::to_string(expected.size()) + " bytes, got " +
               std::to_string(actual_len);
    }
    for (std::size_t i = 0; i < actual_len; ++i) {
        if (expected[i] != actual[i]) {
            return "byte " + std::to_string(i) + ": expected 0x" +
                   hex(expected.data() + i, 1) + ", got 0x" + hex(actual + i, 1);
        }
    }
    return "";
}

}  // namespace mdstack::golden
