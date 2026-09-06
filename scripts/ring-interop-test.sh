#!/usr/bin/env bash
#
# A Rust process and a C++ process on opposite ends of one shared-memory ring.
#
# # Why this is not covered by the two unit suites
#
# `crates/ring` and `cpp/ring` each prove themselves self-consistent. Neither
# can prove they agree, and disagreement here does not fail loudly: a `writeIndex`
# read from the wrong offset is a plausible number, and a `price` read from the
# wrong offset is a plausible price. The generator is what makes the two agree —
# both read `wire::layout::ring_header` from `schema/market-data.xml` — but
# generated is not the same as checked, and this is the check.
#
# The payload is a real `NewOrder`, so two agreements are tested at once: where
# the ring puts its indices, and where a message puts its fields. Every field is
# a deterministic function of the message number, so a failure says *which*
# message was wrong rather than only that the counts differ. Prices alternate
# sign, so a decoder reading them unsigned fails on the second message instead of
# agreeing for half the run.
#
#   scripts/ring-interop-test.sh
#   scripts/ring-interop-test.sh --keep    # leave the rings and logs behind

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

BUILD="${MDSTACK_BUILD_DIR:-$REPO/cpp/build}"
OUT="$REPO/results/ring-interop"
MESSAGES="${RING_INTEROP_MESSAGES:-200000}"
CAPACITY=1024
SLOT=128

rm -rf "$OUT"
mkdir -p "$OUT"

echo "building both ends"
cmake -S cpp -B "$BUILD" -DCMAKE_BUILD_TYPE=RelWithDebInfo >"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2
    exit 1
}
cmake --build "$BUILD" --target ring-poke >>"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2
    exit 1
}
cargo build --release -p ring --bin ring-poke >>"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2
    exit 1
}

CPP="$BUILD/ring/ring-poke"
RUST="${CARGO_TARGET_DIR:-$REPO/target}/release/ring-poke"

status=0
fail() {
    echo "  FAIL: $*" >&2
    status=1
}

# `key=value` out of a poke log; empty if absent, which every caller treats as
# a failure rather than a zero.
val() {
    grep -E "^$1=" "$2" 2>/dev/null | head -1 | cut -d= -f2-
}

# One direction: `$1` produces, `$2` consumes, both against a ring `$3` created.
run_direction() {
    local name="$1" producer="$2" consumer="$3" creator="$4"
    local ring="$OUT/$name.ring"

    echo
    echo "=============================================================="
    echo " $name"
    echo "=============================================================="
    rm -f "$ring"

    "$creator" --path "$ring" --create --capacity "$CAPACITY" --slot-size "$SLOT" \
        >"$OUT/$name-create.log" 2>&1 || {
        cat "$OUT/$name-create.log" >&2
        fail "$name: the ring was not created"
        return
    }

    # The consumer starts first and spins, so the producer is never the one
    # waiting. A ring smaller than the run forces the two to interleave rather
    # than the producer finishing before the consumer starts — which is the
    # case where a stale cached index would show up.
    "$consumer" --path "$ring" --consume "$MESSAGES" >"$OUT/$name-consume.log" 2>&1 &
    local con_pid=$!
    "$producer" --path "$ring" --produce "$MESSAGES" >"$OUT/$name-produce.log" 2>&1
    local prod_status=$?
    wait "$con_pid"
    local con_status=$?

    local produced blocked consumed mismatches first
    produced="$(val produced "$OUT/$name-produce.log")"
    blocked="$(val blocked "$OUT/$name-produce.log")"
    consumed="$(val consumed "$OUT/$name-consume.log")"
    mismatches="$(val mismatches "$OUT/$name-consume.log")"
    first="$(val first_mismatch "$OUT/$name-consume.log")"

    echo "  produced      ${produced:-<none>}"
    echo "  consumed      ${consumed:-<none>}"
    echo "  mismatches    ${mismatches:-<none>}"
    # Not an assertion. A run where the producer never blocked means the ring
    # never filled, so the wrap-around path was not exercised and the numbers
    # above are weaker than they look. Worth printing rather than hiding.
    echo "  times the producer had to wait for a free slot: ${blocked:-<none>}"

    [[ "$produced" == "$MESSAGES" ]] || fail "$name: produced ${produced:-<none>}, expected $MESSAGES"
    [[ "$consumed" == "$MESSAGES" ]] || fail "$name: consumed ${consumed:-<none>}, expected $MESSAGES"
    if [[ "$mismatches" != "0" ]]; then
        fail "$name: ${mismatches:-<none>} message(s) decoded wrong, first at index ${first:-?}"
    fi
    [[ $prod_status -eq 0 ]] || fail "$name: the producer exited $prod_status"
    [[ $con_status -eq 0 ]] || fail "$name: the consumer exited $con_status"

    if [[ -n "$blocked" && "$blocked" == "0" ]]; then
        echo "  note: the consumer kept up, so this direction did not exercise the"
        echo "        back-pressure path. It still wrapped the ring $((MESSAGES / CAPACITY))"
        echo "        times over, which is the part this test is for; a full ring is"
        echo "        covered by both unit suites."
    fi
}

echo "  $MESSAGES messages per direction, $CAPACITY slots of $SLOT bytes"
echo "  (the ring holds $CAPACITY, so the run wraps around it many times over)"

# Both directions, and both creators. Which side wrote the header is not
# supposed to matter, and the only way to know it does not is to try it.
run_direction "rust-produces-cpp-consumes" "$RUST" "$CPP" "$RUST"
run_direction "cpp-produces-rust-consumes" "$CPP" "$RUST" "$CPP"

echo
if [[ $status -eq 0 ]]; then
    echo "ring-interop: PASS — the two languages agree about every byte of the"
    echo "              ring header and of the message inside every slot"
    [[ $KEEP -eq 0 ]] && rm -rf "$OUT"
else
    echo "ring-interop: FAIL — artifacts in $OUT" >&2
fi
exit $status
