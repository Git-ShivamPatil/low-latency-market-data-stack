#!/usr/bin/env bash
#
# Milestone 1's verification step, as a script: a deliberate one-byte edit to a
# golden vector must make BOTH test suites fail.
#
# A golden-vector suite that passes on damaged input is worse than no suite at
# all, because it reports success while the wire format is broken. So this
# inverts the usual polarity: a run that ends with both suites green is a
# FAILURE of this script.
#
# The vectors are copied to a scratch directory first and MDSTACK_GOLDEN_DIR
# points both suites at the copy, so the committed vectors are never touched.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CPP_BUILD="${CPP_BUILD:-cpp/build}"
CARGO="${CARGO:-cargo}"

cd "$REPO"

if [[ ! -x "$CPP_BUILD/wire/wire_golden_test" ]]; then
    echo "error: $CPP_BUILD/wire/wire_golden_test is not built." >&2
    echo "       run 'make build-cpp' first." >&2
    exit 2
fi

SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

cp schema/golden/*.bin "$SCRATCH/"

# Byte 8 of every vector is the first byte of packetHeader.firstSequence: a
# field both suites read, in a message every vector has. Flipping its low bit is
# the smallest possible edit that changes meaning.
CORRUPT_AT=8

vectors=0
for f in "$SCRATCH"/*.bin; do
    python3 - "$f" "$CORRUPT_AT" <<'PY'
import sys
path, off = sys.argv[1], int(sys.argv[2])
data = bytearray(open(path, "rb").read())
if len(data) <= off:
    raise SystemExit(f"{path} is only {len(data)} bytes; cannot corrupt byte {off}")
data[off] ^= 0x01
open(path, "wb").write(data)
PY
    vectors=$((vectors + 1))
done
echo "corrupted byte $CORRUPT_AT of $vectors vectors in $SCRATCH"
echo

status=0

echo "--- Rust suite against the damaged vectors (expected to FAIL) ---"
if MDSTACK_GOLDEN_DIR="$SCRATCH" "$CARGO" test -p wire --test golden >/dev/null 2>&1; then
    echo "NOT DETECTED: cargo test -p wire --test golden passed on corrupted vectors" >&2
    status=1
else
    echo "detected: the Rust golden suite failed, as it must"
fi
echo

echo "--- C++ suite against the damaged vectors (expected to FAIL) ---"
if MDSTACK_GOLDEN_DIR="$SCRATCH" "$CPP_BUILD/wire/wire_golden_test" >/dev/null 2>&1; then
    echo "NOT DETECTED: wire_golden_test passed on corrupted vectors" >&2
    status=1
else
    echo "detected: the C++ golden suite failed, as it must"
fi
echo

# The other half of the proof: with the vectors intact, both suites must pass.
# Without this, a suite that failed unconditionally would sail through above.
echo "--- both suites against the intact vectors (expected to PASS) ---"
if MDSTACK_GOLDEN_DIR="$REPO/schema/golden" "$CARGO" test -p wire --test golden >/dev/null 2>&1; then
    echo "ok: the Rust golden suite passes on the committed vectors"
else
    echo "REGRESSION: the Rust golden suite fails on the committed vectors" >&2
    status=1
fi
if MDSTACK_GOLDEN_DIR="$REPO/schema/golden" "$CPP_BUILD/wire/wire_golden_test" >/dev/null 2>&1; then
    echo "ok: the C++ golden suite passes on the committed vectors"
else
    echo "REGRESSION: the C++ golden suite fails on the committed vectors" >&2
    status=1
fi

echo
if [[ $status -eq 0 ]]; then
    echo "PASS: a one-byte edit is caught by both suites, and neither fails on clean input."
else
    echo "FAIL: see above." >&2
fi
exit $status
