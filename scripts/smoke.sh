#!/usr/bin/env bash
#
# Milestone 2's verification step.
#
# Runs the engine and the handler as separate processes over real UDP sockets,
# then requires that the handler's book matches the engine's own book at the same
# sequence number. That reconciliation is the point: the two processes reach a
# book by completely different routes — one by matching orders, the other by
# replaying the feed those matches produced — so agreement at a shared sequence
# is evidence the feed faithfully describes what the engine did.
#
# It also checks the things that silently break a batched feed: a gap at a
# datagram seam, a duplicate counted as new, a sequence that skips.
#
# Runs in both transport modes by default. The unicast fallback is not a
# second-class path — it is what this project uses when multicast over a Docker
# bridge under WSL2 refuses to cooperate — so it gets the same test.
#
#   scripts/smoke.sh                          # both modes
#   scripts/smoke.sh --transport multicast    # one mode
#   scripts/smoke.sh --messages 250000        # longer run

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

MESSAGES=100000
DIGEST_INTERVAL=1000
IDLE_TIMEOUT=10
MODES=(multicast unicast-fanout)
KEEP=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --transport) MODES=("$2"); shift 2 ;;
        --messages) MESSAGES="$2"; shift 2 ;;
        --digest-interval) DIGEST_INTERVAL="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

OUT="$REPO/results/smoke"
mkdir -p "$OUT"

echo "building release binaries"
cargo build --release --bin matching-engine --bin feed-handler
ENGINE="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/matching-engine"
HANDLER="$(dirname "$ENGINE")/feed-handler"
echo "  engine  $ENGINE"
echo "  handler $HANDLER"
echo

overall=0

for MODE in "${MODES[@]}"; do
    echo "=============================================================="
    echo " transport: $MODE"
    echo "=============================================================="

    ENGINE_DIGESTS="$OUT/engine-$MODE.txt"
    HANDLER_DIGESTS="$OUT/handler-$MODE.txt"
    ENGINE_LOG="$OUT/engine-$MODE.log"
    HANDLER_LOG="$OUT/handler-$MODE.log"
    SUMMARY="$OUT/handler-$MODE.summary"
    rm -f "$ENGINE_DIGESTS" "$HANDLER_DIGESTS" "$ENGINE_LOG" "$HANDLER_LOG" "$SUMMARY"

    if [[ "$MODE" == "multicast" ]]; then
        FEED_A="239.1.1.1:30001"
        FEED_B="239.1.1.2:30001"
    else
        FEED_A="127.0.0.1:31001"
        FEED_B="127.0.0.1:31002"
    fi

    # The handler starts FIRST, deliberately.
    #
    # It has no way to rebuild state it did not see: joining mid-stream leaves it
    # missing every order that rested before it arrived, and its book would
    # diverge from the engine's for the rest of the run. Starting it first is not
    # papering over that — it is the honest scope of this milestone. Recovering
    # from a late join is what the snapshot cycle and the replay service in
    # milestone 4 are for, and the handler says so out loud when it happens.
    "$HANDLER" \
        --config configs/local.toml \
        --transport "$MODE" \
        --feed-a "$FEED_A" \
        --feed-b "$FEED_B" \
        --messages "$MESSAGES" \
        --digest-path "$HANDLER_DIGESTS" \
        --digest-interval "$DIGEST_INTERVAL" \
        --idle-timeout "$IDLE_TIMEOUT" \
        --summary-path "$SUMMARY" \
        >"$HANDLER_LOG" 2>&1 &
    HANDLER_PID=$!

    # Give the group join time to take effect. Without this the engine can
    # publish into a group nobody has joined yet and the first datagrams vanish,
    # which would look like a gap and be blamed on the code.
    sleep 1

    "$ENGINE" \
        --config configs/local.toml \
        --transport "$MODE" \
        --feed-a "$FEED_A" \
        --feed-b "$FEED_B" \
        --messages "$MESSAGES" \
        --digest-path "$ENGINE_DIGESTS" \
        --digest-interval "$DIGEST_INTERVAL" \
        --self-check \
        >"$ENGINE_LOG" 2>&1
    engine_status=$?

    wait "$HANDLER_PID" && handler_status=0 || handler_status=$?

    echo "engine exit $engine_status, handler exit $handler_status"
    echo
    echo "--- engine ---"
    tail -n 6 "$ENGINE_LOG" | sed 's/^/  /'
    echo "--- handler ---"
    tail -n 6 "$HANDLER_LOG" | sed 's/^/  /'
    echo

    status=0
    if [[ $engine_status -ne 0 ]]; then
        echo "FAIL: the engine exited $engine_status" >&2
        status=1
    fi
    if [[ $handler_status -ne 0 ]]; then
        echo "FAIL: the handler exited $handler_status — it saw a gap, a bad datagram," >&2
        echo "      or a message that did not apply. See $HANDLER_LOG" >&2
        status=1
    fi

    python3 - "$ENGINE_DIGESTS" "$HANDLER_DIGESTS" "$MESSAGES" "$DIGEST_INTERVAL" <<'PY' || status=1
import sys

engine_path, handler_path, messages, interval = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])


def load(path):
    rows = {}
    order = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 4:
                sys.exit(f"FAIL: {path}:{lineno}: expected 4 fields, got {len(parts)}")
            seq = int(parts[0])
            if seq in rows:
                sys.exit(f"FAIL: {path}: sequence {seq} checkpointed twice")
            rows[seq] = tuple(parts[1:])
            order.append(seq)
    return rows, order


engine, engine_order = load(engine_path)
handler, handler_order = load(handler_path)

if not engine:
    sys.exit(f"FAIL: {engine_path} is empty — the engine wrote no checkpoints")
if not handler:
    sys.exit(f"FAIL: {handler_path} is empty — the handler wrote no checkpoints")

# Checkpoints must arrive in ascending sequence on both sides. A sequence that
# went backwards would mean the stream was replayed or reordered.
for name, order in (("engine", engine_order), ("handler", handler_order)):
    if order != sorted(order):
        sys.exit(f"FAIL: {name} checkpoints are not in ascending sequence order")

# The handler must have consumed the whole run, not a prefix of it.
expected_checkpoints = messages // interval
if len(handler) < expected_checkpoints:
    sys.exit(
        f"FAIL: the handler wrote {len(handler)} checkpoints; {messages} messages at "
        f"an interval of {interval} should produce {expected_checkpoints}. It stopped early."
    )

shared = sorted(set(engine) & set(handler))
if len(shared) < expected_checkpoints:
    sys.exit(
        f"FAIL: only {len(shared)} checkpoints are shared between the two processes, "
        f"expected {expected_checkpoints}"
    )

mismatches = [s for s in shared if engine[s] != handler[s]]
if mismatches:
    first = mismatches[0]
    print(f"FAIL: the books disagree at {len(mismatches)} of {len(shared)} checkpoints", file=sys.stderr)
    print(f"  first divergence at sequence {first}", file=sys.stderr)
    print(f"    engine  top={engine[first][0]} full={engine[first][1]} orders={engine[first][2]}", file=sys.stderr)
    print(f"    handler top={handler[first][0]} full={handler[first][1]} orders={handler[first][2]}", file=sys.stderr)
    sys.exit(1)

last = shared[-1]
print(f"  {len(shared)} shared checkpoints, every one identical")
print(f"  sequence {shared[0]}..{last}, {engine[last][2]} orders resting at the end")
print(f"  final digest top={engine[last][0]} full={engine[last][1]}")
PY

    # Everything below asserts against the handler's key=value summary rather
    # than its log. Grepping human-readable output means the test breaks when
    # someone improves the wording, and then gets "fixed" by loosening it.
    python3 - "$SUMMARY" "$MESSAGES" <<'PY' || status=1
import sys

path, expected = sys.argv[1], int(sys.argv[2])
try:
    with open(path) as f:
        s = dict(line.strip().split("=", 1) for line in f if "=" in line)
except FileNotFoundError:
    sys.exit(f"FAIL: the handler wrote no summary at {path}")

fails = []


def want(key, value, why):
    got = s.get(key)
    if got != str(value):
        fails.append(f"{key} is {got}, expected {value} — {why}")


want("messages", expected, "the handler must consume the whole run, not a prefix")
want("first_sequence", 1, "starting anywhere else means it joined mid-stream")
want("last_sequence", expected, "the stream must run to the end")
want("gaps", 0, "a gap means a message was lost between the two processes")
want("messages_missed", 0, "nothing may be skipped")
want("bad_datagrams", 0, "every datagram must decode")
want("apply_errors", 0, "every message must apply to the book it describes")
want("joined_mid_stream", "false", "the handler must have seen sequence 1")

# Sequence 1..N with N messages accepted and zero gaps is what "strictly
# monotonic, nothing missing, nothing counted twice" reduces to.
first, last, msgs = int(s["first_sequence"]), int(s["last_sequence"]), int(s["messages"])
if last - first + 1 != msgs:
    fails.append(
        f"sequence range {first}..{last} spans {last - first + 1} but only {msgs} "
        "messages were accepted — something was counted twice or skipped"
    )

# Both arms have to be carrying traffic, or the redundancy is decorative and the
# A/B arbitration in milestone 3 would be testing nothing. This checks datagrams
# received, not which arm won: on a quiet host the two land microseconds apart
# and the winner is decided by poll order, not by health.
for arm in ("a", "b"):
    if int(s.get(f"datagrams_{arm}", 0)) == 0:
        fails.append(f"arm {arm.upper()} received no datagrams — the second channel is not live")

# A mirrored arm that never delivers a duplicate means only one arm is really
# being read.
if int(s.get("duplicates_a", 0)) + int(s.get("duplicates_b", 0)) == 0:
    fails.append("no duplicates were seen at all, so the two arms are not both being consumed")

if fails:
    for f in fails:
        print(f"FAIL: {f}", file=sys.stderr)
    sys.exit(1)

print(
    f"  handler: {msgs} messages, sequence {first}..{last}, 0 gaps"
)
print(
    f"  arms: A {s['first_arrivals_a']} first / {s['duplicates_a']} dup, "
    f"B {s['first_arrivals_b']} first / {s['duplicates_b']} dup"
)
PY

    if grep -q "self-check ok" "$ENGINE_LOG"; then
        echo "  engine self-check: the feed rebuilds the engine's own book"
    else
        echo "FAIL: the engine's self-check did not report success" >&2
        status=1
    fi

    if [[ $status -eq 0 ]]; then
        echo "  PASS ($MODE)"
    else
        echo "  FAIL ($MODE)"
        overall=1
    fi
    echo
done

if [[ $KEEP -eq 0 && $overall -eq 0 ]]; then
    rm -rf "$OUT"
fi

if [[ $overall -eq 0 ]]; then
    echo "smoke: PASS"
else
    echo "smoke: FAIL — artifacts kept in $OUT" >&2
fi
exit $overall
