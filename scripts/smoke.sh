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
# Loss injected on exactly one arm per dropped datagram. Under that model
# "single-arm loss costs nothing" is a property the code either has or does not,
# so zero gaps here is a real assertion rather than a lucky run. The statistics
# of independent loss are covered by the redundancy integration tests, which can
# predict exactly which datagrams die and check the reported gaps against them.
DROP_RATE=0.02
DROP_MODE=exclusive
# The recovery scenario, run after the redundancy one. It forces loss on BOTH
# arms so redundancy cannot help, then requires the handler to rebuild from a
# snapshot and end LIVE with a book that matches the engine's.
RECOVERY=1
RECOVERY_DROP_RATE=0.003
RECOVERY_SNAPSHOT_MS=200
# Rate-limited on purpose: unthrottled, the engine finishes before the first
# snapshot cycle is due, and the run would test nothing.
RECOVERY_RATE=20000
RECOVERY_MESSAGES=20000

while [[ $# -gt 0 ]]; do
    case "$1" in
        --transport) MODES=("$2"); shift 2 ;;
        --messages) MESSAGES="$2"; shift 2 ;;
        --digest-interval) DIGEST_INTERVAL="$2"; shift 2 ;;
        --drop-rate) DROP_RATE="$2"; shift 2 ;;
        --drop-mode) DROP_MODE="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --no-recovery) RECOVERY=0; shift ;;
        -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

OUT="$REPO/results/smoke"
mkdir -p "$OUT"

echo "building release binaries"
cargo build --release --bin matching-engine --bin feed-handler --bin replay-service
ENGINE="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/matching-engine"
HANDLER="$(dirname "$ENGINE")/feed-handler"
REPLAY="$(dirname "$ENGINE")/replay-service"
echo "  engine  $ENGINE"
echo "  handler $HANDLER"
echo

overall=0

for MODE in "${MODES[@]}"; do
    echo "=============================================================="
    echo " transport: $MODE   loss: $DROP_RATE $DROP_MODE"
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
        --drop-rate "$DROP_RATE" \
        --drop-mode "$DROP_MODE" \
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
want("state", "LIVE", "a run that ends GAPPED has lost messages it cannot recover")
want("gaps", 0, "with loss on only one arm at a time, redundancy must cover every one")
want("messages_missed", 0, "nothing may be skipped")
want("bad_datagrams", 0, "every datagram must decode")
want("apply_errors", 0, "every message must apply to the book it describes")
want("joined_mid_stream", "false", "the handler must have seen sequence 1")

# Loss was injected, so the reorder window must actually have been used. If
# nothing was ever buffered out of order, the arbitration path this milestone
# exists for was not exercised and the zero-gap result proves nothing.
buffered = int(s.get("datagrams_buffered_a", 0)) + int(s.get("datagrams_buffered_b", 0))
if buffered == 0:
    fails.append(
        "no datagram was ever buffered out of order, so the reorder window was "
        "never exercised - the run cannot support a claim about arbitration"
    )
window_used = int(s.get("reorder_window_used", 0))
window_cap = int(s.get("reorder_window_capacity", 1))
if window_used >= window_cap:
    fails.append(
        f"the reorder window peaked at {window_used} of {window_cap}; it was at its "
        "limit, so a slightly worse run would have forced gaps that are not real"
    )

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
        fails.append(f"arm {arm.upper()} received no datagrams - the second channel is not live")
    if int(s.get(f"messages_first_{arm}", 0)) == 0:
        fails.append(
            f"arm {arm.upper()} never delivered a message first - with loss on both arms it "
            "must have covered for the other one at some point"
        )

# A mirrored arm that never delivers a duplicate means only one arm is really
# being read.
if int(s.get("datagrams_duplicate_a", 0)) + int(s.get("datagrams_duplicate_b", 0)) == 0:
    fails.append("no duplicates were seen at all, so the two arms are not both being consumed")

if fails:
    for f in fails:
        print(f"FAIL: {f}", file=sys.stderr)
    sys.exit(1)

print(f"  handler: {msgs} messages, sequence {first}..{last}, {s['state']}, 0 gaps")
print(
    f"  arms: A {s['messages_first_a']} msgs first / {s['datagrams_duplicate_a']} dup, "
    f"B {s['messages_first_b']} msgs first / {s['datagrams_duplicate_b']} dup"
)
print(
    f"  reorder: {buffered} datagrams held out of order, window peaked at "
    f"{window_used}/{window_cap}"
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

# --------------------------------------------------------------------------
# Recovery: loss on both arms, then rebuild from a snapshot.
# --------------------------------------------------------------------------
if [[ $RECOVERY -eq 1 ]]; then
    echo "=============================================================="
    echo " recovery: ${RECOVERY_DROP_RATE} correlated loss, ${RECOVERY_SNAPSHOT_MS}ms snapshots"
    echo "=============================================================="

    E_DIG="$OUT/recovery-engine.txt"
    H_DIG="$OUT/recovery-handler.txt"
    H_SUM="$OUT/recovery-handler.summary"
    E_LOG="$OUT/recovery-engine.log"
    H_LOG="$OUT/recovery-handler.log"
    rm -f "$E_DIG" "$H_DIG" "$H_SUM" "$E_LOG" "$H_LOG"

    "$HANDLER"         --config configs/local.toml         --transport unicast-fanout         --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002         --messages "$RECOVERY_MESSAGES"         --digest-path "$H_DIG"         --digest-interval "$DIGEST_INTERVAL"         --idle-timeout "$IDLE_TIMEOUT"         --summary-path "$H_SUM"         >"$H_LOG" 2>&1 &
    RPID=$!
    sleep 1

    "$ENGINE"         --config configs/local.toml         --transport unicast-fanout         --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002         --messages "$RECOVERY_MESSAGES"         --rate "$RECOVERY_RATE"         --snapshot-interval "$RECOVERY_SNAPSHOT_MS"         --drop-rate "$RECOVERY_DROP_RATE"         --drop-mode correlated         --digest-path "$E_DIG"         >"$E_LOG" 2>&1
    wait $RPID && rstatus=0 || rstatus=$?

    echo "--- handler ---"
    grep -E "GAP|RECOVERED|recovery failed|NOT CLEAN" "$H_LOG" | head -8 | sed 's/^/  /'
    echo

    rec=0
    if [[ $rstatus -ne 0 ]]; then
        echo "FAIL: the handler exited $rstatus after a recovery run" >&2
        rec=1
    fi

    python3 - "$H_SUM" "$E_DIG" "$H_DIG" <<'PY' || rec=1
import sys

summary_path, engine_path, handler_path = sys.argv[1], sys.argv[2], sys.argv[3]
with open(summary_path) as f:
    s = dict(line.strip().split("=", 1) for line in f if "=" in line)

fails = []
gaps = int(s.get("gaps", 0))
recoveries = int(s.get("recoveries", 0))

# The scenario has to actually produce gaps, or it proves nothing about
# recovering from them.
if gaps == 0:
    fails.append(
        "no gap ever occurred, so the recovery path was never exercised - raise "
        "the drop rate or lengthen the run"
    )
if recoveries == 0:
    fails.append("no recovery completed")
if s.get("state") != "LIVE":
    fails.append(f"the run ended {s.get('state')}, not LIVE - it never got back to a good book")
if int(s.get("recovery_failures", 0)) != 0:
    fails.append(f"{s['recovery_failures']} recovery attempts failed")
if s.get("still_recovering") != "false":
    fails.append("the run ended mid-recovery, holding traffic it never applied")
if int(s.get("apply_errors", 0)) != 0:
    fails.append(
        f"{s['apply_errors']} messages did not apply - a recovered book that "
        "rejects live traffic was not correctly rebuilt"
    )

# Recovery has to be bounded, not merely eventual.
worst = int(s.get("recovery_worst_millis", 0))
LIMIT = 2000
if worst > LIMIT:
    fails.append(f"the worst recovery took {worst}ms, over the {LIMIT}ms budget")

# And the point of it all: the rebuilt book has to be the engine's book.
def load(path):
    rows = {}
    for line in open(path):
        parts = line.split()
        if len(parts) == 4:
            rows[int(parts[0])] = tuple(parts[1:])
    return rows

engine, handler = load(engine_path), load(handler_path)
shared = sorted(set(engine) & set(handler))
if not shared:
    fails.append("no shared checkpoints, so the books were never compared")
bad = [x for x in shared if engine[x] != handler[x]]
if bad:
    x = bad[0]
    fails.append(
        f"the books disagree at {len(bad)} of {len(shared)} checkpoints; first at "
        f"sequence {x}: engine {engine[x]} vs handler {handler[x]}"
    )

if fails:
    for f in fails:
        print(f"FAIL: {f}", file=sys.stderr)
    sys.exit(1)

print(f"  {gaps} gaps, {recoveries} recoveries, worst {worst}ms, ended {s['state']}")
print(f"  {len(shared)} shared checkpoints after recovery, every one identical")
print(f"  snapshots seen {s.get('snapshots_seen')}, discarded {s.get('snapshots_discarded')}")
PY

    if [[ $rec -eq 0 ]]; then
        echo "  PASS (recovery)"
    else
        echo "  FAIL (recovery)"
        overall=1
    fi
    echo

    # ----------------------------------------------------------------------
    # The same scenario with a replay service, which should fill the gaps
    # exactly rather than fall back to waiting for a snapshot.
    # ----------------------------------------------------------------------
    echo "=============================================================="
    echo " replay: same loss, with a replay service to fill gaps exactly"
    echo "=============================================================="

    RE_DIG="$OUT/replay-engine.txt"
    RH_DIG="$OUT/replay-handler.txt"
    RH_SUM="$OUT/replay-handler.summary"
    RE_LOG="$OUT/replay-engine.log"
    RH_LOG="$OUT/replay-handler.log"
    RS_LOG="$OUT/replay-service.log"
    rm -f "$RE_DIG" "$RH_DIG" "$RH_SUM" "$RE_LOG" "$RH_LOG" "$RS_LOG"

    "$REPLAY" --config configs/local.toml         --uplink-bind 127.0.0.1:32001 --request-bind 127.0.0.1:32002         --history 4096 --duration 60 >"$RS_LOG" 2>&1 &
    RSPID=$!
    sleep 1

    "$HANDLER"         --config configs/local.toml         --transport unicast-fanout         --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002         --replay 127.0.0.1:32002         --messages "$RECOVERY_MESSAGES"         --digest-path "$RH_DIG"         --digest-interval "$DIGEST_INTERVAL"         --idle-timeout "$IDLE_TIMEOUT"         --summary-path "$RH_SUM"         >"$RH_LOG" 2>&1 &
    RHPID=$!
    sleep 1

    "$ENGINE"         --config configs/local.toml         --transport unicast-fanout         --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002         --replay-uplink 127.0.0.1:32001         --messages "$RECOVERY_MESSAGES"         --rate "$RECOVERY_RATE"         --snapshot-interval "$RECOVERY_SNAPSHOT_MS"         --drop-rate "$RECOVERY_DROP_RATE"         --drop-mode correlated         --digest-path "$RE_DIG"         >"$RE_LOG" 2>&1
    wait $RHPID && rpstatus=0 || rpstatus=$?
    kill $RSPID 2>/dev/null || true
    wait $RSPID 2>/dev/null || true

    echo "--- handler ---"
    grep -E "RECOVERED|replay refused|recovery failed|NOT CLEAN" "$RH_LOG" | head -6 | sed 's/^/  /'
    echo

    rp=0
    if [[ $rpstatus -ne 0 ]]; then
        echo "FAIL: the handler exited $rpstatus during the replay run" >&2
        rp=1
    fi

    python3 - "$RH_SUM" "$RE_DIG" "$RH_DIG" <<'PY' || rp=1
import sys

summary_path, engine_path, handler_path = sys.argv[1], sys.argv[2], sys.argv[3]
with open(summary_path) as f:
    s = dict(line.strip().split("=", 1) for line in f if "=" in line)

fails = []
by_replay = int(s.get("recovered_by_replay", 0))
by_snapshot = int(s.get("recovered_by_snapshot", 0))

# The point of this run: gaps get filled by replay rather than fallen back on.
if by_replay == 0:
    fails.append(
        "no gap was recovered by replay - the service was up and holding the "
        "history, so either the request never went out or it was refused"
    )
if int(s.get("gaps", 0)) == 0:
    fails.append("no gap occurred, so nothing about replay was exercised")
if s.get("state") != "LIVE":
    fails.append(f"the run ended {s.get('state')}, not LIVE")
if int(s.get("recovery_failures", 0)) != 0:
    fails.append(f"{s['recovery_failures']} recovery attempts failed")
if int(s.get("apply_errors", 0)) != 0:
    fails.append(
        f"{s['apply_errors']} messages did not apply - a replayed range that "
        "double-applies or misses messages shows up here first"
    )
if s.get("still_recovering") != "false":
    fails.append("the run ended mid-recovery")

# The books have to agree. A replay that filled the hole with the wrong bytes,
# or applied its edges twice, fails here and nowhere else.
def load(path):
    rows = {}
    for line in open(path):
        parts = line.split()
        if len(parts) == 4:
            rows[int(parts[0])] = tuple(parts[1:])
    return rows

engine, handler = load(engine_path), load(handler_path)
shared = sorted(set(engine) & set(handler))
if not shared:
    fails.append("no shared checkpoints, so the books were never compared")
bad = [x for x in shared if engine[x] != handler[x]]
if bad:
    x = bad[0]
    fails.append(
        f"the books disagree at {len(bad)} of {len(shared)} checkpoints; first at "
        f"sequence {x}: engine {engine[x]} vs handler {handler[x]}"
    )

if fails:
    for f in fails:
        print(f"FAIL: {f}", file=sys.stderr)
    sys.exit(1)

print(
    f"  {s['gaps']} gaps: {by_replay} filled by replay, {by_snapshot} by snapshot, "
    f"{s.get('replay_refused')} refused"
)
print(f"  {s['replay_messages']} messages recovered from the replay service")
print(f"  {len(shared)} shared checkpoints, every one identical")
PY

    if [[ $rp -eq 0 ]]; then
        echo "  PASS (replay)"
    else
        echo "  FAIL (replay)"
        overall=1
    fi
    echo
fi

if [[ $KEEP -eq 0 && $overall -eq 0 ]]; then
    rm -rf "$OUT"
fi

if [[ $overall -eq 0 ]]; then
    echo "smoke: PASS"
else
    echo "smoke: FAIL — artifacts kept in $OUT" >&2
fi
exit $overall
