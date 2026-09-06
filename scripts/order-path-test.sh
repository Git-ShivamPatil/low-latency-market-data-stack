#!/usr/bin/env bash
#
# One order across the whole stack, and the three things that have to be true
# about it.
#
# Five processes, four shared-memory rings, one multicast feed and one FIX
# session:
#
#   FIX client ──FIX──▶ gateway ──ring──▶ risk ──ring──▶ engine ──UDP──▶ handler
#              ◀──FIX── gateway ◀─ring── risk ◀─ring── engine
#
# # What each scenario is for
#
#   1. An order crosses resting liquidity. It becomes a fill, the fill comes
#      back as a FIX ExecutionReport, and the handler -- a separate process that
#      has never heard of the order path -- rebuilds the same book the engine
#      holds. That last assertion is the only one that proves the two halves of
#      this system are the same system. Everything before it could be satisfied
#      by an order path talking to a matching engine nobody is watching.
#
#   2. An order over a limit is rejected, and **never reaches the engine**. The
#      engine's own counter is the witness: risk saying "I rejected it" is risk
#      marking its own homework.
#
#   3. The gateway restarts and reconciles. It reports what it found and repairs
#      nothing, which is the milestone's requirement in its own words.
#
#   scripts/order-path-test.sh
#   scripts/order-path-test.sh --keep    # leave the logs, rings and stores

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

BUILD="${MDSTACK_BUILD_DIR:-$REPO/cpp/build}"
OUT="$REPO/results/order-path"
PORT=5601
FEED_A=127.0.0.1:31101
FEED_B=127.0.0.1:31102

# ACME, from configs/local.toml: id 1, reference price 100.0000, tick 0.01.
SYMBOL_NAME=ACME
SYMBOL_ID=1
REF_PRICE=1000000

rm -rf "$OUT"
mkdir -p "$OUT"

echo "building"
cmake -S cpp -B "$BUILD" -DCMAKE_BUILD_TYPE=RelWithDebInfo >"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2; exit 1; }
cmake --build "$BUILD" --target risk-service fix-gateway >>"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2; exit 1; }
cargo build --release --bin matching-engine --bin feed-handler >>"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2; exit 1; }

RISK="$BUILD/risk/risk-service"
GW="$BUILD/gateway/fix-gateway"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO/target}"
ENGINE="$TARGET_DIR/release/matching-engine"
HANDLER="$TARGET_DIR/release/feed-handler"

status=0
fail() { echo "  FAIL: $*" >&2; status=1; }
ok()   { echo "  ok   $*"; }

val() { grep -E "^$1=" "$2" 2>/dev/null | head -1 | cut -d= -f2-; }

expect() {
    local what="$1" got="$2" want="$3"
    if [[ "$got" == "$want" ]]; then ok "$what = $got"; else fail "$what = ${got:-<missing>}, expected $want"; fi
}
expect_at_least() {
    local what="$1" got="$2" want="$3"
    if [[ -n "$got" && "$got" -ge "$want" ]]; then ok "$what = $got (needed >= $want)";
    else fail "$what = ${got:-<missing>}, expected at least $want"; fi
}

pids=()
ENGINE_PID=""
cleanup() {
    for p in "${pids[@]:-}"; do kill -TERM "$p" 2>/dev/null; done
    for p in "${pids[@]:-}"; do wait "$p" 2>/dev/null; done
    pids=()
ENGINE_PID=""
}
trap cleanup EXIT

# The engine installs no signal handler on purpose -- scripts/kill-restart-test.sh
# depends on that -- so SIGTERM kills it outright and its summary never prints.
# Every assertion about what the engine received depends on that summary, so the
# engine is given a shorter deadline than everything else and is waited for
# rather than killed.
finish_engine() {
    [[ -n "$ENGINE_PID" ]] || return 0
    wait "$ENGINE_PID" 2>/dev/null
    local keep=()
    for p in "${pids[@]:-}"; do [[ "$p" == "$ENGINE_PID" ]] || keep+=("$p"); done
    pids=("${keep[@]:-}")
    ENGINE_PID=""
}

wait_for() {
    local needle="$1" file="$2" seconds="${3:-10}"
    for _ in $(seq 1 $((seconds * 20))); do
        grep -q "$needle" "$file" 2>/dev/null && return 0
        sleep 0.05
    done
    return 1
}

# ---------------------------------------------------------------------------
# The stack. The risk service creates all four rings and therefore starts
# first; docs/ORDER-PATH.md says so and this is where it matters.
# ---------------------------------------------------------------------------
start_stack() {
    local tag="$1" run_seconds="$2" max_qty="$3"

    "$RISK" --orders "$OUT/orders.ring" --accepted "$OUT/accepted.ring" \
        --execs "$OUT/execs.ring" --reports "$OUT/reports.ring" \
        --symbol "$SYMBOL_ID" --max-order-qty "$max_qty" \
        --max-notional 100000000000 --max-position 1000000 \
        --collar-bps 2000 --reference-price "$REF_PRICE" \
        --run-seconds "$run_seconds" \
        >"$OUT/$tag-risk.out" 2>"$OUT/$tag-risk.log" &
    pids+=($!)
    wait_for "ready=1" "$OUT/$tag-risk.out" 10 || { fail "$tag: the risk service never came up"; return 1; }

    "$HANDLER" --config configs/local.toml --transport unicast-fanout \
        --feed-a "$FEED_A" --feed-b "$FEED_B" \
        --digest-path "$OUT/$tag-handler-digests.txt" --digest-interval 200 \
        --idle-timeout 3000 \
        >"$OUT/$tag-handler.out" 2>"$OUT/$tag-handler.log" &
    pids+=($!)
    sleep 0.5

    "$ENGINE" --config configs/local.toml --transport unicast-fanout \
        --feed-a "$FEED_A" --feed-b "$FEED_B" \
        --order-ring "$OUT/accepted.ring" --exec-ring "$OUT/execs.ring" \
        --duration "$((run_seconds - 6))" --rate 20000 \
        --digest-path "$OUT/$tag-engine-digests.txt" --digest-interval 200 \
        >"$OUT/$tag-engine.out" 2>"$OUT/$tag-engine.log" &
    ENGINE_PID=$!
    pids+=($!)
    wait_for "order path attached" "$OUT/$tag-engine.log" 10 || {
        fail "$tag: the engine never attached to the order path"; return 1; }

    # The acceptor: this is the gateway with the order path on it.
    "$GW" --listen "$PORT" --store "$OUT/$tag-acceptor.seq" \
        --sender EXCHANGE --target CLIENT --heartbeat 5 --run-seconds "$run_seconds" \
        --orders-ring "$OUT/orders.ring" --reports-ring "$OUT/reports.ring" \
        --order-store "$OUT/orders.log" --symbol "$SYMBOL_NAME=$SYMBOL_ID" \
        "${@:4}" \
        >"$OUT/$tag-acceptor.out" 2>"$OUT/$tag-acceptor.log" &
    pids+=($!)
    wait_for "order path attached" "$OUT/$tag-acceptor.log" 10 || {
        fail "$tag: the gateway never attached to the order path"; return 1; }

    # Time for the generator to build a book worth crossing. Without resting
    # liquidity the client's order just rests, and scenario 1 proves nothing.
    sleep 1.5
    return 0
}

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " one: an order crosses, fills, and reaches the handler's book"
echo "=============================================================="
if start_stack one 14 500; then
    # 105.0000 against a book around 100.0000: aggressive enough to cross, and
    # inside the 20% collar so the collar is exercised as a pass rather than
    # skipped.
    "$GW" --connect "127.0.0.1:$PORT" --store "$OUT/one-client.seq" \
        --sender CLIENT --target EXCHANGE --heartbeat 5 --run-seconds 6 --reset-seq \
        --client-order "$SYMBOL_NAME:1:100:105.0000" \
        >"$OUT/one-client.out" 2>"$OUT/one-client.log"
    client_status=$?
    # Let the engine reach its deadline and print what it saw.
    finish_engine
    sleep 1
    cleanup
    sleep 0.5

    R="$OUT/one-risk.out"
    A="$OUT/one-acceptor.out"
    C="$OUT/one-client.out"

    expect         "risk: orders received"           "$(val orders_in "$R")" 1
    expect         "risk: orders forwarded"          "$(val forwarded "$R")" 1
    expect         "risk: orders rejected"           "$(val rejected "$R")" 0
    expect_at_least "risk: reports handed back"      "$(val reports_out "$R")" 1
    expect         "risk: reports dropped"           "$(val reports_dropped "$R")" 0
    # The service's own account of the claim, from a real run rather than a test.
    expect         "risk: steady-state allocations"  "$(val steady_state_allocations "$R")" 0

    expect          "gateway: orders sent to risk"    "$(val orders_sent "$A")" 1
    expect_at_least "gateway: reports received"       "$(val reports_received "$A")" 1
    expect_at_least "gateway: FIX execution reports"  "$(val exec_reports_sent "$A")" 1
    expect_at_least "gateway: fills"                  "$(val fills "$A")" 1
    expect          "gateway: order log fsyncs match its records" \
        "$(val order_log_syncs "$A")" "$(val order_log_records "$A")"

    # 'F' is ExecType=Trade. The client is a separate process that only ever saw
    # FIX, so this is the round trip closing.
    expect_at_least "client: ExecutionReports of type Trade" "$(val exec_report_F "$C")" 1
    [[ $client_status -eq 0 ]] || fail "the client exited $client_status"

    grep -q "orders_received" "$OUT/one-engine.log" 2>/dev/null
    if grep -qE "order path: [1-9][0-9]* orders" "$OUT/one-engine.log"; then
        ok "engine: the order arrived over the ring"
    else
        fail "engine: no order arrived; $(grep -o 'order path:.*' "$OUT/one-engine.log" | head -1)"
    fi
    if grep -q "EXECUTION REPORTS WERE DROPPED" "$OUT/one-engine.log"; then
        fail "engine: dropped execution reports"
    fi

    # And the assertion that makes the rest mean something: a process that has
    # never heard of the order path rebuilt the same book.
    python3 - "$OUT/one-engine-digests.txt" "$OUT/one-handler-digests.txt" <<'PY'
import sys

def load(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                seq, _, rest = line.partition(" ")
                if seq.isdigit():
                    out[int(seq)] = rest
    except FileNotFoundError:
        return None
    return out

eng = load(sys.argv[1])
han = load(sys.argv[2])
if not eng:
    print("  FAIL: the engine wrote no checkpoints", file=sys.stderr); sys.exit(1)
if not han:
    print("  FAIL: the handler wrote no checkpoints", file=sys.stderr); sys.exit(1)
shared = sorted(set(eng) & set(han))
if not shared:
    print("  FAIL: no shared checkpoints, so the books were never compared", file=sys.stderr)
    sys.exit(1)
bad = [s for s in shared if eng[s] != han[s]]
if bad:
    print(f"  FAIL: the books disagree at {len(bad)} of {len(shared)} checkpoints, "
          f"first at sequence {bad[0]}", file=sys.stderr)
    sys.exit(1)
print(f"  ok   handler and engine books identical at all {len(shared)} shared checkpoints")
PY
    [[ $? -eq 0 ]] || fail "the handler's book does not match the engine's"
fi

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " two: a limit breach is rejected and never reaches the engine"
echo "=============================================================="
rm -f "$OUT/orders.log" "$OUT"/*.ring
if start_stack two 12 500; then
    # 100 is inside the 500 limit; 5000 is not. Both go down the same wire, and
    # exactly one of them may come out the far end.
    "$GW" --connect "127.0.0.1:$PORT" --store "$OUT/two-client.seq" \
        --sender CLIENT --target EXCHANGE --heartbeat 5 --run-seconds 6 --reset-seq \
        --client-order "$SYMBOL_NAME:1:100:105.0000" \
        --client-order "$SYMBOL_NAME:1:5000:105.0000" \
        >"$OUT/two-client.out" 2>"$OUT/two-client.log"
    client_status=$?
    # Let the engine reach its deadline and print what it saw.
    finish_engine
    sleep 1
    cleanup
    sleep 0.5

    R="$OUT/two-risk.out"
    C="$OUT/two-client.out"
    expect "risk: orders received"  "$(val orders_in "$R")" 2
    expect "risk: orders forwarded" "$(val forwarded "$R")" 1
    expect "risk: orders rejected"  "$(val rejected "$R")" 1

    # The witness. Risk reporting its own rejection is risk marking its own
    # homework; the engine's count is what proves the order was stopped.
    received="$(grep -oE 'order path: [0-9]+ orders' "$OUT/two-engine.log" | grep -oE '[0-9]+' | head -1)"
    expect "engine: orders that actually arrived" "${received:-<none>}" 1

    # And the client was told, with a reason rather than silence.
    expect_at_least "client: ExecutionReports of type Rejected" "$(val exec_report_8 "$C")" 1
    if grep -q "quantity above the limit" "$OUT/two-client.log" "$OUT/two-acceptor.log" 2>/dev/null; then
        ok "and the reject carried the reason in Text(58)"
    fi
fi

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " three: SIGKILL the gateway with an order working, then reconcile"
echo "=============================================================="
rm -f "$OUT/orders.log" "$OUT"/*.ring
if start_stack three 20 500; then
    # A passive buy far below the market. It rests rather than filling, which is
    # the whole point: an order that is still working when the process dies is
    # the only kind reconciliation has anything to say about.
    "$GW" --connect "127.0.0.1:$PORT" --store "$OUT/three-client.seq"         --sender CLIENT --target EXCHANGE --heartbeat 5 --run-seconds 4 --reset-seq         --client-order "$SYMBOL_NAME:1:50:90.0000"         >"$OUT/three-client.out" 2>"$OUT/three-client.log"
    sleep 0.5

    # The acceptor is the last pid started by start_stack.
    ACCEPTOR_PID="${pids[-1]}"
    # SIGKILL, not SIGTERM. No destructors, no flush, no chance to write
    # anything on the way out -- so everything the restart knows has to come
    # from what was already fsync'd.
    kill -9 "$ACCEPTOR_PID" 2>/dev/null
    wait "$ACCEPTOR_PID" 2>/dev/null
    unset 'pids[-1]'
    echo "  the gateway was killed with SIGKILL while an order was working"

    log_size_before=$(stat -c %s "$OUT/orders.log" 2>/dev/null || echo 0)
    if [[ "${log_size_before:-0}" -gt 0 ]]; then
        ok "its order log survived ($log_size_before bytes)"
    else
        fail "the order log is empty, so nothing was durable"
    fi

    # Restart against the same log, and ask the engine what it is holding.
    "$GW" --listen "$PORT" --store "$OUT/three-acceptor.seq"         --sender EXCHANGE --target CLIENT --heartbeat 5 --run-seconds 10         --orders-ring "$OUT/orders.ring" --reports-ring "$OUT/reports.ring"         --order-store "$OUT/orders.log" --symbol "$SYMBOL_NAME=$SYMBOL_ID" --reconcile         >"$OUT/three-restart.out" 2>"$OUT/three-restart.log" &
    pids+=($!)
    wait_for "order path attached" "$OUT/three-restart.log" 10 ||
        fail "the restarted gateway never attached"

    # Reconciliation only runs once a session is up, so a client has to log on.
    "$GW" --connect "127.0.0.1:$PORT" --store "$OUT/three-client2.seq"         --sender CLIENT --target EXCHANGE --heartbeat 5 --run-seconds 5 --reset-seq         >"$OUT/three-client2.out" 2>"$OUT/three-client2.log"
    finish_engine
    sleep 1
    cleanup
    sleep 0.5

    A="$OUT/three-restart.out"
    if grep -q "order(s) recovered" "$OUT/three-restart.log"; then
        ok "$(grep -o 'order path attached.*' "$OUT/three-restart.log" | head -1)"
    fi
    expect         "reconciliation completed" "$(val reconciliation_complete "$A")" 1
    expect_at_least "orders the gateway rebuilt from its log" "$(val gateway_orders "$A")" 1
    expect_at_least "orders the engine still holds for it"    "$(val engine_orders "$A")" 1
    expect_at_least "orders both sides agree on"              "$(val agree "$A")" 1

    # The point of the whole exercise: nothing was repaired. The gateway's live
    # set is what its own log says, and any difference is a line in a report
    # rather than a silent adjustment.
    engine_only="$(val engine_only "$A")"
    gw_only="$(val gateway_only "$A")"
    echo "  divergence: $(val agree "$A") agreeing, ${gw_only:-?} gateway-only, ${engine_only:-?} engine-only"
    if grep -q "RECONCILIATION" "$OUT/three-restart.log"; then
        # A divergence here is not a failure of the test -- it is the report
        # doing its job -- but it should be visible rather than buried.
        echo "  note: $(grep -o 'RECONCILIATION.*' "$OUT/three-restart.log" | head -1)"
    fi
fi


echo
if [[ $status -eq 0 ]]; then
    echo "order-path: PASS — an order crossed five processes and four rings,"
    echo "            came back as a fill over FIX, and turned up in the book of"
    echo "            a process that only ever watched the feed"
    [[ $KEEP -eq 0 ]] && rm -rf "$OUT"
else
    echo "order-path: FAIL — artifacts in $OUT" >&2
fi
exit $status
