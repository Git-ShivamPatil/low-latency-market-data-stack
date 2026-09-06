#!/usr/bin/env bash
#
# Milestone 7's headline verification: a FIX session that survives a hard kill.
#
# SIGKILL, not SIGTERM. No destructors run, no buffers flush, no handler gets a
# chance to tidy up — which is the only kind of death worth testing, because a
# session layer that works when shut down politely has not implemented the part
# that matters.
#
# What has to be true on restart:
#
#   1. Both ends resume from the sequence numbers that were durable, not from 1.
#      Resetting to 1 is the failure this whole design exists to prevent: the
#      counterparty would see every subsequent message as a reversal.
#   2. Neither end reports a sequence reversal, which is what a lost or
#      double-issued number produces.
#   3. The session gets back to Active and keeps working.
#
#   scripts/kill-restart-test.sh
#   scripts/kill-restart-test.sh --keep    # leave the logs and stores behind

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

OUT="$REPO/results/killrestart"
rm -rf "$OUT"
mkdir -p "$OUT"

PORT=5401
ACC_STORE="$OUT/acceptor.seq"
INI_STORE="$OUT/initiator.seq"

echo "building the gateway"
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=RelWithDebInfo >/dev/null || exit 1
cmake --build cpp/build --target fix-gateway >/dev/null 2>&1 || {
    cmake --build cpp/build --target fix-gateway
    exit 1
}
GW="$REPO/cpp/build/gateway/fix-gateway"

status=0

fail() {
    echo "FAIL: $*" >&2
    status=1
}

# --------------------------------------------------------------------------
# Round one: establish a session and exchange orders, then kill both ends
# --------------------------------------------------------------------------
echo
echo "=============================================================="
echo " round one: establish, exchange, then SIGKILL"
echo "=============================================================="

"$GW" --listen "$PORT" --store "$ACC_STORE" \
    --sender EXCHANGE --target GATEWAY --heartbeat 5 --run-seconds 20 \
    >"$OUT/acceptor-1.log" 2>&1 &
ACC_PID=$!
sleep 1

"$GW" --connect "127.0.0.1:$PORT" --store "$INI_STORE" \
    --sender GATEWAY --target EXCHANGE --heartbeat 5 --run-seconds 20 --send-orders 5 \
    >"$OUT/initiator-1.log" 2>&1 &
INI_PID=$!

# Long enough to log on and push the orders through, short enough that the run
# is nowhere near its own deadline — the kill has to interrupt a live session,
# not a finished one.
sleep 3

kill -9 "$INI_PID" 2>/dev/null
kill -9 "$ACC_PID" 2>/dev/null
wait "$INI_PID" 2>/dev/null
wait "$ACC_PID" 2>/dev/null
echo "  both ends killed with SIGKILL"

ini_out_1=$(grep -oE 'sequences resume at outbound [0-9]+' "$OUT/initiator-1.log" | grep -oE '[0-9]+$')
echo "  initiator started at outbound ${ini_out_1:-?}"

if ! grep -q "sent" "$OUT/initiator-1.log"; then
    # The process was killed, so its summary line never printed. The store is
    # the evidence, which is the point.
    echo "  (no summary line: the process was killed before it could print one)"
fi

# --------------------------------------------------------------------------
# What the stores say after the kill
# --------------------------------------------------------------------------
[[ -s "$INI_STORE" ]] || fail "the initiator wrote no sequence file"
[[ -s "$ACC_STORE" ]] || fail "the acceptor wrote no sequence file"

# --------------------------------------------------------------------------
# Round two: restart both against the same stores
# --------------------------------------------------------------------------
echo
echo "=============================================================="
echo " round two: restart against the same stores"
echo "=============================================================="

"$GW" --listen "$PORT" --store "$ACC_STORE" \
    --sender EXCHANGE --target GATEWAY --heartbeat 5 --run-seconds 12 \
    >"$OUT/acceptor-2.log" 2>&1 &
ACC_PID=$!
sleep 1

"$GW" --connect "127.0.0.1:$PORT" --store "$INI_STORE" \
    --sender GATEWAY --target EXCHANGE --heartbeat 5 --run-seconds 12 --send-orders 3 \
    >"$OUT/initiator-2.log" 2>&1 &
INI_PID=$!

wait "$INI_PID"; ini_status=$?
wait "$ACC_PID"; acc_status=$?

echo "--- initiator, second run ---"
sed 's/^/  /' "$OUT/initiator-2.log"
echo "--- acceptor, second run ---"
sed 's/^/  /' "$OUT/acceptor-2.log"

# --------------------------------------------------------------------------
# The assertions
# --------------------------------------------------------------------------
echo
ini_resume=$(grep -oE 'sequences resume at outbound [0-9]+' "$OUT/initiator-2.log" \
    | grep -oE '[0-9]+$')
acc_resume=$(grep -oE 'sequences resume at outbound [0-9]+' "$OUT/acceptor-2.log" \
    | grep -oE '[0-9]+$')

if [[ -z "${ini_resume:-}" || -z "${acc_resume:-}" ]]; then
    fail "one of the ends did not report where it resumed"
else
    # The whole claim. Resuming at 1 means the sequence state was lost, and the
    # counterparty would read everything after as a reversal.
    if [[ "$ini_resume" -le 1 ]]; then
        fail "the initiator resumed at $ini_resume — its sequence state did not survive"
    else
        echo "  initiator resumed at outbound $ini_resume, not 1"
    fi
    if [[ "$acc_resume" -le 1 ]]; then
        fail "the acceptor resumed at $acc_resume — its sequence state did not survive"
    else
        echo "  acceptor resumed at outbound $acc_resume, not 1"
    fi
fi

for who in initiator acceptor; do
    if grep -q "sequence number below the expected value" "$OUT/$who-2.log"; then
        fail "$who reported a sequence reversal, which means a number was lost or reissued"
    fi
    if grep -q "ended badly" "$OUT/$who-2.log"; then
        fail "$who ended badly: $(grep 'ended badly' "$OUT/$who-2.log")"
    fi
done

# A torn slot recovered is not a failure — it is the two-slot layout doing its
# job — but it is worth surfacing, because it means the kill landed inside an
# fsync and the fallback was exercised for real rather than only in the unit
# test.
if grep -q "torn slot" "$OUT/initiator-2.log" "$OUT/acceptor-2.log"; then
    echo "  note: a torn slot was recovered, so the kill landed mid-write and the"
    echo "        fallback ran for real"
fi

if [[ $ini_status -ne 0 ]]; then
    fail "the initiator exited $ini_status on its second run"
fi
if [[ $acc_status -ne 0 ]]; then
    fail "the acceptor exited $acc_status on its second run"
fi

echo
if [[ $status -eq 0 ]]; then
    echo "kill-restart: PASS"
    [[ $KEEP -eq 0 ]] && rm -rf "$OUT"
else
    echo "kill-restart: FAIL — artifacts in $OUT" >&2
fi
exit $status
