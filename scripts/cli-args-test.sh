#!/usr/bin/env bash
#
# The command-line contract of the two C++ binaries.
#
# # Why this is a test
#
# `docker-compose.yml` passes every argument in `--flag=value` form, because one
# YAML list entry per argument is the shape that file has. Both parsers compared
# the whole argument against a literal — `a == "--orders"` — so every one of
# those fell through to the usage branch and the process exited 2.
#
# It failed in the quietest possible way. The usage text went to stderr inside a
# container nobody was tailing, the healthcheck never went green, and everything
# downstream reported only "dependency failed to start: unhealthy". The compose
# file had been wrong since the day it was written and `make test` was green
# throughout, because every other suite invokes these binaries with the arguments
# the TESTS need — which is not the same thing as the arguments the deployment
# uses.
#
# So the CLI is a contract now, and this is where it is pinned. It runs in about
# a second and needs no Docker.
#
#   scripts/cli-args-test.sh
#   MDSTACK_BUILD_DIR=cpp/build scripts/cli-args-test.sh

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

BUILD="${MDSTACK_BUILD_DIR:-cpp/build}"
RISK="$BUILD/risk/risk-service"
GW="$BUILD/gateway/fix-gateway"

for bin in "$RISK" "$GW"; do
    if [[ ! -x "$bin" ]]; then
        echo "cli-args: $bin is not built — run \`make build-cpp\` first" >&2
        exit 1
    fi
done

D="$(mktemp -d)"
trap 'rm -rf "$D"' EXIT

status=0
fail() { echo "  FAIL: $*" >&2; status=1; }
ok() { echo "  ok   $*"; }

echo "the command-line contract of risk-service and fix-gateway"
echo

# --------------------------------------------------------------------------
# The four ring paths, the symbol and every limit, in the exact spelling
# docker-compose.yml uses. If this fails, `docker compose up -d` is broken.
echo "1. risk-service accepts --flag=value"
rm -f "$D"/*.ring
timeout 10 "$RISK" \
    --orders="$D/o.ring" --accepted="$D/a.ring" \
    --execs="$D/e.ring" --reports="$D/r.ring" \
    --symbol=1 --max-order-qty=10000 --max-notional=100000000000 \
    --max-position=1000000 --collar-bps=2000 --reference-price=1000000 \
    --run-seconds=1 >/dev/null 2>"$D/risk-eq.err"
code=$?
[[ $code -eq 0 ]] || fail "exited $code with = form; stderr: $(head -1 "$D/risk-eq.err")"
n=$(ls "$D"/*.ring 2>/dev/null | wc -l)
[[ "$n" -eq 4 ]] && ok "created all four rings" || fail "created $n of 4 rings"

echo
echo "2. risk-service still accepts --flag value"
rm -f "$D"/*.ring
timeout 10 "$RISK" \
    --orders "$D/o.ring" --accepted "$D/a.ring" \
    --execs "$D/e.ring" --reports "$D/r.ring" \
    --symbol 1 --run-seconds 1 >/dev/null 2>&1
code=$?
[[ $code -eq 0 ]] || fail "exited $code with space form"
n=$(ls "$D"/*.ring 2>/dev/null | wc -l)
[[ "$n" -eq 4 ]] && ok "created all four rings" || fail "created $n of 4 rings"

# --------------------------------------------------------------------------
# The creator owns the geometry, so it must start from a clean file. Inode
# numbers are reused the moment they are freed, so a hard link is what shows
# the difference: after an unlink-and-create the new file has one link and the
# backup still holds the old bytes.
echo
echo "3. risk-service replaces a stale ring rather than adopting it"
echo "GARBAGE-NOT-A-RING" >"$D/r.ring"
ln -f "$D/r.ring" "$D/r.keep"
timeout 10 "$RISK" \
    --orders "$D/o.ring" --accepted "$D/a.ring" \
    --execs "$D/e.ring" --reports "$D/r.ring" \
    --symbol 1 --run-seconds 1 >/dev/null 2>&1
links=$(stat -c %h "$D/r.ring")
kept=$(head -c 18 "$D/r.keep")
if [[ "$links" == "1" && "$kept" == "GARBAGE-NOT-A-RING" ]]; then
    ok "unlinked the old file and created a new one"
else
    fail "adopted the stale file (links=$links)"
fi

# --------------------------------------------------------------------------
echo
echo "4. fix-gateway accepts --flag=value"
timeout 5 "$GW" --listen=5001 --bind=127.0.0.1 --store="$D/g1.seq" \
    --sender=EXCHANGE --target=CLIENT --heartbeat=30 --run-seconds=1 \
    >/dev/null 2>"$D/gw-eq.err"
if grep -qi "^usage:" "$D/gw-eq.err"; then
    fail "rejected = form: $(head -1 "$D/gw-eq.err")"
else
    ok "parsed every flag"
fi

echo
echo "5. --symbol=NAME=ID splits on the first = only"
timeout 5 "$GW" --listen=5001 --store="$D/g2.seq" --symbol=ACME=1 \
    --run-seconds=1 >/dev/null 2>"$D/gw-sym.err"
if grep -qi "^usage:\|takes NAME=ID" "$D/gw-sym.err"; then
    fail "rejected --symbol=ACME=1, so the split is eating the value"
else
    ok "kept ACME=1 intact"
fi

# --------------------------------------------------------------------------
# Refusing beats ignoring: a silently dropped value is how somebody ends up
# believing they turned something on.
echo
echo "6. a value on a flag that takes none is refused"
"$GW" --listen=5001 --store="$D/g3.seq" --reconcile=yes >/dev/null 2>"$D/gw-bool.err"
code=$?
if [[ $code -eq 2 ]] && grep -q "takes no value" "$D/gw-bool.err"; then
    ok "--reconcile=yes refused, and said why"
else
    fail "--reconcile=yes exited $code without saying the flag takes no value"
fi

# --------------------------------------------------------------------------
# accept_one can only report failure by returning a closed connection, which the
# session loop prints as "state disconnected" -- true, useless, and identical to
# nobody having called. A typo in an address has to say so.
echo
echo "7. a bad --bind is refused at parse time"
"$GW" --listen=5001 --bind=not-an-address --store="$D/g4.seq" \
    --run-seconds=1 >/dev/null 2>"$D/gw-bind.err"
code=$?
if [[ $code -eq 2 ]] && grep -q "not an IPv4 address" "$D/gw-bind.err"; then
    ok "refused, and named the problem"
else
    fail "a bad --bind exited $code without naming the problem"
fi

# --------------------------------------------------------------------------
echo
if [[ $status -eq 0 ]]; then
    echo "cli-args: PASS — both binaries accept the spelling docker-compose.yml uses"
else
    echo "cli-args: FAIL" >&2
    echo "          docker-compose.yml passes every argument in --flag=value form." >&2
fi
exit $status
