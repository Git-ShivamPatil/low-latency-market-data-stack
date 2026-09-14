#!/usr/bin/env bash
#
# The four commands the case study publishes, run exactly as published.
#
# # Why this is a test
#
# The page prints four commands for a stranger to copy. That makes them part of
# the public surface, not illustration: a renamed binary, a moved config or a
# dropped flag turns the page into four commands that do not run, in front of
# precisely the reader who tries them.
#
# Nothing else in this repository would catch that. `make test` runs the
# binaries with the arguments the *tests* need, which is not the same thing as
# the arguments the page promises.
#
#   scripts/case-study-commands-test.sh
#   scripts/case-study-commands-test.sh --with-docker   # also run step 1
#
# Step 1 is `docker compose up -d`, which needs a working Docker. It is opt-in
# rather than skipped silently: the script says which mode it ran in, so a green
# result never quietly means "three of four".

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

WITH_DOCKER=0
[[ "${1:-}" == "--with-docker" ]] && WITH_DOCKER=1

OUT="$REPO/results/case-study"
rm -rf "$OUT"
mkdir -p "$OUT"

status=0
fail() { echo "  FAIL: $*" >&2; status=1; }
ok()   { echo "  ok   $*"; }

# The four commands, verbatim from prisma/seed.ts on the portfolio site. If one
# of these strings has to change, the page has to change with it -- which is the
# entire point of keeping them here as literals rather than as variables.
STEP1='docker compose up -d'
STEP2='cargo run --release --bin matching-engine -- --config configs/local.toml'
STEP3='cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001'
STEP4='cargo run --release --bin feed-handler -- --drop-rate 0.02 --verify-allocations'

echo "the four commands the case study publishes"
echo

# --------------------------------------------------------------------------
echo "1. $STEP1"
if [[ $WITH_DOCKER -eq 1 ]]; then
    if $STEP1 >"$OUT/step1.log" 2>&1; then
        ok "the stack came up"
        docker compose ps >>"$OUT/step1.log" 2>&1
        # Every service the case study's architecture diagram draws.
        for svc in engine replay handler risk gateway; do
            if docker compose ps --services 2>/dev/null | grep -qx "$svc"; then
                ok "  service '$svc' is defined"
            else
                fail "  the compose file has no '$svc' service, but the page draws that node"
            fi
        done
        docker compose down >>"$OUT/step1.log" 2>&1
    else
        fail "docker compose up failed; see $OUT/step1.log"
    fi
else
    # Not skipped quietly. The file is still checked for the things that make
    # the command work at all, so a moved compose file or a lost service still
    # fails here.
    echo "  (not run: pass --with-docker to start containers)"
    [[ -f docker-compose.yml ]] ||
        fail "docker-compose.yml is not in the repository root, where the command expects it"
    for svc in engine replay handler risk gateway; do
        if grep -qE "^  $svc:" docker-compose.yml; then
            ok "  service '$svc' is defined"
        else
            fail "  the compose file has no '$svc' service, but the page draws that node"
        fi
    done
fi

# --------------------------------------------------------------------------
echo
echo "building the binaries the remaining three commands name"
cargo build --release --bin matching-engine --bin feed-handler >"$OUT/build.log" 2>&1 || {
    cat "$OUT/build.log" >&2
    fail "the binaries the page names do not build"
    exit 1
}

# The page's steps 2, 3 and 4 run until interrupted, which a test cannot do. A
# stop condition is added and *nothing else is changed*, so every flag the page
# prints is still exercised exactly as printed.
ENGINE_SECONDS=8
HANDLER_MESSAGES=20000
HANDLER_IDLE=5

# `cargo run` may still compile, and on a Windows-mounted filesystem that is not
# quick. Two of them racing for the build lock is what this waits out: step 3
# does not start until step 2 has stopped building and started running.
wait_for_engine() {
    for _ in $(seq 1 600); do
        grep -q "symbols, seed" "$1" 2>/dev/null && return 0
        sleep 0.2
    done
    return 1
}

echo
echo "2. $STEP2"
# shellcheck disable=SC2086
$STEP2 --duration "$ENGINE_SECONDS" >"$OUT/step2.log" 2>&1 &
ENGINE_PID=$!
if wait_for_engine "$OUT/step2.log"; then
    ok "the engine started and is publishing"
else
    fail "the engine never started publishing; see $OUT/step2.log"
fi

echo
echo "3. $STEP3"
# shellcheck disable=SC2086
$STEP3 --messages "$HANDLER_MESSAGES" --idle-timeout "$HANDLER_IDLE"     >"$OUT/step3.log" 2>&1
STEP3_STATUS=$?
wait "$ENGINE_PID"
ENGINE_STATUS=$?

if [[ $ENGINE_STATUS -eq 0 ]]; then
    ok "the engine ran and exited cleanly"
else
    fail "the engine exited $ENGINE_STATUS; see $OUT/step2.log"
fi
if [[ $STEP3_STATUS -eq 0 ]]; then
    ok "the handler ran and exited cleanly"
else
    fail "the handler exited $STEP3_STATUS; see $OUT/step3.log"
fi

# The handler has to have actually received the feed. One that starts, receives
# nothing and exits cleanly would pass every check above.
if grep -qE "sequence|messages" "$OUT/step3.log"; then
    ok "and it reported on the feed it received"
else
    fail "the handler reported nothing, so steps 2 and 3 did not talk to each other"
fi
if grep -qE "0 messages|received 0" "$OUT/step3.log"; then
    fail "the handler received zero messages"
fi

# --------------------------------------------------------------------------
echo
echo "4. $STEP4"
# Step 4 names no addresses: it takes them from configs/local.toml, which is
# what makes the command short enough to publish. If that default ever stops
# matching step 3's addresses, this is where it shows up.
# shellcheck disable=SC2086
$STEP2 --duration "$ENGINE_SECONDS" >"$OUT/step4-engine.log" 2>&1 &
ENGINE_PID=$!
wait_for_engine "$OUT/step4-engine.log" ||
    fail "the engine never started for step 4"
# shellcheck disable=SC2086
$STEP4 --messages "$HANDLER_MESSAGES" --idle-timeout "$HANDLER_IDLE"     >"$OUT/step4.log" 2>&1
STEP4_STATUS=$?
wait "$ENGINE_PID" 2>/dev/null

if [[ $STEP4_STATUS -eq 0 ]]; then
    ok "the recovery command ran and exited cleanly"
else
    fail "the recovery command exited $STEP4_STATUS; see $OUT/step4.log"
fi

# `--verify-allocations` is half the point of step 4. The page says this command
# proves the handler does not allocate; if it prints no allocation report, the
# page is describing something that did not happen.
if grep -qE "allocation" "$OUT/step4.log"; then
    if grep -qE "^ *0 allocations|allocations 0|allocations=0" "$OUT/step4.log" ||
       grep -qE "0 allocations, 0 deallocations" "$OUT/step4.log"; then
        ok "--verify-allocations reported zero"
    else
        echo "  note: $(grep -m1 -E 'allocation' "$OUT/step4.log")"
        fail "--verify-allocations did not report zero allocations"
    fi
else
    fail "--verify-allocations printed no allocation report"
fi

# And `--drop-rate 0.02` has to have actually dropped something, or the command
# demonstrates recovery from a loss that never happened.
if grep -qiE "drop|gap|recover" "$OUT/step4.log"; then
    ok "and the injected loss is reported"
else
    fail "--drop-rate 0.02 produced no visible loss, so step 4 proves nothing"
fi


# --------------------------------------------------------------------------
echo
if [[ $status -eq 0 ]]; then
    if [[ $WITH_DOCKER -eq 1 ]]; then
        echo "case-study commands: PASS — all four, exactly as published"
    else
        echo "case-study commands: PASS — steps 2, 3 and 4 exactly as published;"
        echo "                     step 1 checked structurally (pass --with-docker to run it)"
    fi
    rm -rf "$OUT"
else
    echo "case-study commands: FAIL — artifacts in $OUT" >&2
    echo "                     The page publishes these four commands verbatim." >&2
fi
exit $status
