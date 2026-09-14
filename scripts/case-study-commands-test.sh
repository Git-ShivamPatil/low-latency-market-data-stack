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
if [[ $WITH_DOCKER -eq 1 ]] && ! command -v docker >/dev/null 2>&1; then
    # Said plainly rather than left to fail as "docker compose up failed". On the
    # machine this is developed on, Docker Desktop runs Windows-side with WSL
    # integration off, so `docker` exists in PowerShell and not in the distro the
    # rest of this script runs in. That is a property of the host, not a defect,
    # and the `case-study` CI job runs this mode on a runner where it works.
    fail "--with-docker was requested but there is no docker on PATH"
    echo "     Docker Desktop with WSL integration off puts it Windows-side only;" >&2
    echo "     run this from PowerShell or Git Bash, or let the CI job do it." >&2
    WITH_DOCKER=0
fi
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

# Step 4 needs a longer run than steps 2 and 3, and the reason is specific.
#
# `--verify-allocations` does not start counting until the handler has applied
# 50,000 messages, so that the count cannot include anything allocated while the
# process was warming up. A run that ends before then reports "allocations: not
# measured", which is honest and useless -- and the page says this command proves
# the handler does not allocate.
#
# The budget has to be counted in messages that reach the BOOKS, not sequences
# consumed. Step 4 joins mid-stream, so it spends its first seconds holding
# traffic while it waits for a snapshot, and none of that is applied until the
# recovery lands. At configs/local.toml's 20,000 msg/s that is a couple of
# seconds of runway before the counter even arms.
STEP4_ENGINE_SECONDS=16
STEP4_MESSAGES=150000

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
#
# Measured on the sequence range rather than by grepping for a number. The
# obvious greps are both wrong: "sequence|messages" matches the banner the
# handler prints before it has received anything, and "0 messages" matches
# "0 messages that did not apply" -- which is the run reporting that nothing
# went wrong. Both were here, and the second failed a passing run.
seq_range=$(grep -oE "seq [0-9]+\.\.[0-9]+" "$OUT/step3.log" | tail -1)
if [[ -n "$seq_range" ]]; then
    seq_from=${seq_range#seq }
    seq_from=${seq_from%%..*}
    seq_to=${seq_range##*..}
    if [[ "$seq_to" -gt "$seq_from" ]]; then
        ok "and it consumed $((seq_to - seq_from + 1)) sequences of the feed"
    else
        fail "the handler reported an empty sequence range ($seq_range)"
    fi
else
    fail "the handler never reported a sequence range, so steps 2 and 3 did not talk"
fi

# --------------------------------------------------------------------------
echo
echo "4. $STEP4"
# Step 4 names no addresses: it takes them from configs/local.toml, which is
# what makes the command short enough to publish. If that default ever stops
# matching step 3's addresses, this is where it shows up.
# shellcheck disable=SC2086
$STEP2 --duration "$STEP4_ENGINE_SECONDS" >"$OUT/step4-engine.log" 2>&1 &
ENGINE_PID=$!
wait_for_engine "$OUT/step4-engine.log" ||
    fail "the engine never started for step 4"
# shellcheck disable=SC2086
$STEP4 --messages "$STEP4_MESSAGES" --idle-timeout "$HANDLER_IDLE"     >"$OUT/step4.log" 2>&1
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
# Anchored on the report line the handler prints, not on the word "allocation"
# anywhere in the file -- `cargo run` echoes the whole command line, flag
# included, so the loose match found "--verify-allocations" and concluded a
# report existed when none did.
alloc_line=$(grep -E "^ *allocations[:( ]|^ *allocations over " "$OUT/step4.log" | tail -1)
if [[ -z "$alloc_line" ]]; then
    fail "--verify-allocations printed no allocation report at all"
elif [[ "$alloc_line" == *"not measured"* ]]; then
    # Distinct from a non-zero count, and fixed differently: the run was too
    # short to clear the counter's warm-up, so raise STEP4_MESSAGES.
    echo "  note: $alloc_line" >&2
    fail "--verify-allocations never armed; the run ended inside the warm-up"
elif [[ "$alloc_line" == *"0 allocations, 0 deallocations"* ]]; then
    ok "--verify-allocations reported zero over $(echo "$alloc_line" |
        grep -oE "[0-9]+ steady-state passes" || echo "the measured window")"
else
    echo "  note: $alloc_line" >&2
    fail "--verify-allocations did not report zero allocations"
fi

# And `--drop-rate 0.02` has to have actually dropped something, or the command
# demonstrates recovery from a loss that never happened.
#
# Counted, not grepped. Matching the words "drop", "gap" or "recover" anywhere in
# the log passes on a banner, a flag echo or the phrase "0 gaps" -- a check that
# cannot fail is not evidence, which is rule 9 in this project's own list.
discarded=$(grep -oE "discarded [0-9]+ datagrams on A and [0-9]+ on B" "$OUT/step4.log" | tail -1)
if [[ -n "$discarded" ]]; then
    d_a=$(echo "$discarded" | grep -oE "discarded [0-9]+" | grep -oE "[0-9]+")
    d_b=$(echo "$discarded" | grep -oE "and [0-9]+ on B" | grep -oE "[0-9]+")
    if [[ $((d_a + d_b)) -gt 0 ]]; then
        ok "and the injected loss is real: $d_a datagrams on A, $d_b on B"
    else
        fail "--drop-rate 0.02 discarded nothing, so step 4 proves nothing"
    fi
else
    fail "--drop-rate 0.02 reported no discard count, so step 4 proves nothing"
fi
# Declaring gaps is the point of the command: loss that nothing noticed is worse
# than no loss at all.
gaps=$(grep -cE "^  gap: sequence" "$OUT/step4.log")
if [[ "$gaps" -gt 0 ]]; then
    ok "and the handler declared $gaps gap(s) covering it"
else
    fail "loss was injected but no gap was declared"
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
