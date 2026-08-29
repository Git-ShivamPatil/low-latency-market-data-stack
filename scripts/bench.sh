#!/usr/bin/env bash
#
# Milestone 6's harness.
#
# Runs the host gate first. If the host cannot produce a publishable number, the
# benchmarks still run — exercising them is how the harness gets debugged — but
# nothing is written that could later be mistaken for a result, and every output
# file is stamped NOT-PUBLISHABLE.
#
# That refusal is the point. This project's portfolio page already advertises
# "1M+ msg/s", and the way that becomes a false claim is not dishonesty: it is a
# benchmark that ran once on a laptop, printed something plausible, and got
# quoted three weeks later by someone who no longer remembered where it came
# from.
#
#   scripts/bench.sh check                 # just the host gate
#   scripts/bench.sh micro                 # Criterion: decode and book update
#   scripts/bench.sh inpath                # engine + handler, rdtsc histogram
#   scripts/bench.sh throughput            # 60s sustained, receiver-side, x3
#   scripts/bench.sh all
#
#   --force        run as if the host were publishable (the report says it was
#                  forced, and CLAIMS.md must not carry a forced number)
#   --duration N   seconds per throughput run (default 60)
#   --messages N   messages for the in-path latency run (default 2000000)
#   --runs N       throughput repetitions (default 3)

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

WHAT="${1:-all}"
[[ "$WHAT" == --* ]] && WHAT=all || shift || true

FORCE=0
DURATION=60
RUNS=3
# Messages for the in-path latency run. Two million is enough for a p99.9 to
# mean something; smaller values exist so the harness itself can be exercised
# on a machine that would take four minutes to reach it.
INPATH_MESSAGES=2000000
# The agreement the milestone requires across repetitions.
TOLERANCE=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        --duration) DURATION="$2"; shift 2 ;;
        --messages) INPATH_MESSAGES="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        -h|--help) sed -n '2,26p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

OUT="$REPO/results/bench"
mkdir -p "$OUT"

echo "building release binaries"
cargo build --release --bin matching-engine --bin feed-handler --bin hostcheck || exit 1
BINDIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release"
ENGINE="$BINDIR/matching-engine"
HANDLER="$BINDIR/feed-handler"
HOSTCHECK="$BINDIR/hostcheck"

# --------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------
echo
echo "=============================================================="
echo " host gate"
echo "=============================================================="
"$HOSTCHECK" | tee "$OUT/hostcheck.txt"
PUBLISHABLE=$?
"$HOSTCHECK" --fields > "$OUT/host.fields" 2>/dev/null

if [[ $PUBLISHABLE -ne 0 ]]; then
    if [[ $FORCE -eq 1 ]]; then
        echo
        echo "  --force given: continuing anyway. Every output is stamped NOT-PUBLISHABLE"
        echo "  and no CLAIMS.md row may cite this run."
    else
        echo
        echo "  Continuing: the benchmarks are worth exercising on any machine."
        echo "  Nothing publishable will be written. Pass --force to say you know."
    fi
fi

STAMP="NOT-PUBLISHABLE"
[[ $PUBLISHABLE -eq 0 ]] && STAMP="publishable"

[[ "$WHAT" == "check" ]] && exit $PUBLISHABLE

# Core pinning, decided on PHYSICAL cores.
#
# `nproc` counts hyperthreads. On a 2-core/4-thread laptop it returns 4, and
# pinning the engine to core 1 and the handler to core 2 lands them on two
# siblings of the same physical core — which is worse than not pinning at all,
# and looks like it worked. The host gate already computed the real number, so
# it is read from there rather than recomputed differently here.
PHYSICAL="$(grep -oE '^host_physical_cores=[0-9]+' "$OUT/host.fields" 2>/dev/null | cut -d= -f2)"
PHYSICAL="${PHYSICAL:-0}"
if [[ "$PHYSICAL" -ge 4 ]] && command -v taskset >/dev/null 2>&1; then
    # Cores 2 and 3 rather than 0 and 1: core 0 takes most interrupt work on a
    # default Linux install, and leaving it free is worth more than the symmetry.
    PIN_ENGINE=(taskset -c 2)
    PIN_HANDLER=(taskset -c 3)
    echo "  pinning: engine -> core 2, handler -> core 3 (of $PHYSICAL physical)"
else
    PIN_ENGINE=()
    PIN_HANDLER=()
    echo "  not pinning: $PHYSICAL physical cores is too few for it to help"
fi

GOV="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
if [[ "$GOV" != "performance" && "$GOV" != "unknown" ]]; then
    echo "  governor is '$GOV'; trying to set performance (needs root)"
    for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        echo performance > "$g" 2>/dev/null || true
    done
    GOV="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
    echo "  governor is now '$GOV'"
fi

overall=0

# --------------------------------------------------------------------------
# Criterion microbenchmarks
# --------------------------------------------------------------------------
if [[ "$WHAT" == "micro" || "$WHAT" == "all" ]]; then
    echo
    echo "=============================================================="
    echo " microbenchmarks: decode, and the book update against its baseline"
    echo "=============================================================="
    # `--` separates cargo's arguments from Criterion's. Nothing is passed, but
    # the separator keeps a future flag from being eaten by cargo.
    "${PIN_HANDLER[@]}" cargo bench -p bench -- 2>&1 | tee "$OUT/micro.txt"
    [[ ${PIPESTATUS[0]} -ne 0 ]] && overall=1
fi

# --------------------------------------------------------------------------
# In-path latency
# --------------------------------------------------------------------------
if [[ "$WHAT" == "inpath" || "$WHAT" == "all" ]]; then
    echo
    echo "=============================================================="
    echo " in-path latency: rdtsc histogram over the real consume path"
    echo "=============================================================="

    H_SUM="$OUT/inpath.summary"
    rm -f "$H_SUM"
    MESSAGES="$INPATH_MESSAGES"

    "${PIN_HANDLER[@]}" "$HANDLER" \
        --config configs/local.toml \
        --transport unicast-fanout \
        --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002 \
        --messages "$MESSAGES" \
        --latency-histogram \
        --idle-timeout 15 \
        --summary-path "$H_SUM" \
        >"$OUT/inpath-handler.log" 2>&1 &
    HPID=$!
    sleep 1

    "${PIN_ENGINE[@]}" "$ENGINE" \
        --config configs/local.toml \
        --transport unicast-fanout \
        --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002 \
        --messages "$MESSAGES" \
        >"$OUT/inpath-engine.log" 2>&1
    wait $HPID || overall=1

    grep -E "latency|receiver-side|timing the consume" "$OUT/inpath-handler.log" | sed 's/^/  /'
fi

# --------------------------------------------------------------------------
# Sustained throughput, receiver-side, repeated
# --------------------------------------------------------------------------
if [[ "$WHAT" == "throughput" || "$WHAT" == "all" ]]; then
    echo
    echo "=============================================================="
    echo " throughput: ${DURATION}s sustained, receiver-side, ${RUNS} runs"
    echo "=============================================================="

    RATES=()
    for run in $(seq 1 "$RUNS"); do
        S="$OUT/throughput-$run.summary"
        rm -f "$S"
        # Unbounded message count; the engine's --duration ends the run, so the
        # handler measures whatever arrived in that window rather than racing to
        # a fixed count.
        "${PIN_HANDLER[@]}" "$HANDLER" \
            --config configs/local.toml \
            --transport unicast-fanout \
            --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002 \
            --idle-timeout 10 \
            --summary-path "$S" \
            >"$OUT/throughput-$run-handler.log" 2>&1 &
        HPID=$!
        sleep 1

        "${PIN_ENGINE[@]}" "$ENGINE" \
            --config configs/local.toml \
            --transport unicast-fanout \
            --feed-a 127.0.0.1:31001 --feed-b 127.0.0.1:31002 \
            --duration "$DURATION" \
            >"$OUT/throughput-$run-engine.log" 2>&1
        wait $HPID || true

        RATE="$(grep -oE 'receiver-side rate: [0-9]+' "$OUT/throughput-$run-handler.log" \
            | grep -oE '[0-9]+$' | tail -1)"
        RATE="${RATE:-0}"
        GAPS="$(grep -oE '^gaps=[0-9]+' "$S" 2>/dev/null | cut -d= -f2)"
        GAPS="${GAPS:-unknown}"
        echo "  run $run: $RATE msg/s, gaps=$GAPS"
        RATES+=("$RATE")
    done

    python3 - "$TOLERANCE" "${RATES[@]}" <<'PY' || overall=1
import sys

tolerance = float(sys.argv[1])
rates = [int(x) for x in sys.argv[2:]]
if not rates or min(rates) == 0:
    print("FAIL: at least one run produced no rate at all", file=sys.stderr)
    sys.exit(1)

lo, hi = min(rates), max(rates)
spread = (hi - lo) / lo * 100.0
print(f"  min {lo} / max {hi} msg/s, spread {spread:.1f}%")
if spread > tolerance:
    print(
        f"FAIL: the runs disagree by {spread:.1f}%, more than the {tolerance:.0f}% "
        "this project promises. A figure nobody can reproduce is worse for a "
        "portfolio than a more modest one that they can.",
        file=sys.stderr,
    )
    sys.exit(1)
PY
fi

# --------------------------------------------------------------------------
# What gets written
# --------------------------------------------------------------------------
echo
if [[ $PUBLISHABLE -ne 0 && $FORCE -eq 0 ]]; then
    {
        echo "# NOT A RESULT"
        echo
        echo "This run happened on a host that cannot produce a publishable number."
        echo "Nothing here may be quoted, and no CLAIMS.md row may cite it."
        echo
        cat "$OUT/hostcheck.txt"
    } > "$OUT/NOT-PUBLISHABLE.md"
    echo "  wrote $OUT/NOT-PUBLISHABLE.md — the numbers above describe this host,"
    echo "  not this code. bench/REPORT.md was not touched."
else
    echo "  host gate: $STAMP"
    echo "  raw output in $OUT. Fill in bench/REPORT.md from it by hand — the"
    echo "  methodology paragraphs are the part that makes the numbers mean"
    echo "  something, and they are not generated."
fi

exit $overall
