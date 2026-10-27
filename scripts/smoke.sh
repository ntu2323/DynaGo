#!/usr/bin/env bash
# Local 3-replica smoke test over UDP (localhost).
#
#   scripts/smoke.sh                       # generous deadline: mostly fast path
#   DYNA_INITIAL_BOUND_US=50 DYNA_MAX_OWD_US=100 scripts/smoke.sh   # tight: slow path
#
#   SMOKE_KILL_REPLICA=2 scripts/smoke.sh                 # kill -9 r2 1.5 s in: needs view 1
#   SMOKE_KILL_REPLICA=2 SMOKE_RESTART_AFTER_MS=2000 scripts/smoke.sh   # + restart: view 2
#
# Env: SMOKE_DURATION (s, default 5), SMOKE_THREADS (default 4), SMOKE_RATE (open-loop
# ops/s, default 0 = closed loop), SMOKE_PROFILE=1 (CPU profiles cpu<i>.prof in SMOKE_DIR), SMOKE_READS (read fraction), SMOKE_PORT (default 23000), SMOKE_DIR (logs).
set -euo pipefail
cd "$(dirname "$0")/.."

DUR=${SMOKE_DURATION:-5}
THREADS=${SMOKE_THREADS:-4}
RATE=${SMOKE_RATE:-0}
PORT=${SMOKE_PORT:-23000}
DIR=${SMOKE_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/dyna-go-smoke.XXXXXX")}
BIN=${SMOKE_BIN:-bin}   # prebuilt binaries (docker-smoke.sh); default: build into bin/
export DYNA_BUSY_POLL=${DYNA_BUSY_POLL:-0}
export DYNA_INITIAL_BOUND_US=${DYNA_INITIAL_BOUND_US:-2000}

[ -n "${SMOKE_BIN:-}" ] || go build -o bin/ ./cmd/...
mkdir -p "$DIR"
CFG="$DIR/config"
printf 'f 1\nreplica 127.0.0.1:%d\nreplica 127.0.0.1:%d\nreplica 127.0.0.1:%d\n' \
    "$PORT" "$((PORT + 1))" "$((PORT + 2))" > "$CFG"

KILL=${SMOKE_KILL_REPLICA:-}
RESTART_MS=${SMOKE_RESTART_AFTER_MS:-}
[ -n "$KILL" ] && [ -z "${SMOKE_DURATION:-}" ] && DUR=8

pids=()
cleanup() { for p in "${pids[@]:-}"; do [ -n "$p" ] || continue; kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
start_replica() {   # $1 = index; appends to its log (a restart keeps the history)
    local prof=()
    [ -n "${SMOKE_PROFILE:-}" ] && prof=(-cpuprofile "$DIR/cpu$1.prof" -memprofile "$DIR/mem$1.prof")
    (cd "$DIR" && exec "$OLDPWD/$BIN/dyna-replica" -c "$CFG" -i "$1" ${prof[@]+"${prof[@]}"}) >> "$DIR/replica$1.log" 2>&1 &
    pids[$1]=$!
}
for i in 0 1 2; do start_replica "$i"; done
sleep 0.5

if [ -n "$KILL" ]; then
    (
        sleep 2.5   # 1 s warmup + 1.5 s
        echo "smoke: kill -9 replica $KILL"
        kill -9 "${pids[$KILL]}"
        if [ -n "$RESTART_MS" ]; then
            sleep "$(awk "BEGIN{print $RESTART_MS/1000}")"
            echo "smoke: restarting replica $KILL"
            start_replica "$KILL"
            echo "${pids[$KILL]}" > "$DIR/restarted.pid"
        fi
    ) &
    killer=$!
fi

cprof=()
[ -n "${SMOKE_PROFILE:-}" ] && cprof=(-cpuprofile "$DIR/cpu-client.prof")
"./$BIN/dyna-client" ${cprof[@]+"${cprof[@]}"} -c "$CFG" -j "$THREADS" -e "$DUR" -w 1 -rate "$RATE" -reads "${SMOKE_READS:-0}" | tee "$DIR/client.log"
if [ -n "$KILL" ]; then
    wait "$killer" || true
    [ -f "$DIR/restarted.pid" ] && pids[$KILL]=$(cat "$DIR/restarted.pid")
fi

sleep 0.5   # let in-flight commits replicate and execute
for i in 0 1 2; do
    [ "$i" = "$KILL" ] && [ -z "$RESTART_MS" ] && continue
    kill -0 "${pids[$i]}" 2>/dev/null || { echo "FAIL: replica $i died"; cat "$DIR/replica$i.log"; exit 1; }
done
if [ -n "$KILL" ]; then
    want=1; [ -n "$RESTART_MS" ] && want=2
    for i in 0 1 2; do
        [ "$i" = "$KILL" ] && [ -z "$RESTART_MS" ] && continue
        grep -q "view $want installed" "$DIR/replica$i.log" ||
            { echo "FAIL: replica $i never logged 'view $want installed' (logs: $DIR)"; exit 1; }
    done
    echo "smoke: every live replica installed view $want"
fi
cleanup; pids=()
for i in 0 1 2; do grep "final:" "$DIR/replica$i.log" || true; done
n=$(sed -n 's/^Completed \([0-9]*\) requests.*/\1/p' "$DIR/client.log")
if [ -z "$n" ] || [ "$n" -eq 0 ]; then echo "FAIL: no requests completed (logs: $DIR)"; exit 1; fi
echo "PASS: $n requests completed (logs: $DIR)"
