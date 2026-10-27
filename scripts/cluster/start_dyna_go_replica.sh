#!/usr/bin/env bash
# Start the Go DYNA replica on a cluster node (deployed to ~/bench/ by
# Ionia/protocols/experiments/GCP_tests/setup_dyna_go_cluster.py; launched by
# run_cluster_bench.py --protocol dyna-go). Mirrors start_dyna_replica.sh: same config,
# env defaults, run dir and log (~/bench/replica-<idx>.log). The binary lives at
# ~/dyna_go/bench/replica, so the bench scripts' pkill '[b]ench/replica' patterns match it.
set -euo pipefail
IDX="${1:?replica index 0|1|2}"
# Store: IONIA_DB_MODE=memory → in-memory; anything else (the C++ default "splinter") →
# Pebble on disk, in the run dir (wiped at startup: not trusted across a restart).
export IONIA_DB_MODE="${IONIA_DB_MODE:-splinter}"
export DYNA_DB_CACHE_MB="${DYNA_DB_CACHE_MB:-256}"
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-100}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-1000}"
export DYNA_OWD_PERCENTILE="${DYNA_OWD_PERCENTILE:-95}"
export DYNA_OWD_WINDOW_LEN="${DYNA_OWD_WINDOW_LEN:-100}"
export DYNA_BUSY_POLL="${DYNA_BUSY_POLL:-1}"
export DYNA_TS_SCAN_MS="${DYNA_TS_SCAN_MS:-50}"
export DYNA_TS_TIMEOUT_MS="${DYNA_TS_TIMEOUT_MS:-100}"
# Executed-set / reply-cache pre-size (never GC'd). 2M covers a ~40 s run at 50k ops/s
# without a resize; much larger tables cost cache/TLB misses on every insert.
export DYNA_PRERESERVE="${DYNA_PRERESERVE:-2000000}"
# CPU placement: Go can't pin individual threads, so confine the whole process to the
# NIC's NUMA node (physical cores first, skipping the first core, then hyperthread
# siblings) and size GOMAXPROCS to it. DYNA_PIN=0 disables.
TASKSET=()
if [[ "${DYNA_PIN:-1}" != "0" ]] && command -v lscpu >/dev/null && command -v taskset >/dev/null; then
  IFACE=$( (ip -4 -o addr show 2>/dev/null || true) | awk '$4 ~ /^10[.]10[.]/ {print $2; exit}')
  NODE=$(cat "/sys/class/net/${IFACE}/device/numa_node" 2>/dev/null || echo 0)
  [[ "${NODE}" -lt 0 ]] && NODE=0
  mapfile -t PHYS < <(lscpu -p=CPU,CORE,NODE | grep -v '^#' \
                         | awk -F, -v n="${NODE}" '$3==n && !seen[$2]++ {print $1}' | tail -n +2)
  mapfile -t SIBS < <(lscpu -p=CPU,CORE,NODE | grep -v '^#' \
                         | awk -F, -v n="${NODE}" '$3==n && seen[$2]++ {print $1}')
  CORES=("${PHYS[@]}" "${SIBS[@]}")
  if (( ${#CORES[@]} > 0 )); then
    LIST=$(IFS=,; echo "${CORES[*]}")
    TASKSET=(taskset -c "${LIST}")
    export GOMAXPROCS="${GOMAXPROCS:-${#CORES[@]}}"
  fi
fi
RUN_DIR="${HOME}/dyna_go/run"
if [[ -d /mydata ]]; then RUN_DIR=/mydata/dyna; fi
mkdir -p "${RUN_DIR}"
export DYNA_DB_DIR="${DYNA_DB_DIR:-${RUN_DIR}}"
cd "${RUN_DIR}"
pkill -f '[b]ench/replica' 2>/dev/null || true
sleep 1
nohup ${TASKSET[@]+"${TASKSET[@]}"} "${HOME}/dyna_go/bench/replica" -i "${IDX}" -c ~/bench/dynaconfig -m dyna \
  > ~/bench/replica-"${IDX}".log 2>&1 &
echo "Started DYNA-go replica index=${IDX} pid=$! cpus=${LIST:-all} GOMAXPROCS=${GOMAXPROCS:-default} run_dir=${RUN_DIR}"
