#!/usr/bin/env bash
# Launch one independent C process per visitor, with random inter-arrival times.
set -euo pipefail
if (( $# < 3 || $# > 6 )); then
    echo "Usage: $0 HOST PORT COUNT [ARRIVAL_MIN_MS=10] [ARRIVAL_MAX_MS=50] [SEED=1]" >&2
    exit 1
fi
host=$1 port=$2 count=$3 min=${4:-10} max=${5:-50} seed=${6:-1}
for value in "$port" "$count" "$min" "$max" "$seed"; do
    if [[ ! $value =~ ^[0-9]{1,7}$ ]]; then echo 'Invalid numeric argument' >&2; exit 1; fi
done
port=$((10#$port)) count=$((10#$count)) min=$((10#$min)) max=$((10#$max)) seed=$((10#$seed))
if (( port < 1 || port > 65535 || count < 0 || count > 1000 || min > max || max > 3600 || seed > 32767 )); then
    echo 'Require port 1..65535, count 0..1000, 0 <= min <= max <= 3600, seed 0..32767' >&2
    exit 1
fi
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
binary=${GALLERY_BIN:-"$root/build"}/gallery_visitor
logs=${GALLERY_LOG_DIR:-"$root/logs/visitors"}
mkdir -p "$logs"
pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap 'cleanup; exit 130' INT TERM
RANDOM=$seed
delay=0
for ((id=1; id<=count; ++id)); do
    if (( id > 1 )); then delay=$((delay + min + RANDOM % (max - min + 1))); fi
    "$binary" --host "$host" --port "$port" --id "$id" --arrival-ms "$delay" >"$logs/visitor-$id.log" 2>&1 &
    pids+=("$!")
done
status=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then status=1; fi
done
exit "$status"
