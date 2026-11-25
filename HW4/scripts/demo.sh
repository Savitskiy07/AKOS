#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
make
port=${1:-5050}
logs=${GALLERY_LOG_DIR:-"$root/logs/demo"}
mkdir -p "$logs"
pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT TERM
./build/gallery_server --host 127.0.0.1 --port "$port" --visitors 100 \
    --capacity 10 --pictures 5 --picture-capacities 2,3,2,1,2 \
    --view-min-ms 50 --view-max-ms 100 --strategy random --seed 16 \
    >"$logs/server.log" 2>"$logs/server.err" &
server=$!; pids+=("$server")
ready=0
for ((attempt=0; attempt<100; ++attempt)); do
    if grep -q '^READY ' "$logs/server.log"; then ready=1; break; fi
    if ! kill -0 "$server" 2>/dev/null; then cat "$logs/server.err" >&2; exit 1; fi
    sleep 0.05
done
if (( ready == 0 )); then echo 'Server did not become ready' >&2; exit 1; fi
./build/gallery_observer --host 127.0.0.1 --port "$port" >"$logs/observer-1.log" 2>&1 &
observer1=$!; pids+=("$observer1")
./build/gallery_observer --host 127.0.0.1 --port "$port" >"$logs/observer-2.log" 2>&1 &
observer2=$!; pids+=("$observer2")
GALLERY_LOG_DIR="$logs/visitors" bash scripts/visitors.sh 127.0.0.1 "$port" 100 5 15 16 &
visitors=$!; pids+=("$visitors")
sleep 0.5
kill "$observer2" 2>/dev/null || true
wait "$observer2" 2>/dev/null || true
./build/gallery_observer --host 127.0.0.1 --port "$port" >"$logs/observer-2-reconnected.log" 2>&1 &
observer3=$!; pids+=("$observer3")
wait "$visitors"
wait "$server"
wait "$observer1"
wait "$observer3"
echo "Demo completed. Logs: $logs"
grep '^SUMMARY ' "$logs/server.log"
