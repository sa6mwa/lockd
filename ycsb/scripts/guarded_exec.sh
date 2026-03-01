#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: guarded_exec.sh -- <command...>

Runs a command with host-safety guardrails:
- nice/ionice priority demotion
- optional per-process virtual memory cap
- watchdog that aborts on low host memory / high swap usage
- optional wall-clock timeout

Environment variables:
  YCSB_GUARD_ENABLE=1                Enable guardrails (default 1)
  YCSB_GUARD_NICE=10                 nice level for workload process
  YCSB_GUARD_IONICE_CLASS=3          ionice class (3=idle)
  YCSB_GUARD_MIN_MEM_AVAILABLE_KB=2097152   Abort if MemAvailable drops below this
  YCSB_GUARD_MAX_SWAP_USED_KB=1048576       Abort if swap usage exceeds this
  YCSB_GUARD_POLL_SECS=2             Watchdog poll interval
  YCSB_GUARD_TIMEOUT_SECS=0          0 disables timeout
  YCSB_GUARD_PROCESS_VMEM_KB=0       0 disables process virtual memory cap (ulimit -v)
USAGE
}

if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
  usage
  exit 0
fi

if [[ ${1:-} != "--" ]]; then
  echo "guarded_exec.sh: missing -- before command" >&2
  usage >&2
  exit 2
fi
shift

if [[ $# -eq 0 ]]; then
  echo "guarded_exec.sh: missing command" >&2
  usage >&2
  exit 2
fi

guard_enable=${YCSB_GUARD_ENABLE:-1}
if [[ "$guard_enable" == "0" ]]; then
  exec "$@"
fi

nice_level=${YCSB_GUARD_NICE:-10}
ionice_class=${YCSB_GUARD_IONICE_CLASS:-3}
min_mem_kb=${YCSB_GUARD_MIN_MEM_AVAILABLE_KB:-2097152}
max_swap_used_kb=${YCSB_GUARD_MAX_SWAP_USED_KB:-1048576}
poll_secs=${YCSB_GUARD_POLL_SECS:-2}
timeout_secs=${YCSB_GUARD_TIMEOUT_SECS:-0}
process_vmem_kb=${YCSB_GUARD_PROCESS_VMEM_KB:-0}

if ! [[ "$nice_level" =~ ^-?[0-9]+$ ]]; then
  echo "guarded_exec.sh: invalid YCSB_GUARD_NICE=$nice_level" >&2
  exit 2
fi
for numeric_var in min_mem_kb max_swap_used_kb poll_secs timeout_secs process_vmem_kb ionice_class; do
  value=${!numeric_var}
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "guarded_exec.sh: invalid value for $numeric_var=$value" >&2
    exit 2
  fi
done

echo "guarded_exec: enabled"
echo "  nice=$nice_level ionice_class=$ionice_class"
echo "  min_mem_available_kb=$min_mem_kb max_swap_used_kb=$max_swap_used_kb"
echo "  poll_secs=$poll_secs timeout_secs=$timeout_secs process_vmem_kb=$process_vmem_kb"

start_epoch=$(date +%s)

(
  if [[ "$process_vmem_kb" -gt 0 ]]; then
    ulimit -v "$process_vmem_kb"
  fi
  if command -v ionice >/dev/null 2>&1; then
    exec ionice -c "$ionice_class" nice -n "$nice_level" "$@"
  fi
  exec nice -n "$nice_level" "$@"
) &
child_pid=$!

abort_reason=""
while kill -0 "$child_pid" >/dev/null 2>&1; do
  mem_available_kb=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
  swap_total_kb=$(awk '/SwapTotal:/ {print $2}' /proc/meminfo)
  swap_free_kb=$(awk '/SwapFree:/ {print $2}' /proc/meminfo)
  if [[ -z "$mem_available_kb" || -z "$swap_total_kb" || -z "$swap_free_kb" ]]; then
    sleep "$poll_secs"
    continue
  fi
  swap_used_kb=$((swap_total_kb - swap_free_kb))

  if (( mem_available_kb < min_mem_kb )); then
    abort_reason="mem_available_kb=${mem_available_kb} below threshold=${min_mem_kb}"
    break
  fi
  if (( swap_used_kb > max_swap_used_kb )); then
    abort_reason="swap_used_kb=${swap_used_kb} above threshold=${max_swap_used_kb}"
    break
  fi
  if (( timeout_secs > 0 )); then
    now_epoch=$(date +%s)
    elapsed=$((now_epoch - start_epoch))
    if (( elapsed > timeout_secs )); then
      abort_reason="timeout_secs=${timeout_secs} exceeded"
      break
    fi
  fi
  sleep "$poll_secs"
done

if [[ -n "$abort_reason" ]]; then
  echo "guarded_exec: ABORTING child pid=$child_pid reason=$abort_reason" >&2
  kill -TERM "$child_pid" >/dev/null 2>&1 || true
  sleep 3
  kill -KILL "$child_pid" >/dev/null 2>&1 || true
  wait "$child_pid" >/dev/null 2>&1 || true
  exit 124
fi

wait "$child_pid"
