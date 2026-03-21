#!/usr/bin/env bash
set -euo pipefail

BASELINE_FILE=${BASELINE_FILE:-docs/performance/lockd-bench-query-disk-baseline.json}
INDEX_REGRESSION_PCT=${INDEX_REGRESSION_PCT:-10}
SCAN_REGRESSION_PCT=${SCAN_REGRESSION_PCT:-15}
OPS=${OPS:-1000}
CONCURRENCY=${CONCURRENCY:-8}
WARMUP=${WARMUP:-1}
RUNS=${RUNS:-3}
QUERY_RETURN=${QUERY_RETURN:-documents}
HA_MODE=${HA_MODE:-failover}
LOG_LEVEL=${LOG_LEVEL:-error}
FREEZE=0

if [[ "${1:-}" == "--freeze" ]]; then
  FREEZE=1
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--freeze]" >&2
  exit 1
fi

store=${LOCKD_STORE:-}
if [[ -z "$store" ]]; then
  echo "LOCKD_STORE must be set to a disk:///absolute/path URL" >&2
  exit 1
fi
if [[ "$store" != disk:///* ]]; then
  echo "LOCKD_STORE must reference a disk:///absolute/path URL, got: $store" >&2
  exit 1
fi

tmp_index=$(mktemp)
tmp_scan=$(mktemp)
trap 'rm -f "$tmp_index" "$tmp_scan"' EXIT

run_workload() {
  local workload=$1
  local outfile=$2
  go run ./cmd/lockd-bench \
    -backend disk \
    -ha "$HA_MODE" \
    -workload "$workload" \
    -ops "$OPS" \
    -concurrency "$CONCURRENCY" \
    -warmup "$WARMUP" \
    -runs "$RUNS" \
    -query-return "$QUERY_RETURN" \
    -log-level "$LOG_LEVEL" >"$outfile" 2>&1
}

extract_summary_ops() {
  local workload=$1
  local infile=$2
  awk -v workload="$workload" '
    $0 ~ ("bench summary: workload=" workload) { in_summary=1; next }
    in_summary && /^total: / {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^ops\/s=/) {
          sub(/^ops\/s=/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "$infile"
}

extract_disk_root() {
  local infile=$1
  awk -F': ' '/^bench disk root: / { print $2; exit }' "$infile"
}

run_workload query-index "$tmp_index"
run_workload query-scan "$tmp_scan"

index_ops=$(extract_summary_ops query-index "$tmp_index")
scan_ops=$(extract_summary_ops query-scan "$tmp_scan")
disk_root=$(extract_disk_root "$tmp_index")

if [[ -z "$index_ops" || -z "$scan_ops" ]]; then
  echo "failed to parse lockd-bench query summary output" >&2
  echo "--- query-index ---" >&2
  cat "$tmp_index" >&2
  echo "--- query-scan ---" >&2
  cat "$tmp_scan" >&2
  exit 1
fi

if [[ "$FREEZE" -eq 1 ]]; then
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  cat >"$BASELINE_FILE" <<EOF
{
  "timestamp": "$ts",
  "store": "$store",
  "disk_root": "$disk_root",
  "config": {
    "ha_mode": "$HA_MODE",
    "ops": $OPS,
    "concurrency": $CONCURRENCY,
    "warmup": $WARMUP,
    "runs": $RUNS,
    "query_return": "$QUERY_RETURN",
    "log_level": "$LOG_LEVEL"
  },
  "thresholds": {
    "query_index_regression_pct": $INDEX_REGRESSION_PCT,
    "query_scan_regression_pct": $SCAN_REGRESSION_PCT
  },
  "workloads": {
    "query-index": {
      "ops_per_sec": $index_ops
    },
    "query-scan": {
      "ops_per_sec": $scan_ops
    }
  }
}
EOF
  echo "disk query baseline frozen: $BASELINE_FILE"
  printf "disk root: %s\n" "$disk_root"
  printf "%-14s %12s\n" "Workload" "Ops/s"
  printf "%-14s %12s\n" "--------------" "------------"
  printf "%-14s %12s\n" "query-index" "$index_ops"
  printf "%-14s %12s\n" "query-scan" "$scan_ops"
  exit 0
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "baseline file not found: $BASELINE_FILE" >&2
  echo "freeze one with: make perf-freeze-query-disk-baseline FREEZE=1" >&2
  exit 1
fi

read_baseline() {
  local field=$1
  node -e "const fs=require('fs'); const b=JSON.parse(fs.readFileSync(process.argv[1], 'utf8')); console.log(${field});" "$BASELINE_FILE"
}

base_index=$(read_baseline "b.workloads['query-index'].ops_per_sec")
base_scan=$(read_baseline "b.workloads['query-scan'].ops_per_sec")

delta_pct() {
  awk -v base="$1" -v cur="$2" 'BEGIN { if (base == 0) print "0.00"; else printf "%.2f", ((cur-base)/base)*100 }'
}

index_delta=$(delta_pct "$base_index" "$index_ops")
scan_delta=$(delta_pct "$base_scan" "$scan_ops")

printf "Running disk query perf guard...\n"
printf "baseline: %s\n" "$BASELINE_FILE"
printf "store: %s\n" "$store"
printf "disk root: %s\n\n" "$disk_root"
printf "%-14s %12s %12s %9s\n" "Workload" "Base ops/s" "Cur ops/s" "Delta%"
printf "%-14s %12s %12s %9s\n" "--------------" "------------" "------------" "---------"
printf "%-14s %12s %12s %9s\n" "query-index" "$base_index" "$index_ops" "$index_delta"
printf "%-14s %12s %12s %9s\n" "query-scan" "$base_scan" "$scan_ops" "$scan_delta"

index_fail=$(awk -v d="$index_delta" -v t="$INDEX_REGRESSION_PCT" 'BEGIN { print (d < (0-t)) ? 1 : 0 }')
scan_fail=$(awk -v d="$scan_delta" -v t="$SCAN_REGRESSION_PCT" 'BEGIN { print (d < (0-t)) ? 1 : 0 }')

if [[ "$index_fail" -eq 1 || "$scan_fail" -eq 1 ]]; then
  echo
  echo "disk query perf guard FAILED: regression threshold exceeded"
  exit 1
fi

echo
echo "disk query perf guard PASSED"
