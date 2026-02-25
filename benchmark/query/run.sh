#!/usr/bin/env bash
set -euo pipefail

BACKENDS_CSV=${QUERY_BENCH_BACKENDS:-mem,disk}
OPS=${QUERY_BENCH_OPS:-2000}
CONCURRENCY=${QUERY_BENCH_CONCURRENCY:-8}
WARMUP=${QUERY_BENCH_WARMUP:-0}
RUNS=${QUERY_BENCH_RUNS:-1}
QUERY_LIMIT=${QUERY_BENCH_QUERY_LIMIT:-50}
QUERY_RETURN=${QUERY_BENCH_QUERY_RETURN:-documents}
LOG_LEVEL=${QUERY_BENCH_LOG_LEVEL:-error}
LOG_DIR=${QUERY_BENCH_LOG_DIR:-benchmark-logs/query}
BASELINE_CSV=${QUERY_BENCH_BASELINE_CSV:-docs/performance/2026-01-24-lockd-bench-baseline-rerun/summary.csv}

mkdir -p "$LOG_DIR"

if [[ ! -f "$BASELINE_CSV" ]]; then
  echo "query benchmark baseline CSV not found: $BASELINE_CSV" >&2
  exit 1
fi

tmp_summary=$(mktemp)
trap 'rm -f "$tmp_summary"' EXIT

IFS=',' read -r -a backend_list <<< "$BACKENDS_CSV"
workloads=(query-index query-scan)

echo "Running query benchmark matrix"
echo "backends=$BACKENDS_CSV ops=$OPS concurrency=$CONCURRENCY runs=$RUNS warmup=$WARMUP limit=$QUERY_LIMIT return=$QUERY_RETURN"
echo "baseline_csv=$BASELINE_CSV"
echo

for backend_raw in "${backend_list[@]}"; do
  backend=$(echo "$backend_raw" | xargs)
  if [[ -z "$backend" ]]; then
    continue
  fi

  for workload in "${workloads[@]}"; do
    log_file="$LOG_DIR/${backend}-${workload}.log"
    cmd=(
      go run ./cmd/lockd-bench
      -backend "$backend"
      -workload "$workload"
      -ops "$OPS"
      -concurrency "$CONCURRENCY"
      -warmup "$WARMUP"
      -runs "$RUNS"
      -query-limit "$QUERY_LIMIT"
      -query-return "$QUERY_RETURN"
      -log-level "$LOG_LEVEL"
    )
    if [[ "$backend" == "disk" ]]; then
      cmd+=( -ha failover )
    fi

    echo "==> backend=$backend workload=$workload"
    echo "Command: ${cmd[*]}"
    if ! "${cmd[@]}" 2>&1 | tee "$log_file"; then
      echo "query benchmark failed for backend=$backend workload=$workload" >&2
      exit 1
    fi

    total_line=$(awk '/^total: / { line=$0 } END { print line }' "$log_file")
    query_line=$(awk '/^query: / { line=$0 } END { print line }' "$log_file")
    total_ops=$(sed -n 's/.*ops\/s=\([0-9.]*\).*/\1/p' <<< "$total_line")
    query_ops=$(sed -n 's/.*ops\/s=\([0-9.]*\).*/\1/p' <<< "$query_line")
    total_avg=$(sed -n 's/.*avg=\([^ ]*\).*/\1/p' <<< "$total_line")
    query_avg=$(sed -n 's/.*avg=\([^ ]*\).*/\1/p' <<< "$query_line")

    if [[ -z "$query_ops" ]]; then
      echo "failed to parse query ops/s from $log_file" >&2
      exit 1
    fi

    baseline_ops=$(awk -F',' -v backend="$backend" -v workload="$workload" '
      $1 == backend && $2 == workload {
        print $5
        exit
      }
    ' "$BASELINE_CSV" | tr -d '\r')

    delta_vs_baseline="n/a"
    if [[ -n "$baseline_ops" ]]; then
      delta_vs_baseline=$(awk -v cur="$query_ops" -v base="$baseline_ops" 'BEGIN {
        if (base == 0) {
          print "n/a"
        } else {
          printf "%.2f", ((cur - base) / base) * 100
        }
      }')
    fi

    printf "%s|%s|%s|%s|%s|%s|%s\n" \
      "$backend" "$workload" "$total_ops" "$query_ops" "$total_avg" "$query_avg" "$baseline_ops" >> "$tmp_summary"

    if [[ "$delta_vs_baseline" == "n/a" ]]; then
      echo "result: query_ops/s=$query_ops total_avg=$total_avg query_avg=$query_avg baseline=n/a"
    else
      echo "result: query_ops/s=$query_ops total_avg=$total_avg query_avg=$query_avg baseline=$baseline_ops delta=${delta_vs_baseline}%"
    fi
    echo "log: $log_file"
    echo
  done
done

echo "Query benchmark summary"
printf "| %-8s | %-11s | %-11s | %-11s | %-12s | %-12s | %-12s | %-10s |\n" \
  "Backend" "Workload" "Total ops/s" "Query ops/s" "Total avg" "Query avg" "Baseline" "Delta %"
printf "|-%-8s-|-%-11s-|-%-11s-|-%-11s-|-%-12s-|-%-12s-|-%-12s-|-%-10s-|\n" \
  "--------" "-----------" "-----------" "-----------" "------------" "------------" "------------" "----------"

while IFS='|' read -r backend workload total_ops query_ops total_avg query_avg baseline_ops; do
  delta="n/a"
  if [[ -n "$baseline_ops" ]]; then
    delta=$(awk -v cur="$query_ops" -v base="$baseline_ops" 'BEGIN {
      if (base == 0) {
        print "n/a"
      } else {
        printf "%.2f", ((cur - base) / base) * 100
      }
    }')
  fi
  printf "| %-8s | %-11s | %11s | %11s | %-12s | %-12s | %12s | %10s |\n" \
    "$backend" "$workload" "$total_ops" "$query_ops" "$total_avg" "$query_avg" "${baseline_ops:-n/a}" "$delta"
done < "$tmp_summary"

echo
echo "Done. Logs are in $LOG_DIR"
