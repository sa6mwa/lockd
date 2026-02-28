#!/usr/bin/env bash
set -euo pipefail

PERF_LOG=${PERF_LOG:-performance.log}
BENCHMARKS_FILE=${BENCHMARKS_FILE:-BENCHMARKS.md}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --perf-log)
      PERF_LOG=${2:-}
      shift 2
      ;;
    --benchmarks)
      BENCHMARKS_FILE=${2:-}
      shift 2
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: compare_query_perf.sh [--perf-log PATH] [--benchmarks PATH]

Parses lockd run entries tagged with query_engine in ycsb performance.log and
prints index vs scan comparison, including previous-run deltas and historical
lockd-bench reference numbers from BENCHMARKS.md.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$PERF_LOG" ]]; then
  echo "perf log not found: $PERF_LOG" >&2
  exit 1
fi
if [[ ! -f "$BENCHMARKS_FILE" ]]; then
  echo "benchmarks file not found: $BENCHMARKS_FILE" >&2
  exit 1
fi

entries_tmp=$(mktemp)
trap 'rm -f "$entries_tmp"' EXIT

awk '
  function parse_ops(line,   out) {
    out = line
    sub(/^.*OPS: /, "", out)
    sub(/,.*$/, "", out)
    return out
  }
  function emit() {
    if (capture && has_error == 0 && total_ops == "" && component_ops_sum > 0) {
      total_ops = sprintf("%.1f", component_ops_sum)
    }
    if (capture && has_error == 0 && engine != "" && scan_ops != "") {
      printf "%s|%s|%s|%s\n", ts, engine, total_ops, scan_ops
    } else if (capture && has_error == 1) {
      skipped_error++
    }
  }
  BEGIN {
    ts = ""
    capture = 0
    engine = ""
    total_ops = ""
    scan_ops = ""
    component_ops_sum = 0
    has_error = 0
    skipped_error = 0
  }
  /^## / {
    emit()
    ts = substr($0, 4)
    capture = 0
    engine = ""
    total_ops = ""
    scan_ops = ""
    component_ops_sum = 0
    has_error = 0
    next
  }
  /^backend=/ {
    capture = 0
    engine = ""
    total_ops = ""
    scan_ops = ""
    component_ops_sum = 0
    has_error = 0
    if ($0 ~ /backend=lockd/ && $0 ~ /phase=run/ && $0 ~ /query_engine=/) {
      if (match($0, /query_engine=[^ ]+/)) {
        engine = substr($0, RSTART + length("query_engine="), RLENGTH - length("query_engine="))
        capture = 1
      }
    }
    next
  }
  capture && /^[A-Z_]+[[:space:]]+- Takes\(s\):/ {
    op = $1
    if (op ~ /_ERROR$/) {
      has_error = 1
      next
    }
    ops = parse_ops($0)
    if (op == "TOTAL") {
      total_ops = ops
    } else {
      component_ops_sum += ops + 0
      if (op == "SCAN") {
        scan_ops = ops
      }
    }
    next
  }
  /^$/ {
    emit()
    capture = 0
    engine = ""
    total_ops = ""
    scan_ops = ""
    component_ops_sum = 0
    has_error = 0
    next
  }
  END {
    emit()
    if (skipped_error > 0) {
      print "skipped_error_entries=" skipped_error > "/dev/stderr"
    }
  }
' "$PERF_LOG" > "$entries_tmp"

if [[ ! -s "$entries_tmp" ]]; then
  echo "no lockd run entries with query_engine found in $PERF_LOG" >&2
  echo "hint: run ycsb query targets first (lockd-run-query-index + lockd-run-query-scan)" >&2
  exit 1
fi

last_for_engine() {
  local engine=$1
  awk -F'|' -v e="$engine" '$2 == e { line = $0 } END { print line }' "$entries_tmp"
}

prev_for_engine() {
  local engine=$1
  awk -F'|' -v e="$engine" '
    $2 == e {
      rows[++n] = $0
    }
    END {
      if (n >= 2) {
        print rows[n-1]
      }
    }
  ' "$entries_tmp"
}

field_or_na() {
  local line=$1
  local idx=$2
  if [[ -z "$line" ]]; then
    echo "n/a"
    return
  fi
  awk -F'|' -v i="$idx" '{ if (NF >= i && $i != "") print $i; else print "n/a" }' <<< "$line"
}

delta_pct() {
  local current=$1
  local previous=$2
  if [[ "$current" == "n/a" || "$previous" == "n/a" ]]; then
    echo "n/a"
    return
  fi
  awk -v c="$current" -v p="$previous" 'BEGIN {
    if (p == 0) {
      print "n/a"
    } else {
      printf "%.2f", ((c - p) / p) * 100
    }
  }'
}

bench_ref_for_engine() {
  local engine=$1
  local bench_key=""
  case "$engine" in
    index) bench_key="query-index" ;;
    scan) bench_key="query-scan" ;;
    *) bench_key="$engine" ;;
  esac
  sed -n "s/^- \*\*${bench_key}\*\*.*: \([0-9.]*\) ops\/s.*/\1/p" "$BENCHMARKS_FILE" | head -n 1
}

print_engine_row() {
  local engine=$1
  local current_line previous_line current_ts current_total current_scan prev_scan delta_prev ref delta_ref

  current_line=$(last_for_engine "$engine")
  previous_line=$(prev_for_engine "$engine")
  current_ts=$(field_or_na "$current_line" 1)
  current_total=$(field_or_na "$current_line" 3)
  current_scan=$(field_or_na "$current_line" 4)
  prev_scan=$(field_or_na "$previous_line" 4)
  delta_prev=$(delta_pct "$current_scan" "$prev_scan")

  ref=$(bench_ref_for_engine "$engine")
  if [[ -z "$ref" ]]; then
    ref="n/a"
  fi
  delta_ref=$(delta_pct "$current_scan" "$ref")

  if [[ "$delta_prev" != "n/a" ]]; then
    delta_prev="${delta_prev}%"
  fi
  if [[ "$delta_ref" != "n/a" ]]; then
    delta_ref="${delta_ref}%"
  fi

  printf "| %-10s | %-20s | %12s | %12s | %12s | %10s | %12s | %10s |\n" \
    "$engine" "$current_ts" "$current_total" "$current_scan" "$prev_scan" "$delta_prev" "$ref" "$delta_ref"
}

echo "Query engine comparison (YCSB)"
printf "| %-10s | %-20s | %-12s | %-12s | %-12s | %-10s | %-12s | %-10s |\n" \
  "Engine" "Latest ts" "TOTAL ops/s" "SCAN ops/s" "Prev SCAN" "Delta" "BENCHMARKS" "Delta"
printf "|-%-10s-|-%-20s-|-%-12s-|-%-12s-|-%-12s-|-%-10s-|-%-12s-|-%-10s-|\n" \
  "----------" "--------------------" "------------" "------------" "------------" "----------" "------------" "----------"
print_engine_row "index"
print_engine_row "scan"

echo
echo "notes:"
echo "- BENCHMARKS reference is lockd-bench query workload ops/s (not YCSB SCAN ops/s)."
echo "- Prev SCAN and Delta are computed from earlier entries in $PERF_LOG with matching query_engine metadata."
