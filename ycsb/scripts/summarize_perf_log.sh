#!/usr/bin/env bash
set -euo pipefail

PERF_LOG=${PERF_LOG:-performance.log}
SERIES_FILTER=${SERIES_FILTER:-}
BASELINE_SERIES=${BASELINE_SERIES:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --perf-log)
      PERF_LOG=${2:-}
      shift 2
      ;;
    --series)
      SERIES_FILTER=${2:-}
      shift 2
      ;;
    --baseline-series)
      BASELINE_SERIES=${2:-}
      shift 2
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: summarize_perf_log.sh [--perf-log PATH] [--series NAME] [--baseline-series NAME]

Summarizes YCSB performance.log into a markdown table keyed by:
backend + phase + scenario + workload + query_engine + query_return.

When --series is provided, only matching entries are summarized as "current".
When --baseline-series is provided, matching entries are used for baseline TOTAL ops/s delta.
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

awk -v series_filter="$SERIES_FILTER" -v baseline_series="$BASELINE_SERIES" '
function trim(s) {
  gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s)
  return s
}

function clear_entry() {
  ts=""
  backend=""
  phase=""
  scenario=""
  series=""
  baseline_ref=""
  run_id=""
  workload=""
  query_engine=""
  query_return=""
  total_ops=""
  read_ops=""
  update_ops=""
  insert_ops=""
  scan_ops=""
  has_error_metrics=0
}

function parse_metadata(line,    n, parts, i, kv, k, v) {
  n = split(line, parts, " ")
  for (i = 1; i <= n; i++) {
    if (index(parts[i], "=") == 0) {
      continue
    }
    split(parts[i], kv, "=")
    k = kv[1]
    v = kv[2]
    if (k == "backend") backend = v
    else if (k == "phase") phase = v
    else if (k == "scenario") scenario = v
    else if (k == "series") series = v
    else if (k == "baseline_ref") baseline_ref = v
    else if (k == "run_id") run_id = v
    else if (k == "workload") workload = v
    else if (k == "query_engine") query_engine = v
    else if (k == "query_return") query_return = v
  }
}

function parse_ops(line, op_name,   out) {
  out = line
  sub(/^.*OPS: /, "", out)
  sub(/,.*$/, "", out)
  out = trim(out)
  if (op_name == "TOTAL") total_ops = out
  else if (op_name == "READ") read_ops = out
  else if (op_name == "UPDATE") update_ops = out
  else if (op_name == "INSERT") insert_ops = out
  else if (op_name == "SCAN") scan_ops = out
}

function to_num(s) {
  if (s ~ /^[0-9]+(\.[0-9]+)?$/) return s + 0
  return -1
}

function emit_entry(    key, cur_row, base_row, delta) {
  if (backend == "") {
    return
  }
  if (has_error_metrics == 1) {
    skipped_error_entries++
    return
  }
  if (total_ops == "") {
    total_sum = 0
    has_component = 0
    if (read_ops != "") {
      n = to_num(read_ops)
      if (n >= 0) {
        total_sum += n
        has_component = 1
      }
    }
    if (update_ops != "") {
      n = to_num(update_ops)
      if (n >= 0) {
        total_sum += n
        has_component = 1
      }
    }
    if (insert_ops != "") {
      n = to_num(insert_ops)
      if (n >= 0) {
        total_sum += n
        has_component = 1
      }
    }
    if (scan_ops != "") {
      n = to_num(scan_ops)
      if (n >= 0) {
        total_sum += n
        has_component = 1
      }
    }
    if (has_component) {
      total_ops = sprintf("%.1f", total_sum)
    }
  }
  if (scenario == "") {
    scenario = workload
  }
  key = backend "|" phase "|" scenario "|" workload "|" query_engine "|" query_return
  if (series_filter == "" || series == series_filter) {
    current_rows[key] = ts "|" series "|" baseline_ref "|" run_id "|" total_ops "|" read_ops "|" update_ops "|" insert_ops "|" scan_ops
    keys[key] = 1
  }
  if (baseline_series != "" && series == baseline_series) {
    baseline_rows[key] = total_ops
  }
}

BEGIN {
  clear_entry()
  skipped_error_entries = 0
}

/^## / {
  emit_entry()
  clear_entry()
  ts = substr($0, 4)
  next
}

/^backend=/ {
  parse_metadata($0)
  next
}

/^[A-Z_]+[[:space:]]+- Takes\(s\):/ {
  op = $1
  if (op ~ /_ERROR$/) {
    has_error_metrics = 1
    next
  }
  parse_ops($0, op)
  next
}

/^$/ {
  emit_entry()
  clear_entry()
  next
}

END {
  emit_entry()

  print "# YCSB Perf Summary"
  print ""
  print "- Perf log: `" PERF_LOG "`"
  if (series_filter != "") {
    print "- Series: `" series_filter "`"
  } else {
    print "- Series: `(all)`"
  }
  if (baseline_series != "") {
    print "- Baseline series: `" baseline_series "`"
  } else {
    print "- Baseline series: `(none)`"
  }
  if (skipped_error_entries > 0) {
    print "- Skipped entries with error metrics: `" skipped_error_entries "`"
  }
  print ""
  print "| Backend | Phase | Scenario | Workload | Query Engine | Query Return | Timestamp | Run ID | TOTAL ops/s | READ ops/s | UPDATE ops/s | INSERT ops/s | SCAN ops/s | Baseline TOTAL | Delta vs Baseline |"
  print "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"

  count = asorti(keys, sorted)
  for (i = 1; i <= count; i++) {
    key = sorted[i]
    split(key, dims, "|")
    backend = dims[1]
    phase = dims[2]
    scenario = dims[3]
    workload = dims[4]
    query_engine = dims[5]
    query_return = dims[6]

    split(current_rows[key], cur, "|")
    ts = cur[1]
    run_id = cur[4]
    total = cur[5]
    read = cur[6]
    update = cur[7]
    insert = cur[8]
    scan = cur[9]

    baseline_total = "n/a"
    delta = "n/a"
    if (baseline_series != "" && (key in baseline_rows)) {
      baseline_total = baseline_rows[key]
      cur_num = to_num(total)
      base_num = to_num(baseline_total)
      if (cur_num >= 0 && base_num > 0) {
        delta = sprintf("%.2f%%", ((cur_num - base_num) / base_num) * 100)
      }
    }

    if (query_engine == "") query_engine = "n/a"
    if (query_return == "") query_return = "n/a"
    if (run_id == "") run_id = "n/a"
    if (read == "") read = "n/a"
    if (update == "") update = "n/a"
    if (insert == "") insert = "n/a"
    if (scan == "") scan = "n/a"
    if (total == "") total = "n/a"

    printf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
      backend, phase, scenario, workload, query_engine, query_return,
      ts, run_id, total, read, update, insert, scan, baseline_total, delta)
  }
}
' PERF_LOG="$PERF_LOG" "$PERF_LOG"
