#!/usr/bin/env bash
# Run selected benchmark suites under ./benchmark.
set -uo pipefail

crypto_enabled=1
mtls_enabled=1
benchtime=${BENCHTIME:-1x}
prefetch_double=8
isolate_mem=0

print_usage() {
  cat <<'USAGE'
Usage: run-benchmark-suites.sh [--disable-crypto] [--disable-mtls] [--prefetch-double N] [--benchtime DUR] [suite ...]

Options:
  --disable-crypto   Run benchmarks with LOCKD_TEST_STORAGE_ENCRYPTION=0 (default 1).
  --disable-mtls     Run benchmarks with LOCKD_TEST_WITH_MTLS=0 (default 1).
  --isolate-mem      Run mem benchmarks one scenario per process.
  --prefetch-double N Set MEM_LQ_BENCH_PREFETCH_DOUBLE for multi-server mem benches (default 8).
  --benchtime DUR     Value passed to -benchtime (default 1x; e.g. 1s).
  --help, -h         Show this help text.

Environment:
  BENCHTIME          Value passed to -benchtime (default: 1x to avoid repeated
                     reruns; set e.g. BENCHTIME=1s to restore Go's default).
  QUERY_BENCH_*      Passed through to benchmark/query/run.sh for the query suite.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --disable-crypto)
      crypto_enabled=0
      shift
      ;;
    --disable-mtls)
      mtls_enabled=0
      shift
      ;;
    --isolate-mem)
      isolate_mem=1
      shift
      ;;
    --prefetch-double)
      prefetch_double=${2:-}
      shift 2
      ;;
    --benchtime)
      benchtime=${2:-}
      shift 2
      ;;
    --help|-h)
      print_usage
      exit 0
      ;;
    --*)
      echo "Unknown option: $1" >&2
      print_usage >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

declare -a SUITES
declare -A SUITE_DIR SUITE_TAGS SUITE_ENVFILE
declare -A SUITE_CMD

add_suite() {
  local name=$1 dir=$2 tags=$3 env_file=$4 cmd=${5:-}
  SUITES+=("$name")
  SUITE_DIR["$name"]=$dir
  SUITE_TAGS["$name"]=$tags
  SUITE_ENVFILE["$name"]=$env_file
  SUITE_CMD["$name"]=$cmd
}

add_suite disk benchmark/disk "bench disk" ".env.disk"
add_suite minio benchmark/minio "bench minio" ".env.minio"
add_suite mem benchmark/mem/lq "bench mem lq" ""
add_suite aws benchmark/aws "bench aws" ".env.aws"
add_suite azure benchmark/azure "bench azure" ".env.azure"
add_suite query "" "" "" "./benchmark/query/run.sh"

MEM_BENCH_SCENARIOS=(
  "single_server_prefetch1_100p_100c"
  "single_server_prefetch4_100p_100c"
  "single_server_subscribe_100p_1c"
  "single_server_dequeue_guard"
  "double_server_prefetch4_100p_100c"
)

list_suites() {
  echo "Available benchmark suites:"
  for name in "${SUITES[@]}"; do
    printf '  - %s\n' "$name"
  done
  echo
  print_usage
}

if [[ $# -eq 0 || ${1:-} == "list" ]]; then
  list_suites
  exit 0
fi

SUITES_TO_RUN=()
if [[ ${1:-} == "all" ]]; then
  SUITES_TO_RUN=("${SUITES[@]}")
  shift
  if [[ $# -gt 0 ]]; then
    SUITES_TO_RUN+=("$@")
  fi
else
  SUITES_TO_RUN=("$@")
fi

LOG_DIR="benchmark-logs"
mkdir -p "$LOG_DIR"
declare -A STATUS
declare -A SUITE_LOGS
EXIT_CODE=0

export LOCKD_TEST_STORAGE_ENCRYPTION=$crypto_enabled
export LOCKD_TEST_WITH_MTLS=$mtls_enabled
export OTEL_SDK_DISABLED=1
export MEM_LQ_BENCH_PREFETCH_DOUBLE=$prefetch_double
if [[ $crypto_enabled -eq 1 ]]; then
  echo "LOCKD_TEST_STORAGE_ENCRYPTION=1 (encryption enabled)"
else
  echo "LOCKD_TEST_STORAGE_ENCRYPTION=0 (encryption disabled)"
fi
if [[ $mtls_enabled -eq 1 ]]; then
  echo "LOCKD_TEST_WITH_MTLS=1 (mTLS enabled)"
else
  echo "LOCKD_TEST_WITH_MTLS=0 (mTLS disabled)"
fi
echo "BENCHTIME=${benchtime} (passed to go test -benchtime)"
echo "MEM_LQ_BENCH_PREFETCH_DOUBLE=${prefetch_double} (multi-server prefetch override)"
echo "OTEL_SDK_DISABLED=1 (telemetry disabled for benchmarks)"
echo

for suite in "${SUITES_TO_RUN[@]}"; do
  dir=${SUITE_DIR[$suite]:-}
  tags=${SUITE_TAGS[$suite]:-}
  suite_cmd=${SUITE_CMD[$suite]:-}
  if [[ -z $dir && -z $suite_cmd ]]; then
    echo "Unknown suite: $suite" >&2
    EXIT_CODE=1
    continue
  fi
  env_file=${SUITE_ENVFILE[$suite]}
  if [[ -n $env_file && ! -f $env_file ]]; then
    echo "Missing environment file: $env_file (suite $suite)" >&2
    EXIT_CODE=1
    continue
  fi
  log_file="$LOG_DIR/${suite//\//-}.log"
  if [[ -n $suite_cmd ]]; then
    echo "==> Running benchmark suite: $suite"
    echo "Command: $suite_cmd"
    if bash -c "$suite_cmd" 2>&1 | tee "$log_file"; then
      STATUS[$suite]=0
    else
      STATUS[$suite]=1
      EXIT_CODE=1
    fi
    SUITE_LOGS["$suite"]+="$log_file "
    echo "Log: $log_file"
    echo
    continue
  fi
  echo "==> Running benchmark suite: $suite"
  if [[ $suite == "mem" && $isolate_mem -eq 1 ]]; then
    suite_failed=0
    for scenario in "${MEM_BENCH_SCENARIOS[@]}"; do
      scenario_log="$LOG_DIR/${suite//\//-}-${scenario}.log"
      echo "--> Scenario: $scenario"
      if [[ -n $env_file ]]; then
        cmd="set -a && source '$env_file' && set +a && go test -run=^$ -bench=BenchmarkMemQueueThroughput/$scenario -benchtime=$benchtime -count=1 -benchmem -tags '$tags' ./$dir"
      else
        cmd="go test -run=^$ -bench=BenchmarkMemQueueThroughput/$scenario -benchtime=$benchtime -count=1 -benchmem -tags '$tags' ./$dir"
      fi
      echo "Command: $cmd"
      if bash -c "$cmd" 2>&1 | tee "$scenario_log"; then
        :
      else
        suite_failed=1
        EXIT_CODE=1
      fi
      SUITE_LOGS["$suite"]+="$scenario_log "
      echo "Log: $scenario_log"
      echo
    done
    STATUS[$suite]=$suite_failed
    continue
  fi

  if [[ -n $env_file ]]; then
    cmd="set -a && source '$env_file' && set +a && go test -run=^$ -bench=. -benchtime=$benchtime -count=1 -benchmem -tags '$tags' ./$dir"
  else
    cmd="go test -run=^$ -bench=. -benchtime=$benchtime -count=1 -benchmem -tags '$tags' ./$dir"
  fi
  echo "Command: $cmd"
  if bash -c "$cmd" 2>&1 | tee "$log_file"; then
    STATUS[$suite]=0
  else
    STATUS[$suite]=1
    EXIT_CODE=1
  fi
  SUITE_LOGS["$suite"]+="$log_file "
  echo "Log: $log_file"
  echo
done

echo "Summary:"
for suite in "${SUITES_TO_RUN[@]}"; do
  status=${STATUS[$suite]:-1}
  if [[ $status -eq 0 ]]; then
    printf "  %-10s OK\n" "$suite"
  else
    printf "  %-10s FAIL\n" "$suite"
  fi
done

print_benchmark_summary() {
  local summary_tmp
  summary_tmp=$(mktemp)

  for suite in "${SUITES_TO_RUN[@]}"; do
    logs=${SUITE_LOGS[$suite]:-}
    for log in $logs; do
      if [[ -f $log ]]; then
        awk -v suite="$suite" '
          $1 ~ /^Benchmark/ {
            name=$1
            sub(/^Benchmark/, "", name)
            sub(/-[0-9]+$/, "", name)
            ns=""
            mb=""
            alloc=""
            enq=""
            deq=""
            for (i=3; i<=NF; i++) {
              unit=$i
              val=$(i-1)
              if (unit == "ns/op") { ns=val }
              else if (unit == "MB/s") { mb=val }
              else if (unit == "allocs/op") { alloc=val }
              else if (unit == "enqueue/s") { enq=val }
              else if (unit == "dequeue/s") { deq=val }
            }
            ops=""
            if (ns != "" && ns > 0) {
              ops = 1000000000 / ns
            }
            group="ops"
            if (enq != "" || deq != "") {
              group="queue"
            } else if (mb != "") {
              group="throughput"
            }
            printf "%s|%s|%s|%s|%s|%s|%s|%s\n", group, suite, name, ops, mb, alloc, enq, deq
          }
        ' "$log" >> "$summary_tmp"
      fi
    done
  done

  if [[ ! -s $summary_tmp ]]; then
    rm -f "$summary_tmp"
    return
  fi

  echo "Benchmark summary (compact):"
  echo

  awk -F'|' -v max_rows=30 '
    function trim(s, n) {
      if (length(s) > n) return substr(s, 1, n-3) "...";
      return s;
    }
    function fmt_rate(v) {
      if (v == "" || v == 0) return "-";
      if (v >= 1000000) return sprintf("%.1fM", v/1000000);
      if (v >= 1000) return sprintf("%.1fk", v/1000);
      return sprintf("%.0f", v);
    }
    function fmt_mbs(v) {
      if (v == "" || v == 0) return "-";
      if (v >= 1000) return sprintf("%.0f", v);
      return sprintf("%.1f", v);
    }
    function fmt_int(v) {
      if (v == "" || v == 0) return "-";
      return sprintf("%.0f", v);
    }
    $1 == "queue" { queue[++queue_n] = $0; next }
    $1 == "throughput" { thr[++thr_n] = $0; next }
    $1 == "ops" { ops[++ops_n] = $0; next }
    END {
      if (queue_n > 0) {
        print "Queue throughput:";
        printf "| %-6s | %-32s | %8s | %8s | %8s |\n", "Suite", "Bench", "enq/s", "deq/s", "allocs";
        printf "|-%-6s-|-%-32s-|-%8s-|-%8s-|-%8s-|\n", "------", "--------------------------------", "--------", "--------", "--------";
        for (i=1; i<=queue_n && i<=max_rows; i++) {
          split(queue[i], f, "|");
          suite=f[2]; bench=trim(f[3], 32); alloc=fmt_int(f[6]);
          enq=fmt_rate(f[7]); deq=fmt_rate(f[8]);
          printf "| %-6s | %-32s | %8s | %8s | %8s |\n", suite, bench, enq, deq, alloc;
        }
        if (queue_n > max_rows) {
          printf "_truncated: %d more rows_\n", queue_n - max_rows;
        }
        print "";
      }
      if (thr_n > 0) {
        print "Storage throughput:";
        printf "| %-6s | %-26s | %8s | %8s | %8s |\n", "Suite", "Bench", "ops/s", "MB/s", "allocs";
        printf "|-%-6s-|-%-26s-|-%8s-|-%8s-|-%8s-|\n", "------", "--------------------------", "--------", "--------", "--------";
        for (i=1; i<=thr_n && i<=max_rows; i++) {
          split(thr[i], f, "|");
          suite=f[2]; bench=trim(f[3], 26);
          ops_rate=fmt_rate(f[4]); mb=fmt_mbs(f[5]); alloc=fmt_int(f[6]);
          printf "| %-6s | %-26s | %8s | %8s | %8s |\n", suite, bench, ops_rate, mb, alloc;
        }
        if (thr_n > max_rows) {
          printf "_truncated: %d more rows_\n", thr_n - max_rows;
        }
        print "";
      }
      if (ops_n > 0) {
        print "Ops overview:";
        printf "| %-6s | %-36s | %8s | %8s |\n", "Suite", "Bench", "ops/s", "allocs";
        printf "|-%-6s-|-%-36s-|-%8s-|-%8s-|\n", "------", "------------------------------------", "--------", "--------";
        for (i=1; i<=ops_n && i<=max_rows; i++) {
          split(ops[i], f, "|");
          suite=f[2]; bench=trim(f[3], 36);
          ops_rate=fmt_rate(f[4]); alloc=fmt_int(f[6]);
          printf "| %-6s | %-36s | %8s | %8s |\n", suite, bench, ops_rate, alloc;
        }
        if (ops_n > max_rows) {
          printf "_truncated: %d more rows_\n", ops_n - max_rows;
        }
      }
    }
  ' "$summary_tmp"

  rm -f "$summary_tmp"
}

echo
print_benchmark_summary

echo
if [[ $EXIT_CODE -ne 0 ]]; then
  echo "One or more benchmark suites failed."
fi

exit $EXIT_CODE
