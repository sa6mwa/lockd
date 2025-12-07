#!/usr/bin/env bash
# Run selected benchmark suites under ./benchmark.
set -uo pipefail

crypto_enabled=1
mtls_enabled=1
benchtime=${BENCHTIME:-1x}
prefetch_double=8

print_usage() {
  cat <<'USAGE'
Usage: run-benchmark-suites.sh [--disable-crypto] [--disable-mtls] [--prefetch-double N] [--benchtime DUR] [suite ...]

Options:
  --disable-crypto   Run benchmarks with LOCKD_TEST_STORAGE_ENCRYPTION=0 (default 1).
  --disable-mtls     Run benchmarks with LOCKD_TEST_WITH_MTLS=0 (default 1).
  --prefetch-double N Set MEM_LQ_BENCH_PREFETCH_DOUBLE for multi-server mem benches (default 8).
  --benchtime DUR     Value passed to -benchtime (default 1x; e.g. 1s).
  --help, -h         Show this help text.

Environment:
  BENCHTIME          Value passed to -benchtime (default: 1x to avoid repeated
                     reruns; set e.g. BENCHTIME=1s to restore Go's default).
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
declare -A SUITE_DIR SUITE_TAGS

add_suite() {
  local name=$1 dir=$2 tags=$3
  SUITES+=("$name")
  SUITE_DIR["$name"]=$dir
  SUITE_TAGS["$name"]=$tags
}

add_suite disk benchmark/disk "bench disk"
add_suite minio benchmark/minio "bench minio"
add_suite mem benchmark/mem/lq "bench mem lq"

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
  if [[ -z $dir ]]; then
    echo "Unknown suite: $suite" >&2
    EXIT_CODE=1
    continue
  fi
  log_file="$LOG_DIR/${suite//\//-}.log"
  echo "==> Running benchmark suite: $suite"
  cmd="go test -run=^$ -bench=. -benchtime=$benchtime -count=1 -benchmem -tags '$tags' ./$dir"
  echo "Command: $cmd"
  if bash -c "$cmd" 2>&1 | tee "$log_file"; then
    STATUS[$suite]=0
  else
    STATUS[$suite]=1
    EXIT_CODE=1
  fi
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

echo
if [[ $EXIT_CODE -ne 0 ]]; then
  echo "One or more benchmark suites failed."
fi

exit $EXIT_CODE
