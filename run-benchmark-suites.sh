#!/usr/bin/env bash
# Run selected benchmark suites under ./benchmark.
set -uo pipefail

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
  echo "Usage: $0 [suite ...]"
}

if [[ $# -eq 0 || $1 == "list" ]]; then
  list_suites
  exit 0
fi

SUITES_TO_RUN=()
if [[ $1 == "all" ]]; then
  SUITES_TO_RUN=("${SUITES[@]}")
  shift
  if [[ $# -gt 0 ]]; then
    SUITES_TO_RUN+=($@)
  fi
else
  SUITES_TO_RUN=($@)
fi

LOG_DIR="benchmark-logs"
mkdir -p "$LOG_DIR"
declare -A STATUS
EXIT_CODE=0

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
  cmd="go test -run=^$ -bench=. -count=1 -benchmem -tags '$tags' ./$dir"
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
