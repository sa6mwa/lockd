#!/usr/bin/env bash
# Run selected integration suites. Without arguments, lists available suites.

set -uo pipefail

crypto_enabled=1
mtls_enabled=1

go_test_timeout=${LOCKD_GO_TEST_TIMEOUT:-2m}

print_usage() {
  cat <<'USAGE'
Usage: run-integration-suites.sh [--disable-crypto] [--disable-mtls] [suite ...]

Options:
  --disable-crypto   Run suites with LOCKD_TEST_STORAGE_ENCRYPTION=0 (default is 1).
  --disable-mtls     Run suites with LOCKD_TEST_WITH_MTLS=0 (default is 1).
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

### Suite definitions #########################################################
declare -a SUITE_NAMES=()
declare -A SUITE_ENV SUITE_TAGS SUITE_ENVFILE

add_suite() {
  local name=$1 env=$2 tags=$3 env_file=$4
  SUITE_NAMES+=("$name")
  SUITE_ENV["$name"]=$env
  SUITE_TAGS["$name"]=$tags
  SUITE_ENVFILE["$name"]=$env_file
}

add_backend() {
  local backend=$1 has_lq=$2 has_crypto=$3 use_env=$4
  local env_file=""
  if [[ $use_env == 1 ]]; then
    env_file=".env.${backend}"
  fi
  add_suite "$backend" "$backend" "integration $backend" "$env_file"
  if [[ $has_lq == 1 ]]; then
    add_suite "$backend/lq" "$backend" "integration $backend lq" "$env_file"
  fi
  if [[ $has_crypto == 1 ]]; then
    add_suite "$backend/crypto" "$backend" "integration $backend crypto" "$env_file"
  fi
}

add_backend mem 1 0 0
add_suite "mem/query" "mem" "integration mem query" ""
add_backend disk 1 1 1
add_suite "disk/query" "disk" "integration disk query" ".env.disk"
add_backend nfs 1 0 1
add_suite "nfs/query" "nfs" "integration nfs query" ".env.nfs"
add_backend aws 1 1 1
add_suite "aws/query" "aws" "integration aws query" ".env.aws"
add_backend azure 1 1 1
add_suite "azure/query" "azure" "integration azure query" ".env.azure"
add_backend minio 1 1 1
add_suite "minio/query" "minio" "integration minio query" ".env.minio"

list_suites() {
  echo "Available suites:"
  for name in "${SUITE_NAMES[@]}"; do
    printf '  - %s\n' "$name"
  done
  echo
  echo "Usage: $0 [suite ...]"
}

SUITE_ARGS=("$@")

if [[ ${#SUITE_ARGS[@]} -eq 0 || ${SUITE_ARGS[0]} == "list" ]]; then
  list_suites
  exit 0
fi

SUITES_TO_RUN=()
bail_on_fail=0

if [[ ${SUITE_ARGS[0]:-} == "all" ]]; then
  SUITES_TO_RUN=("${SUITE_NAMES[@]}")
  bail_on_fail=1
  if [[ ${#SUITE_ARGS[@]} -gt 1 ]]; then
    SUITES_TO_RUN+=("${SUITE_ARGS[@]:1}")
  fi
else
  SUITES_TO_RUN=("${SUITE_ARGS[@]}")
fi

LOG_DIR="integration-logs"
mkdir -p "$LOG_DIR"

export LOCKD_TEST_STORAGE_ENCRYPTION=$crypto_enabled
export LOCKD_TEST_WITH_MTLS=$mtls_enabled
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
echo

declare -a RUN_SUITES=()
declare -A SUITE_STATUS
EXIT_CODE=0

for suite in "${SUITES_TO_RUN[@]}"; do
  if [[ -z ${SUITE_ENV[$suite]+_} ]]; then
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
  tags=${SUITE_TAGS[$suite]}
  log_file="$LOG_DIR/${suite//\//-}.log"
  echo "==> Running $suite (tags: $tags)"
  if [[ -n $env_file ]]; then
    cmd_string="set -a && source '$env_file' && set +a && go test -timeout $go_test_timeout -v -tags '$tags' -count=1 ./integration/..."
  else
    cmd_string="go test -timeout $go_test_timeout -v -tags '$tags' -count=1 ./integration/..."
  fi
  echo "Command: $cmd_string"
  cmd_goflags="${GOFLAGS:-}"
  if [[ $cmd_goflags != *"-p="* ]]; then
    cmd_goflags="${cmd_goflags:+$cmd_goflags }-p=1"
  fi
  if GOFLAGS="$cmd_goflags" bash -c "$cmd_string" 2>&1 | tee "$log_file"; then
    SUITE_STATUS[$suite]=0
  else
    SUITE_STATUS[$suite]=1
    EXIT_CODE=1
  fi
  RUN_SUITES+=("$suite")
  echo "Log: $log_file"
  echo
  # shellcheck disable=SC2154
  if [[ ${RESET_AFTER_SUITE:-} ]]; then
    eval "$RESET_AFTER_SUITE"
  fi
  printf '\n'

  if [[ $bail_on_fail -eq 1 && ${SUITE_STATUS[$suite]} -ne 0 ]]; then
    echo "Suite $suite failed; aborting remaining suites."
    break
  fi
done

echo "Summary:"
for suite in "${RUN_SUITES[@]}"; do
  status=${SUITE_STATUS[$suite]:-1}
  if [[ $status -eq 0 ]]; then
    printf "  %-15s OK\n" "$suite"
  else
    printf "  %-15s FAIL\n" "$suite"
  fi
done

echo
if [[ $EXIT_CODE -ne 0 ]]; then
  echo "One or more suites failed. See logs in $LOG_DIR."
fi

exit $EXIT_CODE
