#!/usr/bin/env bash
# Run selected integration suites. Without arguments, lists available suites.

set -uo pipefail

crypto_enabled=1
mtls_enabled=1

go_test_timeout=${LOCKD_GO_TEST_TIMEOUT:-2m}

start_ts=$(date +%s)

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
add_suite "mem/crypto" "mem" "integration mem crypto" ""
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
add_suite "mixed" "mixed" "integration mixed" ".env.minio .env.aws .env.azure"

format_duration() {
  local total=$1
  local hours=$((total / 3600))
  local mins=$(((total % 3600) / 60))
  local secs=$((total % 60))
  if [[ $hours -gt 0 ]]; then
    printf "%d:%02d:%02d" "$hours" "$mins" "$secs"
  else
    printf "%d:%02d" "$mins" "$secs"
  fi
}

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
  suite_timeout=$go_test_timeout
  if [[ $suite == mem* ]]; then
    suite_timeout=${LOCKD_MEM_GO_TEST_TIMEOUT:-5m}
  fi
  if [[ $suite == disk* ]]; then
    suite_timeout=${LOCKD_DISK_GO_TEST_TIMEOUT:-5m}
  fi
  if [[ $suite == nfs* ]]; then
    suite_timeout=${LOCKD_NFS_GO_TEST_TIMEOUT:-5m}
  fi
  if [[ $suite == minio* ]]; then
    suite_timeout=${LOCKD_MINIO_GO_TEST_TIMEOUT:-5m}
  fi
  if [[ $suite == azure* ]]; then
    suite_timeout=${LOCKD_AZURE_GO_TEST_TIMEOUT:-5m}
  fi
  if [[ $suite == aws* ]]; then
    suite_timeout=${LOCKD_AWS_GO_TEST_TIMEOUT:-10m}
  fi
  if [[ $suite == mixed* ]]; then
    suite_timeout=${LOCKD_MIXED_GO_TEST_TIMEOUT:-8m}
  fi
  env_file=${SUITE_ENVFILE[$suite]}
  if [[ -n $env_file ]]; then
    for file in $env_file; do
      if [[ ! -f $file ]]; then
        echo "Missing environment file: $file (suite $suite)" >&2
        EXIT_CODE=1
        continue 2
      fi
    done
  fi
  tags=${SUITE_TAGS[$suite]}
  log_file="$LOG_DIR/${suite//\//-}.log"
  echo "==> Running $suite (tags: $tags)"
  if [[ -n $env_file ]]; then
    if [[ $suite == "mixed" ]]; then
      cmd_string=$(cat <<EOF
set -e
load_env_file() {
  local file=\$1
  set -a
  source "\$file"
  set +a
}
capture_minio() {
  if [[ -n "\${LOCKD_STORE:-}" ]]; then
    export LOCKD_MINIO_STORE="\$LOCKD_STORE"
  fi
  if [[ -n "\${LOCKD_S3_ROOT_USER:-}" ]]; then
    export LOCKD_MINIO_S3_ROOT_USER="\$LOCKD_S3_ROOT_USER"
  fi
  if [[ -n "\${LOCKD_S3_ROOT_PASSWORD:-}" ]]; then
    export LOCKD_MINIO_S3_ROOT_PASSWORD="\$LOCKD_S3_ROOT_PASSWORD"
  fi
  if [[ -n "\${LOCKD_S3_ACCESS_KEY_ID:-}" ]]; then
    export LOCKD_MINIO_S3_ACCESS_KEY_ID="\$LOCKD_S3_ACCESS_KEY_ID"
  fi
  if [[ -n "\${LOCKD_S3_SECRET_ACCESS_KEY:-}" ]]; then
    export LOCKD_MINIO_S3_SECRET_ACCESS_KEY="\$LOCKD_S3_SECRET_ACCESS_KEY"
  fi
  if [[ -n "\${LOCKD_S3_SESSION_TOKEN:-}" ]]; then
    export LOCKD_MINIO_S3_SESSION_TOKEN="\$LOCKD_S3_SESSION_TOKEN"
  fi
  unset LOCKD_STORE LOCKD_S3_ROOT_USER LOCKD_S3_ROOT_PASSWORD LOCKD_S3_ACCESS_KEY_ID LOCKD_S3_SECRET_ACCESS_KEY LOCKD_S3_SESSION_TOKEN
}
capture_aws() {
  if [[ -n "\${LOCKD_STORE:-}" ]]; then
    export LOCKD_AWS_STORE="\$LOCKD_STORE"
  fi
  if [[ -n "\${LOCKD_AWS_REGION:-}" ]]; then
    export LOCKD_AWS_REGION="\$LOCKD_AWS_REGION"
  elif [[ -n "\${AWS_REGION:-}" ]]; then
    export LOCKD_AWS_REGION="\$AWS_REGION"
  elif [[ -n "\${AWS_DEFAULT_REGION:-}" ]]; then
    export LOCKD_AWS_REGION="\$AWS_DEFAULT_REGION"
  fi
  unset LOCKD_STORE
}
capture_azure() {
  if [[ -n "\${LOCKD_STORE:-}" ]]; then
    export LOCKD_AZURE_STORE="\$LOCKD_STORE"
  fi
  if [[ -n "\${LOCKD_AZURE_ACCOUNT_KEY:-}" ]]; then
    export LOCKD_AZURE_ACCOUNT_KEY="\$LOCKD_AZURE_ACCOUNT_KEY"
  fi
  if [[ -n "\${LOCKD_AZURE_ENDPOINT:-}" ]]; then
    export LOCKD_AZURE_ENDPOINT="\$LOCKD_AZURE_ENDPOINT"
  fi
  if [[ -n "\${LOCKD_AZURE_SAS_TOKEN:-}" ]]; then
    export LOCKD_AZURE_SAS_TOKEN="\$LOCKD_AZURE_SAS_TOKEN"
  fi
  unset LOCKD_STORE
}
for file in $env_file; do
  load_env_file "\$file"
  case "\$file" in
    *minio*) capture_minio ;;
    *aws*) capture_aws ;;
    *azure*) capture_azure ;;
  esac
done
go test -timeout $suite_timeout -v -tags '$tags' -count=1 ./integration/...
EOF
)
    else
      cmd_string="set -a && source '$env_file' && set +a && go test -timeout $suite_timeout -v -tags '$tags' -count=1 ./integration/..."
    fi
  else
    cmd_string="go test -timeout $suite_timeout -v -tags '$tags' -count=1 ./integration/..."
  fi
  echo "Command: $cmd_string"
  cmd_goflags="${GOFLAGS:-}"
  if [[ $cmd_goflags != *"-p="* ]]; then
    if [[ $suite == aws* ]]; then
      default_p=${LOCKD_AWS_GOFLAGS_P:-2}
    else
      default_p=1
    fi
    cmd_goflags="${cmd_goflags:+$cmd_goflags }-p=${default_p}"
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
end_ts=$(date +%s)
elapsed=$((end_ts - start_ts))
printf "Elapsed: %s\n" "$(format_duration "$elapsed")"

echo
if [[ $EXIT_CODE -ne 0 ]]; then
  echo "One or more suites failed. See logs in $LOG_DIR."
fi

exit $EXIT_CODE
