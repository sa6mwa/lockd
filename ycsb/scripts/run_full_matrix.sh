#!/usr/bin/env bash
set -euo pipefail

PERF_LOG=${PERF_LOG:-performance.log}
SERIES=${SERIES:-}
BASELINE_REF=${BASELINE_REF:-}
ARCHIVE_ROOT=${ARCHIVE_ROOT:-../docs/performance}
RUN_ID=${RUN_ID:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --perf-log)
      PERF_LOG=${2:-}
      shift 2
      ;;
    --series)
      SERIES=${2:-}
      shift 2
      ;;
    --baseline-ref)
      BASELINE_REF=${2:-}
      shift 2
      ;;
    --archive-root)
      ARCHIVE_ROOT=${2:-}
      shift 2
      ;;
    --run-id)
      RUN_ID=${2:-}
      shift 2
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: run_full_matrix.sh [--perf-log PATH] [--series NAME] [--baseline-ref NAME] [--archive-root DIR] [--run-id ID]

Runs the full YCSB matrix used by BENCHMARKS.md and archives artifacts:
- lockd core load/run
- lockd attachments load/run
- lockd explicit txn load/run
- lockd read-heavy runs (workloadb/c/d)
- etcd core load/run
- etcd read-heavy runs (workloadb/c/d)
- lockd query load + run index + run scan

Outputs:
- copied perf log
- query compare table
- markdown perf summary
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$SERIES" ]]; then
  SERIES="$(date -u +%Y-%m-%d)-full-rerun"
fi
if [[ -z "$RUN_ID" ]]; then
  RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
fi

run_make() {
  local scenario=$1
  shift
  echo
  echo "==> make $*"
  make "$@" \
    PERF_LOG="$PERF_LOG" \
    PERF_SERIES="$SERIES" \
    PERF_BASELINE_REF="$BASELINE_REF" \
    PERF_RUN_ID="$RUN_ID" \
    PERF_SCENARIO="$scenario"
}

mkdir -p "$(dirname "$PERF_LOG")"

echo "Running full YCSB matrix"
echo "  perf_log     : $PERF_LOG"
echo "  series       : $SERIES"
echo "  baseline_ref : ${BASELINE_REF:-<empty>}"
echo "  run_id       : $RUN_ID"

run_make workloada lockd-load
run_make workloada lockd-run
run_make workloada-attach lockd-load-attach
run_make workloada-attach lockd-run-attach
run_make workloada-txn lockd-load-txn
run_make workloada-txn lockd-run-txn
run_make workloadb lockd-run WORKLOAD=workloadb
run_make workloadc lockd-run WORKLOAD=workloadc
run_make workloadd lockd-run WORKLOAD=workloadd
run_make workloada etcd-load
run_make workloada etcd-run
run_make workloadb etcd-run WORKLOAD=workloadb
run_make workloadc etcd-run WORKLOAD=workloadc
run_make workloadd etcd-run WORKLOAD=workloadd
run_make workloade lockd-load-query
run_make workloade-query-index lockd-run-query-index
run_make workloade-query-scan lockd-run-query-scan

archive_dir="${ARCHIVE_ROOT}/${SERIES}"
if [[ -d "$archive_dir" ]]; then
  archive_dir="${archive_dir}-$(date -u +%H%M%S)"
fi
mkdir -p "$archive_dir"

cp "$PERF_LOG" "$archive_dir/performance.log"

if bash scripts/compare_query_perf.sh --perf-log "$PERF_LOG" --benchmarks BENCHMARKS.md > "$archive_dir/query-compare.md"; then
  :
else
  cat <<'EOF' > "$archive_dir/query-compare.md"
Query comparison unavailable (missing index/scan entries in perf log).
EOF
fi

bash scripts/summarize_perf_log.sh \
  --perf-log "$PERF_LOG" \
  --series "$SERIES" \
  --baseline-series "$BASELINE_REF" \
  > "$archive_dir/summary.md"

cat > "$archive_dir/README.md" <<EOF
# YCSB Full Matrix Rerun

- Date (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)
- Series: \`$SERIES\`
- Baseline Ref: \`${BASELINE_REF:-n/a}\`
- Run ID: \`$RUN_ID\`
- Perf Log: \`$PERF_LOG\`

Artifacts:
- \`performance.log\`: raw YCSB run log snapshot
- \`summary.md\`: series summary table (with baseline delta when baseline series exists)
- \`query-compare.md\`: query index vs scan comparison
EOF

echo
echo "Archive written:"
echo "  $archive_dir"
