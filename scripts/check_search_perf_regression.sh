#!/usr/bin/env bash
set -euo pipefail

BASELINE_FILE=${BASELINE_FILE:-docs/performance/2026-02-25-search-index-phase7-baseline/summary.md}
NS_REGRESSION_PCT=${NS_REGRESSION_PCT:-5}
ALLOCS_REGRESSION_PCT=${ALLOCS_REGRESSION_PCT:-5}
BENCHTIME=${BENCHTIME:-100ms}
COUNT=${COUNT:-1}
GOMAXPROCS_VALUE=${GOMAXPROCS_VALUE:-1}

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "baseline file not found: $BASELINE_FILE" >&2
  exit 1
fi

bench_pattern='Benchmark(SegmentReaderResolveWildcardFields|SegmentReaderResolveRecursiveFields|SegmentReaderWildcardContains|AdapterQueryWildcardContains|AdapterQueryRecursiveDeepLowSelectivity|AdapterQueryRecursiveDeepHighSelectivity|AdapterQueryWildcardWideLowSelectivity|AdapterQueryWildcardWideHighSelectivity|AdapterQueryWildcardContainsProfiles|AdapterQueryRecursiveProfiles|AdapterQueryFullTextContainsProfiles|AdapterQueryFullTextAllTextProfiles)$'

tmp_bench=$(mktemp)
tmp_base=$(mktemp)
tmp_cur=$(mktemp)
trap 'rm -f "$tmp_bench" "$tmp_base" "$tmp_cur"' EXIT

echo "Running search/index benchmark guard..."
echo "baseline: $BASELINE_FILE"
echo "thresholds: ns/op +${NS_REGRESSION_PCT}% allocs/op +${ALLOCS_REGRESSION_PCT}%"
echo

GOMAXPROCS="$GOMAXPROCS_VALUE" go test ./internal/search/index \
  -run '^$' \
  -bench "$bench_pattern" \
  -benchmem \
  -benchtime="$BENCHTIME" \
  -count="$COUNT" | tee "$tmp_bench"

awk -F'|' '
  BEGIN { in_table=0 }
  /^\| Benchmark \| ns\/op \| B\/op \| allocs\/op \|$/ { in_table=1; next }
  in_table && /^\| ---/ { next }
  in_table && /^\| / {
    n=$2; gsub(/^ +| +$/, "", n)
    ns=$3; gsub(/,/, "", ns); gsub(/^ +| +$/, "", ns)
    alloc=$5; gsub(/,/, "", alloc); gsub(/^ +| +$/, "", alloc)
    if (n != "" && ns ~ /^[0-9.]+$/ && alloc ~ /^[0-9.]+$/) {
      print n "|" ns "|" alloc
    }
    next
  }
  in_table && !/^\| / { exit }
' "$BASELINE_FILE" > "$tmp_base"

awk '
  $1 ~ /^Benchmark/ {
    name=$1
    sub(/^Benchmark/, "", name)
    sub(/-[0-9]+$/, "", name)
    gsub(/^ +| +$/, "", name)
    ns=""
    alloc=""
    for (i=2; i<=NF; i++) {
      if ($i == "ns/op" && i>1) ns=$(i-1)
      if ($i == "allocs/op" && i>1) alloc=$(i-1)
    }
    gsub(/,/, "", ns)
    gsub(/,/, "", alloc)
    if (name != "" && ns ~ /^[0-9.]+$/ && alloc ~ /^[0-9.]+$/) {
      print name "|" ns "|" alloc
    }
  }
' "$tmp_bench" > "$tmp_cur"

fail=0
if [[ ! -s "$tmp_base" ]]; then
  echo "failed to parse baseline benchmark table from $BASELINE_FILE" >&2
  exit 1
fi
printf "\n%-48s %12s %12s %9s %12s %12s %9s\n" "Benchmark" "Base ns/op" "Cur ns/op" "Delta%" "Base alloc" "Cur alloc" "Delta%"
printf "%-48s %12s %12s %9s %12s %12s %9s\n" "------------------------------------------------" "------------" "------------" "---------" "------------" "------------" "---------"

while IFS='|' read -r name base_ns base_alloc; do
  cur_line=$(awk -F'|' -v n="$name" '$1 == n { print $0; exit }' "$tmp_cur")
  if [[ -z "$cur_line" ]]; then
    echo "missing current benchmark result for: $name" >&2
    fail=1
    continue
  fi
  cur_ns=${cur_line#*|}
  cur_alloc=${cur_ns#*|}
  cur_ns=${cur_ns%%|*}

  ns_delta=$(awk -v b="$base_ns" -v c="$cur_ns" 'BEGIN { if (b==0) print 0; else printf "%.2f", ((c-b)/b)*100 }')
  alloc_delta=$(awk -v b="$base_alloc" -v c="$cur_alloc" 'BEGIN { if (b==0) print 0; else printf "%.2f", ((c-b)/b)*100 }')

  printf "%-48s %12s %12s %9s %12s %12s %9s\n" \
    "$name" "$base_ns" "$cur_ns" "$ns_delta" "$base_alloc" "$cur_alloc" "$alloc_delta"

  ns_regress=$(awk -v d="$ns_delta" -v t="$NS_REGRESSION_PCT" 'BEGIN { print (d > t) ? 1 : 0 }')
  alloc_regress=$(awk -v d="$alloc_delta" -v t="$ALLOCS_REGRESSION_PCT" 'BEGIN { print (d > t) ? 1 : 0 }')
  if [[ "$ns_regress" -eq 1 || "$alloc_regress" -eq 1 ]]; then
    fail=1
  fi
done < "$tmp_base"

if [[ "$fail" -ne 0 ]]; then
  echo
  echo "search perf guard FAILED: regression threshold exceeded"
  exit 1
fi

echo
echo "search perf guard PASSED"
