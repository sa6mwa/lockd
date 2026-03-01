#!/usr/bin/env bash
set -euo pipefail

bundle_path=${1:-}
ca_path=${2:-}

if [[ -z "$bundle_path" || -z "$ca_path" ]]; then
  echo "usage: ensure_bench_bundle.sh <bundle-path> <ca-path>" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

resolve_abs() {
  local raw=$1
  if [[ "$raw" = /* ]]; then
    printf "%s\n" "$raw"
    return
  fi
  printf "%s/%s\n" "$(pwd)" "$raw"
}

bundle_path="$(resolve_abs "$bundle_path")"
ca_path="$(resolve_abs "$ca_path")"

has_all_claim() {
  local path=$1
  (
    cd "$repo_root"
    go run ./cmd/lockd auth inspect client --in "$path" 2>/dev/null \
      | rg -q 'lockd://ns/ALL\?perm=rw'
  )
}

mkdir -p "$(dirname "$bundle_path")"

if [[ -f "$bundle_path" ]] && has_all_claim "$bundle_path"; then
  exit 0
fi

if [[ ! -f "$ca_path" ]]; then
  echo "benchmark CA bundle not found: $ca_path" >&2
  exit 1
fi

echo "minting benchmark client bundle with ALL=rw at $bundle_path"
(
  cd "$repo_root"
  go run ./cmd/lockd auth new client \
    --ca-in "$ca_path" \
    --out "$bundle_path" \
    --cn "lockd-bench-client" \
    --rw-all \
    --force >/dev/null
)

if ! has_all_claim "$bundle_path"; then
  echo "minted benchmark bundle is missing ALL=rw claim: $bundle_path" >&2
  exit 1
fi
