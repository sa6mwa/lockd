SHELL := /bin/bash
.DEFAULT_GOAL := help

RUNNER ?= ./lockd-ycsb
WORKLOAD ?= workloada
WORKLOAD_FILE = workloads/$(WORKLOAD).properties

RECORDCOUNT ?= 10000
OPERATIONCOUNT ?= 100000
THREADS ?= 8
TARGET ?= 0
PERF_LOG ?= performance.log
PERF_SERIES ?=
PERF_BASELINE_REF ?=
PERF_RUN_ID ?=
PERF_SCENARIO ?=
WARMUP_OPS ?= 2000
WARMUP_MIN_WINDOWS ?= 2
WARMUP_MAX_WINDOWS ?= 3
WARMUP_STABLE_PERCENT ?= 7.5
WARMUP_STABLE_COUNT ?= 2
WARMUP_COOLDOWN ?= 10s

LOCKD_ENDPOINTS ?=
LOCKD_ENDPOINTS := $(if $(strip $(LOCKD_ENDPOINTS)),$(LOCKD_ENDPOINTS),https://127.0.0.1:9341)
LOCKD_BUNDLE ?=
LOCKD_BUNDLE := $(if $(strip $(LOCKD_BUNDLE)),$(LOCKD_BUNDLE),../devenv/volumes/lockd-config/client.pem)
ETCD_ENDPOINTS ?=
ETCD_ENDPOINTS := $(if $(strip $(ETCD_ENDPOINTS)),$(ETCD_ENDPOINTS),127.0.0.1:2379)

LOCKD_PROPS := workloads/lockd.properties
ETCD_PROPS := workloads/etcd.properties
LOCKD_ATTACH_PROPS := workloads/lockd.attach.properties
LOCKD_TXN_PROPS := workloads/lockd.txn.properties
LOCKD_QUERYSYNC_PROPS := workloads/lockd.querysync.properties
LOCKD_QUERY_INDEX_PROPS := workloads/lockd.query.index.properties
LOCKD_QUERY_SCAN_PROPS := workloads/lockd.query.scan.properties

LOCKD_EXTRA_PROPS ?=
PERF_TAG_ARGS := $(if $(strip $(PERF_SERIES)),--perf-series=$(PERF_SERIES),) $(if $(strip $(PERF_BASELINE_REF)),--perf-baseline-ref=$(PERF_BASELINE_REF),) $(if $(strip $(PERF_RUN_ID)),--perf-run-id=$(PERF_RUN_ID),) $(if $(strip $(PERF_SCENARIO)),--perf-scenario=$(PERF_SCENARIO),)

COMMON_ARGS = -P $(WORKLOAD_FILE) -p recordcount=$(RECORDCOUNT) -p operationcount=$(OPERATIONCOUNT) -p threadcount=$(THREADS) -p target=$(TARGET)
COMMON_ARGS += --warmup-ops=$(WARMUP_OPS) --warmup-min-windows=$(WARMUP_MIN_WINDOWS) --warmup-max-windows=$(WARMUP_MAX_WINDOWS)
COMMON_ARGS += --warmup-stable-percent=$(WARMUP_STABLE_PERCENT) --warmup-stable-count=$(WARMUP_STABLE_COUNT) --warmup-cooldown=$(WARMUP_COOLDOWN)
LOCKD_ARGS = -P $(LOCKD_PROPS) $(foreach p,$(LOCKD_EXTRA_PROPS),-P $(p)) -p lockd.endpoints=$(LOCKD_ENDPOINTS) -p lockd.bundle=$(LOCKD_BUNDLE)
ETCD_ARGS = -P $(ETCD_PROPS) -p etcd.endpoints=$(ETCD_ENDPOINTS)

.PHONY: help build clean \
	lockd-load lockd-run lockd-load-attach lockd-run-attach lockd-load-txn lockd-run-txn \
	lockd-load-query lockd-run-query-index lockd-run-query-scan lockd-run-query-pair lockd-compare-query \
	etcd-load etcd-run compare perf-summary full-rerun preflight-lockd preflight-etcd

help:
	@echo "Available targets:"
	@echo "  make build                     # build lockd-ycsb runner"
	@echo "  make lockd-load                # load lockd with standard workload"
	@echo "  make lockd-run                 # run lockd with standard workload"
	@echo "  make lockd-load-attach         # load lockd with attachment overlay"
	@echo "  make lockd-run-attach          # run lockd with attachment overlay"
	@echo "  make lockd-load-txn            # load lockd with explicit txn overlay"
	@echo "  make lockd-run-txn             # run lockd with explicit txn overlay"
	@echo "  make lockd-load-query          # load lockd with scan-heavy query workload (workloade)"
	@echo "  make lockd-run-query-index     # run query workload using lockd.query.engine=index"
	@echo "  make lockd-run-query-scan      # run query workload using lockd.query.engine=scan"
	@echo "  make lockd-run-query-pair      # run index + scan query workloads then compare"
	@echo "  make lockd-compare-query       # compare latest query index vs scan runs from PERF_LOG"
	@echo "  make perf-summary              # summarize latest perf series in PERF_LOG as markdown table"
	@echo "  make full-rerun                # run the full benchmark matrix and archive dated results"
	@echo "  make etcd-load                 # load etcd with standard workload"
	@echo "  make etcd-run                  # run etcd with standard workload"
	@echo "  make compare                   # lockd load/run + etcd load/run"
	@echo ""
	@echo "Variables:"
	@echo "  WORKLOAD=workloada             # workloada..workloadf"
	@echo "  RECORDCOUNT=10000"
	@echo "  OPERATIONCOUNT=100000"
	@echo "  THREADS=8"
	@echo "  TARGET=0                       # 0 disables throttling"
	@echo "  LOCKD_ENDPOINTS=https://127.0.0.1:9341"
	@echo "  LOCKD_BUNDLE=../devenv/volumes/lockd-config/client.pem"
	@echo "  ETCD_ENDPOINTS=127.0.0.1:2379"
	@echo "  LOCKD_EXTRA_PROPS=\"workloads/lockd.attach.properties\""
	@echo "  PERF_LOG=performance.log        # set empty to disable perf logging"
	@echo "  PERF_SERIES=2026-02-25-rerun    # group related runs in PERF_LOG metadata"
	@echo "  PERF_BASELINE_REF=2026-01-27    # optional baseline reference label in metadata"
	@echo "  PERF_RUN_ID=rerun-001           # optional run-id to tie full-matrix commands together"
	@echo "  PERF_SCENARIO=workloada         # optional explicit scenario label in metadata"
	@echo "  WARMUP_OPS=2000                 # warmup ops per window (0 disables)"
	@echo "  WARMUP_MIN_WINDOWS=2            # minimum warmup windows"
	@echo "  WARMUP_MAX_WINDOWS=3            # maximum warmup windows"
	@echo "  WARMUP_STABLE_PERCENT=7.5       # percent spread to consider warm"
	@echo "  WARMUP_STABLE_COUNT=2           # windows used for stability check"
	@echo "  WARMUP_COOLDOWN=10s             # cooldown after warmup"

build:
	@go build -o $(RUNNER) ./cmd/lockd-ycsb

clean:
	@rm -f $(RUNNER)

preflight-lockd:
	@bash -c '\
		set -e; \
		if [ ! -f "$(LOCKD_BUNDLE)" ]; then \
			echo "lockd bundle not found at $(LOCKD_BUNDLE). Is docker compose up?"; \
			echo "hint: cd ../devenv && docker compose up -d"; \
			exit 1; \
		fi; \
		endpoint="$(LOCKD_ENDPOINTS)"; \
		endpoint="$${endpoint%%,*}"; \
		endpoint="$${endpoint#*://}"; \
		hostport="$$endpoint"; \
		host="$${hostport%%:*}"; \
		port="$${hostport##*:}"; \
		if [[ "$$hostport" == "$$port" ]]; then port=9341; fi; \
		if command -v timeout >/dev/null 2>&1; then \
			if ! timeout 1 bash -c "</dev/tcp/$$host/$$port" >/dev/null 2>&1; then \
				echo "lockd not reachable at $$host:$$port. Is docker compose up?"; \
				echo "hint: cd ../devenv && docker compose up -d"; \
				exit 1; \
			fi; \
		else \
			if ! (exec 3<>/dev/tcp/$$host/$$port) >/dev/null 2>&1; then \
				echo "lockd not reachable at $$host:$$port. Is docker compose up?"; \
				echo "hint: cd ../devenv && docker compose up -d"; \
				exit 1; \
			fi; \
		fi; \
	'

preflight-etcd:
	@bash -c '\
		set -e; \
		endpoint="$(ETCD_ENDPOINTS)"; \
		endpoint="$${endpoint%%,*}"; \
		endpoint="$${endpoint#*://}"; \
		url="http://$$endpoint/health"; \
		if ! curl -sf "$$url" >/dev/null 2>&1; then \
			echo "etcd not reachable at $$endpoint. Is docker compose up?"; \
			echo "hint: cd ../devenv && docker compose up -d"; \
			exit 1; \
		fi; \
	'

lockd-load: build preflight-lockd
	@$(RUNNER) load lockd $(COMMON_ARGS) $(LOCKD_ARGS) --perf-log=$(PERF_LOG) $(PERF_TAG_ARGS)

lockd-run: build preflight-lockd
	@$(RUNNER) run lockd $(COMMON_ARGS) $(LOCKD_ARGS) --perf-log=$(PERF_LOG) $(PERF_TAG_ARGS)

lockd-load-attach: LOCKD_EXTRA_PROPS := $(LOCKD_ATTACH_PROPS)
lockd-load-attach: lockd-load

lockd-run-attach: LOCKD_EXTRA_PROPS := $(LOCKD_ATTACH_PROPS)
lockd-run-attach: lockd-run

lockd-load-txn: LOCKD_EXTRA_PROPS := $(LOCKD_TXN_PROPS)
lockd-load-txn: lockd-load

lockd-run-txn: LOCKD_EXTRA_PROPS := $(LOCKD_TXN_PROPS)
lockd-run-txn: lockd-run

lockd-load-query: WORKLOAD := workloade
lockd-load-query: lockd-load

lockd-run-query-index: WORKLOAD := workloade
lockd-run-query-index: LOCKD_EXTRA_PROPS := $(LOCKD_QUERY_INDEX_PROPS) $(LOCKD_QUERYSYNC_PROPS)
lockd-run-query-index: lockd-run

lockd-run-query-scan: WORKLOAD := workloade
lockd-run-query-scan: LOCKD_EXTRA_PROPS := $(LOCKD_QUERY_SCAN_PROPS)
lockd-run-query-scan: lockd-run

lockd-compare-query:
	@bash scripts/compare_query_perf.sh --perf-log "$(PERF_LOG)" --benchmarks BENCHMARKS.md

lockd-run-query-pair: lockd-run-query-index lockd-run-query-scan lockd-compare-query

etcd-load: build preflight-etcd
	@$(RUNNER) load etcd $(COMMON_ARGS) $(ETCD_ARGS) --perf-log=$(PERF_LOG) $(PERF_TAG_ARGS)

etcd-run: build preflight-etcd
	@$(RUNNER) run etcd $(COMMON_ARGS) $(ETCD_ARGS) --perf-log=$(PERF_LOG) $(PERF_TAG_ARGS)

compare: lockd-load lockd-run etcd-load etcd-run

perf-summary:
	@bash scripts/summarize_perf_log.sh --perf-log "$(PERF_LOG)" --series "$(PERF_SERIES)"

full-rerun:
	@bash scripts/run_full_matrix.sh --perf-log "$(PERF_LOG)" --series "$(PERF_SERIES)" --baseline-ref "$(PERF_BASELINE_REF)"
