SHELL := /bin/bash
.DEFAULT_GOAL := help

RUNNER ?= ./lockd-ycsb
WORKLOAD ?= workloada
WORKLOAD_FILE := workloads/$(WORKLOAD).properties

RECORDCOUNT ?= 10000
OPERATIONCOUNT ?= 100000
THREADS ?= 8
TARGET ?= 0
PERF_LOG ?= performance.log

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

LOCKD_EXTRA_PROPS ?=

COMMON_ARGS := -P $(WORKLOAD_FILE) -p recordcount=$(RECORDCOUNT) -p operationcount=$(OPERATIONCOUNT) -p threadcount=$(THREADS) -p target=$(TARGET)
LOCKD_ARGS := -P $(LOCKD_PROPS) $(foreach p,$(LOCKD_EXTRA_PROPS),-P $(p)) -p lockd.endpoints=$(LOCKD_ENDPOINTS) -p lockd.bundle=$(LOCKD_BUNDLE)
ETCD_ARGS := -P $(ETCD_PROPS) -p etcd.endpoints=$(ETCD_ENDPOINTS)

.PHONY: help build clean \
	lockd-load lockd-run lockd-load-attach lockd-run-attach lockd-load-txn lockd-run-txn \
	etcd-load etcd-run compare preflight-lockd preflight-etcd

help:
	@echo "Available targets:"
	@echo "  make build                     # build lockd-ycsb runner"
	@echo "  make lockd-load                # load lockd with standard workload"
	@echo "  make lockd-run                 # run lockd with standard workload"
	@echo "  make lockd-load-attach         # load lockd with attachment overlay"
	@echo "  make lockd-run-attach          # run lockd with attachment overlay"
	@echo "  make lockd-load-txn            # load lockd with explicit txn overlay"
	@echo "  make lockd-run-txn             # run lockd with explicit txn overlay"
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
	@$(RUNNER) load lockd $(COMMON_ARGS) $(LOCKD_ARGS) --perf-log=$(PERF_LOG)

lockd-run: build preflight-lockd
	@$(RUNNER) run lockd $(COMMON_ARGS) $(LOCKD_ARGS) --perf-log=$(PERF_LOG)

lockd-load-attach: LOCKD_EXTRA_PROPS := $(LOCKD_ATTACH_PROPS)
lockd-load-attach: lockd-load

lockd-run-attach: LOCKD_EXTRA_PROPS := $(LOCKD_ATTACH_PROPS)
lockd-run-attach: lockd-run

lockd-load-txn: LOCKD_EXTRA_PROPS := $(LOCKD_TXN_PROPS)
lockd-load-txn: lockd-load

lockd-run-txn: LOCKD_EXTRA_PROPS := $(LOCKD_TXN_PROPS)
lockd-run-txn: lockd-run

etcd-load: build preflight-etcd
	@$(RUNNER) load etcd $(COMMON_ARGS) $(ETCD_ARGS) --perf-log=$(PERF_LOG)

etcd-run: build preflight-etcd
	@$(RUNNER) run etcd $(COMMON_ARGS) $(ETCD_ARGS) --perf-log=$(PERF_LOG)

compare: lockd-load lockd-run etcd-load etcd-run
