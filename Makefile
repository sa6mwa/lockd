GO ?= go
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
BIN_DIR ?= bin
TARGET ?= lockd
BIN_PATH ?= $(BIN_DIR)/$(TARGET)
CGO_ENABLED = 0
SUDO ?= sudo
SHELL := /bin/bash
CONTAINER_BUILDER ?= $(shell command -v podman || command -v nerdctl || command -v docker)
IMAGE ?= docker.io/pktsystems/lockd
LOCKD_VERSION ?= $(shell $(GO) run -buildvcs=true ./cmd/lockd version --version)
LOCKD_SEMVER ?= $(shell $(GO) run -buildvcs=true ./cmd/lockd version --semver)
.DEFAULT_GOAL := help

PERF_FAST_PRESET := fast-v1
PERF_FAST_BASELINE_HISTORY := docs/performance/lockd-bench-fast-baseline-history.jsonl
PERF_FAST_RUN_HISTORY := docs/performance/lockd-bench-fast-run-history.jsonl
PERF_FULL_PRESET := full-v1
PERF_FULL_BASELINE_HISTORY := docs/performance/lockd-bench-full-baseline-history.jsonl
PERF_FULL_RUN_HISTORY := docs/performance/lockd-bench-full-run-history.jsonl

.PHONY: help test test-integration bench perf-show-frozen-fast perf-show-run-history-fast perf-guard-fast perf-freeze-fast-baseline perf-show-frozen-full perf-show-run-history-full perf-guard-full perf-freeze-full-baseline fuzz diagrams swagger build container push-container clean install

help:
	@echo "Available targets:"
	@echo "  make test                    # run unit tests"
	@echo "  make test-integration        # run integration suites (pass SUITES=...)"
	@echo "  make bench                   # run benchmark suites (pass SUITES=...)"
	@echo "  make perf-show-frozen-fast   # print frozen fast perf baseline summary"
	@echo "  make perf-show-run-history-fast # print recent fast perf run history (override LIMIT=3)"
	@echo "  make perf-guard-fast         # run the daily fast disk perf guard and append fast run-history"
	@echo "  make perf-freeze-fast-baseline # freeze the latest recorded fast run as the new baseline"
	@echo "  make perf-show-frozen-full   # print frozen full perf baseline summary"
	@echo "  make perf-show-run-history-full # print recent full perf run history (override LIMIT=3)"
	@echo "  make perf-guard-full         # run the full disk perf comparison and append full run-history"
	@echo "  make perf-freeze-full-baseline # freeze the latest recorded full run as the new baseline"
	@echo "  make fuzz                    # run all fuzzers (default 15s each, override FUZZ_TIME=...)"
	@echo "  make swagger                 # regenerate Swagger/OpenAPI artifacts"
	@echo "  make diagrams                # render PlantUML sequence diagrams to JPEG"
	@echo "  make build                   # build ./cmd/lockd into $(BIN_PATH)"
	@echo "  make container               # build container image (podman/nerdctl/docker)"
	@echo "  make push-container          # push version + latest container tags"
	@echo "  make clean                   # remove $(BIN_DIR)/"
	@echo "  make install                 # install $(BIN_PATH) into $(BINDIR)"

# Example environment file contents shown when missing
AWS_ENV_EXAMPLE := AWS_ACCESS_KEY_ID=your-access-key\nAWS_SECRET_ACCESS_KEY=your-secret\nAWS_REGION=us-west-2\nLOCKD_STORE=aws://your-bucket/prefix
MINIO_ENV_EXAMPLE := MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=minioadmin\nMINIO_ACCESS_KEY=minioadmin\nMINIO_SECRET_KEY=minioadmin\nLOCKD_S3_ACCESS_KEY_ID=minioadmin\nLOCKD_S3_SECRET_ACCESS_KEY=minioadmin\nLOCKD_STORE=s3://localhost:9000/lockd-integration?insecure=1
DISK_ENV_EXAMPLE := LOCKD_STORE=disk:///mnt/nfs4-lockd
AZURE_ENV_EXAMPLE := LOCKD_STORE=azure://youraccount/container/prefix\nLOCKD_AZURE_ACCOUNT_KEY=yourkey
OTLP_ENV_EXAMPLE := LOCKD_OTLP_ENDPOINT=localhost:4317\nLOCKD_STORE=s3://localhost:9000/lockd-integration-test?insecure=1\nMINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=minioadmin\nLOCKD_S3_ACCESS_KEY_ID=minioadmin\nLOCKD_S3_SECRET_ACCESS_KEY=minioadmin

define ENSURE_ENV
	@if [ ! -f $(1) ]; then \
		echo "Missing $(1). Create it with contents similar to:"; \
		printf '%b\n' "$($(2))"; \
		exit 1; \
	fi
endef

define RUN_WITH_ENV
	set -a && source $(1) && set +a && time $(2)
endef

test:
	go test -v -count=1 -cover ./...
	cd ycsb && go test -v -count=1 -cover ./...
	cd examples && go test -v -count=1 -cover ./...

test-integration:
	@if [[ -z "$(SUITES)" ]]; then \
		echo "Running all integration suites"; \
		./run-integration-suites.sh all; \
	else \
		echo "Running suites: $(SUITES)"; \
		./run-integration-suites.sh $(SUITES); \
	fi

bench:
	@if [[ -z "$(SUITES)" ]]; then \
		echo "Running all benchmark suites"; \
		./run-benchmark-suites.sh all; \
	else \
		echo "Running benchmark suites: $(SUITES)"; \
		./run-benchmark-suites.sh $(SUITES); \
	fi

perf-show-frozen-fast:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-report \
			-baseline-preset $(PERF_FAST_PRESET) \
			-baseline-history $(PERF_FAST_BASELINE_HISTORY)

perf-show-run-history-fast:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-run-report \
			-baseline-preset $(PERF_FAST_PRESET) \
			-baseline-run-history $(PERF_FAST_RUN_HISTORY) \
			-baseline-report-limit $(or $(LIMIT),3)

perf-guard-fast:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@SECONDS=0; \
	status=0; \
	set -a && source .env.disk && set +a || status=$$?; \
	if [[ $$status -eq 0 ]]; then \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline \
			-baseline-preset $(PERF_FAST_PRESET) \
			-baseline-backends disk \
			-baseline-history $(PERF_FAST_BASELINE_HISTORY) \
			-baseline-run-history $(PERF_FAST_RUN_HISTORY) \
			-baseline-append-history=false || status=$$?; \
	fi; \
	printf 'Elapsed: %02d:%02d\n' $$((SECONDS/60)) $$((SECONDS%60)); \
	exit $$status

perf-freeze-fast-baseline:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-freeze-latest \
			-baseline-preset $(PERF_FAST_PRESET) \
			-baseline-history $(PERF_FAST_BASELINE_HISTORY) \
			-baseline-run-history $(PERF_FAST_RUN_HISTORY)

perf-show-frozen-full:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-report \
			-baseline-preset $(PERF_FULL_PRESET) \
			-baseline-history $(PERF_FULL_BASELINE_HISTORY)

perf-show-run-history-full:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-run-report \
			-baseline-preset $(PERF_FULL_PRESET) \
			-baseline-run-history $(PERF_FULL_RUN_HISTORY) \
			-baseline-report-limit $(or $(LIMIT),3)

perf-guard-full:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@SECONDS=0; \
	status=0; \
	set -a && source .env.disk && set +a || status=$$?; \
	if [[ $$status -eq 0 ]]; then \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline \
			-baseline-preset $(PERF_FULL_PRESET) \
			-baseline-backends disk \
			-baseline-history $(PERF_FULL_BASELINE_HISTORY) \
			-baseline-run-history $(PERF_FULL_RUN_HISTORY) \
			-baseline-append-history=false || status=$$?; \
	fi; \
	printf 'Elapsed: %02d:%02d\n' $$((SECONDS/60)) $$((SECONDS%60)); \
	exit $$status

perf-freeze-full-baseline:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@set -a && source .env.disk && set +a && \
		$(GO) run ./cmd/lockd-bench \
			-mode baseline-freeze-latest \
			-baseline-preset $(PERF_FULL_PRESET) \
			-baseline-history $(PERF_FULL_BASELINE_HISTORY) \
			-baseline-run-history $(PERF_FULL_RUN_HISTORY)

FUZZ_TIME ?= 15s

fuzz:
	@set -euo pipefail; \
	fuzzers=( \
		". FuzzAuthenticatedAPISurfaceNoServerError" \
		". FuzzDiskNamespaceAndPathContainment" \
		"./internal/httpapi FuzzCompactJSON" \
		"./internal/jsonutil FuzzCompactWriter" \
		"./internal/jsonutilv2 FuzzCompactWriter" \
		"./internal/connguard FuzzConnectionGuardPortRotationStillBlocks" \
		"./internal/connguard FuzzConnectionGuardExpiryAndIsolation" \
		"./internal/connguard FuzzPrefixedConnReadConsistency" \
	); \
	failed=0; \
	for entry in "$${fuzzers[@]}"; do \
		pkg="$${entry%% *}"; \
		name="$${entry#* }"; \
		echo "==> go test $$pkg -run=^$$ -fuzz=$$name -fuzztime=$(FUZZ_TIME)"; \
		if ! $(GO) test "$$pkg" -run=^$$ -fuzz="$$name" -fuzztime="$(FUZZ_TIME)"; then \
			failed=1; \
		fi; \
	done; \
	exit "$$failed"

PLANTUML_SOURCES := $(wildcard docs/diagrams/*.puml)
PLANTUML_OUT_SVG := $(PLANTUML_SOURCES:.puml=.svg)

diagrams: $(PLANTUML_SOURCES)
	@echo "Rendering PlantUML diagrams to PNG (high resolution)"
	@plantuml -tsvg $(PLANTUML_SOURCES)
	@echo "Fixing background color of SVGs"
	@for svg in $(PLANTUML_OUT_SVG); do \
		sed -i '0,/<svg[^>]*>/s//&\n  <rect width="100%" height="100%" fill="#ffffff"\/>/' "$$svg"; \
	done

swagger:
	@echo "Generating Swagger/OpenAPI documentation"
	@echo "(requires swag, install with: go install github.com/swaggo/swag/v2/cmd/swag@latest)"
	$(GO) generate ./swagger

tidy:
	$(GO) mod tidy
	cd devenv/assure && $(GO) mod tidy
	cd ycsb && $(GO) mod tidy
	cd examples/ && $(GO) mod tidy

$(BIN_PATH):
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o $(BIN_PATH) -trimpath -ldflags '-s -w' ./cmd/lockd

build: $(BIN_PATH)

podman.yaml:
	envsubst < podman.template.yaml > podman.yaml

podman-mcp.yaml:
	envsubst < podman-mcp.template.yaml > podman-mcp.yaml

container:
	@if [ -z "$(CONTAINER_BUILDER)" ]; then \
		echo "Error: no container builder found (podman, nerdctl, docker)." >&2; \
		exit 1; \
	fi
	$(CONTAINER_BUILDER) build -f Containerfile --build-arg TARGETOS=$(shell $(GO) env GOOS) --build-arg TARGETARCH=$(shell $(GO) env GOARCH) -t $(IMAGE):$(LOCKD_VERSION) .
	$(CONTAINER_BUILDER) tag $(IMAGE):$(LOCKD_VERSION) $(IMAGE):latest

push-container:
	$(CONTAINER_BUILDER) push $(IMAGE):$(LOCKD_VERSION)
	$(CONTAINER_BUILDER) push $(IMAGE):latest

clean:
	rm -rf $(BIN_DIR)

install:
	$(SUDO) install -m 0755 $(BIN_PATH) $(BINDIR)/$(TARGET)
