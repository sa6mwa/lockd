SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help test test-integration bench diagrams

help:
	@echo "Available targets:"
	@echo "  make test                    # run unit tests"
	@echo "  make test-integration        # run integration suites (pass SUITES=...)"
	@echo "  make bench                    # run benchmark suites (pass SUITES=...)"
	@echo "  make swagger                 # regenerate Swagger/OpenAPI artifacts"
	@echo "  make diagrams                # render PlantUML sequence diagrams to JPEG"

# Example environment file contents shown when missing
AWS_ENV_EXAMPLE := AWS_ACCESS_KEY_ID=your-access-key\nAWS_SECRET_ACCESS_KEY=your-secret\nAWS_REGION=us-west-2\nLOCKD_STORE=aws://your-bucket/prefix
MINIO_ENV_EXAMPLE := MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=minioadmin\nMINIO_ACCESS_KEY=minioadmin\nMINIO_SECRET_KEY=minioadmin\nLOCKD_S3_ACCESS_KEY_ID=minioadmin\nLOCKD_S3_SECRET_ACCESS_KEY=minioadmin\nLOCKD_STORE=s3://localhost:9000/lockd-integration?insecure=1
DISK_ENV_EXAMPLE := LOCKD_STORE=disk:///mnt/nfs4-lockd\nLOCKD_DISK_ROOT=/mnt/nfs4-lockd
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
	@echo "Running unit tests"
	@go test ./...

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

PLANTUML_SOURCES := $(wildcard docs/diagrams/*.puml)
PLANTUML_OUT_SVG := $(PLANTUML_SOURCES:.puml=.svg)

.PHONY: diagrams
diagrams: $(PLANTUML_SOURCES)
	@echo "Rendering PlantUML diagrams to PNG (high resolution)"
	@plantuml -tsvg $(PLANTUML_SOURCES)
	@echo "Fixing background color of SVGs"
	@for svg in $(PLANTUML_OUT_SVG); do \
		sed -i '0,/<svg[^>]*>/s//&\n  <rect width="100%" height="100%" fill="#ffffff"\/>/' "$$svg"; \
	done

.PHONY: swagger
swagger:
	@echo "Generating Swagger/OpenAPI documentation"
	@go generate ./swagger
