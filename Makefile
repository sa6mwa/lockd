SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help test test-integration-aws test-integration-minio test-integration-disk test-integration-pebble test-integration-azure diagrams

help:
	@echo "Available targets:"
	@echo "  make test                    # run unit tests"
	@echo "  make test-integration-aws    # run AWS S3 integration suite"
	@echo "  make test-integration-minio  # run MinIO integration suite"
	@echo "  make test-integration-disk   # run disk backend integration suite"
	@echo "  make test-integration-pebble # run Pebble integration suite"
	@echo "  make test-integration-azure  # run Azure Blob integration suite"
	@echo "  make diagrams                # render PlantUML sequence diagrams to JPEG"

# Example environment file contents shown when missing
AWS_ENV_EXAMPLE := AWS_ACCESS_KEY_ID=your-access-key\nAWS_SECRET_ACCESS_KEY=your-secret\nAWS_REGION=us-west-2\nLOCKD_STORE=s3://your-bucket/prefix
MINIO_ENV_EXAMPLE := MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=minioadmin\nMINIO_ACCESS_KEY=minioadmin\nMINIO_SECRET_KEY=minioadmin\nLOCKD_STORE=minio://localhost:9000/lockd-integration?insecure=1
DISK_ENV_EXAMPLE := LOCKD_STORE=disk:///mnt/nfs4-lockd\nLOCKD_DISK_ROOT=/mnt/nfs4-lockd
PEBBLE_ENV_EXAMPLE := LOCKD_STORE=pebble:///tmp/lockd-pebble
AZURE_ENV_EXAMPLE := LOCKD_AZURE_ACCOUNT=youraccount\nLOCKD_AZURE_ACCOUNT_KEY=yourkey\nLOCKD_STORE=azure://youraccount/container/prefix

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

test-integration-aws:
	$(call ENSURE_ENV,.env.aws,AWS_ENV_EXAMPLE)
	@echo "Running AWS integration tests"
	@$(call RUN_WITH_ENV,.env.aws,go test -timeout 1m -count 1 -v -tags "integration aws" ./integration/aws)

test-integration-minio:
	$(call ENSURE_ENV,.env.minio,MINIO_ENV_EXAMPLE)
	@echo "Running MinIO integration tests"
	@$(call RUN_WITH_ENV,.env.minio,go test -timeout 1m -count 1 -v -tags "integration minio" ./integration/minio)

test-integration-disk:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@echo "Running disk integration tests"
	@$(call RUN_WITH_ENV,.env.disk,go test -timeout 1m -count 1 -v -tags "integration disk" ./integration/disk)

test-integration-pebble:
	$(call ENSURE_ENV,.env.pebble,PEBBLE_ENV_EXAMPLE)
	@echo "Running Pebble integration tests"
	@$(call RUN_WITH_ENV,.env.pebble,go test -timeout 1m -count 1 -v -tags "integration pebble" ./integration/pebble)

test-integration-azure:
	$(call ENSURE_ENV,.env.azure,AZURE_ENV_EXAMPLE)
	@echo "Running Azure integration tests"
	@$(call RUN_WITH_ENV,.env.azure,go test -timeout 1m -count 1 -v -tags "integration azure" ./integration/azure)

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
