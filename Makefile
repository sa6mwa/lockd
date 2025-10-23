SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help test test-integration-aws test-integration-minio test-integration-disk test-integration-azure test-integration-otlp test-integration-otlp-external bench-disk diagrams

help:
	@echo "Available targets:"
	@echo "  make test                    # run unit tests"
	@echo "  make test-integration-aws    # run AWS S3 integration suite"
	@echo "  make test-integration-minio  # run MinIO integration suite"
	@echo "  make test-integration-disk   # run disk backend integration suite"
	@echo "  make test-integration-azure  # run Azure Blob integration suite"
	@echo "  make test-integration-otlp   # run OTLP observability integration suite"
	@echo "  make test-integration-otlp-external # run OTLP tests against external collector"
	@echo "  make bench-disk              # run disk backend benchmarks"
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

test-integration-aws:
	$(call ENSURE_ENV,.env.aws,AWS_ENV_EXAMPLE)
	@echo "Running AWS integration tests"
	@$(call RUN_WITH_ENV,.env.aws,go test -timeout 2m -count 1 -v -tags "integration aws" ./integration/aws)

test-integration-minio:
	$(call ENSURE_ENV,.env.minio,MINIO_ENV_EXAMPLE)
	@echo "Running MinIO integration tests"
	@$(call RUN_WITH_ENV,.env.minio,go test -timeout 1m -count 1 -v -tags "integration minio" ./integration/minio)

test-integration-disk:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@echo "Running disk integration tests"
	@$(call RUN_WITH_ENV,.env.disk,go test -timeout 1m -count 1 -v -tags "integration disk" ./integration/disk)

test-integration-azure:
	$(call ENSURE_ENV,.env.azure,AZURE_ENV_EXAMPLE)
	@echo "Running Azure integration tests"
	@$(call RUN_WITH_ENV,.env.azure,go test -timeout 2m -count 1 -v -tags "integration azure" ./integration/azure)

test-integration-otlp:
	$(call ENSURE_ENV,.env.otlp,OTLP_ENV_EXAMPLE)
	@echo "Running OTLP observability integration tests"
	@$(call RUN_WITH_ENV,.env.otlp,go test -timeout 90s -count 1 -v -tags "integration disk minio otlp" ./integration/disk ./integration/minio)

test-integration-otlp-external:
	$(call ENSURE_ENV,.env.otlp,OTLP_ENV_EXAMPLE)
	@echo "Running OTLP external collector integration tests"
	@$(call RUN_WITH_ENV,.env.otlp,go test -timeout 90s -count 1 -v -tags "integration disk minio otlp external" ./integration/disk ./integration/minio)

bench-disk:
	$(call ENSURE_ENV,.env.disk,DISK_ENV_EXAMPLE)
	@echo "Running disk backend benchmarks (5 iterations each)"
	@$(call RUN_WITH_ENV,.env.disk,go test -run=^$$ -bench=LockdDisk -benchtime=5x -count 1 -tags "integration disk bench" ./integration/disk)

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
