.PHONY: help build test e2e-setup e2e-start e2e-stop e2e-test e2e-cleanup e2e-full

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build KMinion binary
	go build -o kminion .

test: ## Run unit tests
	go test -v ./...

fmt: ## Format all Go code
	go fmt ./...

e2e-setup: ## Start Kafka cluster for E2E testing
	@chmod +x e2e/bin/setup-kafka.sh
	./e2e/bin/setup-kafka.sh start

e2e-start: ## Start KMinion with E2E configuration
	@chmod +x e2e/bin/start-kminion.sh
	./e2e/bin/start-kminion.sh start

e2e-stop: ## Stop KMinion
	@chmod +x e2e/bin/start-kminion.sh
	./e2e/bin/start-kminion.sh stop

e2e-test: ## Run E2E integration tests (requires Kafka and KMinion to be running)
	@chmod +x e2e/bin/integration-test.sh
	./e2e/bin/integration-test.sh

e2e-cleanup: ## Stop and cleanup Kafka cluster and KMinion
	@chmod +x e2e/bin/start-kminion.sh e2e/bin/setup-kafka.sh
	./e2e/bin/start-kminion.sh stop || true
	./e2e/bin/setup-kafka.sh stop || true

e2e-full: e2e-setup ## Run full E2E test suite (setup, build, start, test, cleanup)
	@echo "Starting full E2E test suite..."
	@trap '$(MAKE) e2e-cleanup' EXIT; \
	$(MAKE) build && \
	$(MAKE) e2e-start && \
	$(MAKE) e2e-test


IMAGE ?= kminion:latest

docker-build: ## Build and push Docker image for multiple archs (IMAGE=kminion:latest)
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "development") \
		--build-arg COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown") \
		--build-arg BUILT_AT=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ") \
		--push \
		-t $(IMAGE) \
		.
