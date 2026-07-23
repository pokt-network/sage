##########################
### SAGE Gateway Makefile #
##########################

BINARY      := sagegw
BIN_DIR     := bin
CMD         := ./cmd/sagegw

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS     := -ldflags "-X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildDate=$(BUILD_DATE)"

CONFIG_PATH ?=

DOCKER_IMAGE ?= sage:latest

.DEFAULT_GOAL := help

.PHONY: help
help: ## List all available targets with descriptions
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

##########################
### Build               ###
##########################

.PHONY: sage_build
sage_build: ## Build the sagegw binary to bin/sagegw
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build $(LDFLAGS) -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: sage_run
sage_run: ## Run sagegw directly (set CONFIG_PATH env variable)
	go run $(LDFLAGS) $(CMD) -config $(CONFIG_PATH)

##########################
### Test                ###
##########################

.PHONY: test_unit
test_unit: ## Run unit tests (short, with race detector)
	go test ./... -short -count=1 -race

.PHONY: test_all
test_all: ## Run all tests (no -short, with race detector)
	go test ./... -count=1 -race

.PHONY: test_cover
test_cover: ## Run tests and open HTML coverage report
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out

##########################
### Lint                ###
##########################

.PHONY: go_lint
go_lint: ## Run golangci-lint
	golangci-lint run --timeout 5m

##########################
### Docker              ###
##########################

.PHONY: docker_build
docker_build: ## Build the Docker image (sage:latest)
	docker build -t $(DOCKER_IMAGE) .

.PHONY: docker_run
docker_run: ## Run the Docker image (requires CONFIG_PATH)
	docker run --rm \
		-v $(abspath $(CONFIG_PATH)):/etc/sage/config.yaml:ro \
		-p 3069:3069 \
		$(DOCKER_IMAGE) -config /etc/sage/config.yaml

##########################
### E2E Tests           ###
##########################

SAGE_URL ?= http://localhost:3069

.PHONY: e2e_test
e2e_test: ## Run E2E tests against SAGE_URL (default: http://localhost:3069)
	go test ./e2e/... -tags e2e -count=1 -timeout 120s -v

.PHONY: load_test
load_test: ## Run load tests against a running SAGE instance
	SAGE_URL=$(SAGE_URL) go test ./e2e/... -tags e2e -run TestLoad -count=1 -timeout 120s -v

.PHONY: integration_test
integration_test: ## Run integration tests (requires fullnode + SAGE_CONFIG)
	go test ./protocol/shannon/... -tags integration -count=1 -timeout 60s -v

##########################
### Clean               ###
##########################

.PHONY: clean
clean: ## Remove build artifacts and coverage reports
	rm -rf $(BIN_DIR)/ coverage.out
