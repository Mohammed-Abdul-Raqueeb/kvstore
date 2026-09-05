# kvstore — build, test and benchmark targets.
#
# Everything here works on Linux and macOS. Windows users: the Go commands
# work unchanged (use `go build ./...` etc.); only the shell targets that
# chain commands together need adapting. See README.md.

GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"
BIN     := bin
DATA    ?= ./data

.PHONY: all build clean fmt vet lint test test-short test-race test-crash \
        test-chaos test-diff test-cluster fuzz fuzz-long bench bench-suite \
        run run-replica cover tidy check ci

all: build

build: ## Build kvserver, kvctl and kvbench into ./bin
	@mkdir -p $(BIN)
	$(GO) build $(LDFLAGS) -o $(BIN)/ ./cmd/...
	@echo "built $(VERSION) -> $(BIN)/"

clean: ## Remove build artefacts and test data
	rm -rf $(BIN) coverage.out coverage.html bench-results.csv
	$(GO) clean -testcache

fmt: ## Format all source
	gofmt -w .

vet: ## Run go vet
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

# --- tests -----------------------------------------------------------------

test: ## Full test suite (several minutes)
	$(GO) test -timeout 20m ./...

test-short: ## Fast subset, suitable for a pre-commit hook
	$(GO) test -short -timeout 5m ./...

test-race: ## Full suite under the race detector
	$(GO) test -race -timeout 30m ./...

test-crash: ## Crash-recovery suite only (spawns real processes and SIGKILLs them)
	$(GO) test -v -timeout 15m ./test/crash/

test-chaos: ## Adversarial / malformed-input suite
	$(GO) test -v -timeout 10m ./test/chaos/

test-diff: ## Model-based differential suite
	$(GO) test -v -timeout 10m ./test/differential/

test-cluster: ## Replication suite (Phase 2)
	$(GO) test -v -timeout 10m ./test/cluster/

cover: ## Coverage report -> coverage.html
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./internal/...
	$(GO) tool cover -html=coverage.out -o coverage.html
	$(GO) tool cover -func=coverage.out | tail -1

# --- fuzzing ---------------------------------------------------------------

fuzz: ## Fuzz the protocol decoders for 30s each
	$(GO) test -run=xxx -fuzz=FuzzDecodeFrame   -fuzztime=30s ./internal/protocol/
	$(GO) test -run=xxx -fuzz=FuzzDecodeCommand -fuzztime=30s ./internal/protocol/
	$(GO) test -run=xxx -fuzz=FuzzDecodeResponse -fuzztime=30s ./internal/protocol/

fuzz-long: ## Fuzz for 10 minutes each (run before claiming the parser is solid)
	$(GO) test -run=xxx -fuzz=FuzzDecodeFrame   -fuzztime=10m ./internal/protocol/
	$(GO) test -run=xxx -fuzz=FuzzDecodeCommand -fuzztime=10m ./internal/protocol/

# --- benchmarks ------------------------------------------------------------

bench: ## Go micro-benchmarks
	$(GO) test -bench=. -benchmem -run=xxx ./internal/...

bench-suite: build ## End-to-end load sweep -> bench-results.csv (needs a running server)
	@echo "Sweeping connection counts (closed loop)..."
	@for n in 1 2 4 8 16 32 64 128; do \
		$(BIN)/kvbench --conns $$n --duration 10s --warmup 3s --workload 95/5 \
			--csv bench-results.csv --label "closed-conns-$$n" --preload=false | grep throughput; \
	done
	@echo "Sweeping offered load (open loop, immune to coordinated omission)..."
	@for r in 10000 25000 50000 100000; do \
		$(BIN)/kvbench --mode open --rate $$r --conns 64 --duration 10s --warmup 3s \
			--csv bench-results.csv --label "open-rate-$$r" --preload=false | grep -E "throughput|latency"; \
	done
	@echo "results in bench-results.csv"

# --- running ---------------------------------------------------------------

run: build ## Run a server on :7379 with sensible development settings
	$(BIN)/kvserver --addr 127.0.0.1:7379 --data-dir $(DATA) \
		--fsync everysec --text-addr 127.0.0.1:7380 --log-level info

run-replica: build ## Run a replica of a local primary on :7381
	$(BIN)/kvserver --addr 127.0.0.1:7381 --data-dir $(DATA)-replica \
		--replicaof 127.0.0.1:7379 --log-level info

# --- composite -------------------------------------------------------------

check: fmt vet test-short ## Format, vet and run the fast tests

ci: vet test-race fuzz ## What CI should run

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
