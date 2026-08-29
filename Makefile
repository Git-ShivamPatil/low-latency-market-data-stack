# Entry points for both toolchains. Everything here is meant to run inside
# WSL2 Ubuntu (or any Linux); the multicast socket options, recvmmsg,
# SO_REUSEPORT, core pinning and rdtsc timing that later milestones need only
# behave correctly on Linux, so the C++ build is not wired for Windows.

SHELL := /bin/bash
.DEFAULT_GOAL := help

CPP_BUILD ?= cpp/build
CMAKE_BUILD_TYPE ?= RelWithDebInfo
CARGO ?= cargo
PYTHON ?= python3

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- codegen ---------------------------------------------------------------

.PHONY: codegen
codegen: ## Regenerate the Rust codec and C++ header from schema/market-data.xml
	$(PYTHON) schema/codegen.py

.PHONY: goldens
goldens: ## Rebuild the golden byte vectors and the generated assertions
	$(PYTHON) schema/goldens.py

.PHONY: generate
generate: codegen goldens ## Run every generator

.PHONY: check-generated
check-generated: ## Fail if any generated file is stale (what CI runs)
	$(PYTHON) schema/codegen.py --check
	$(PYTHON) schema/goldens.py --check

# --- build -----------------------------------------------------------------

.PHONY: build
build: build-rust build-cpp ## Build both toolchains

.PHONY: build-rust
build-rust: ## cargo build the workspace
	$(CARGO) build --workspace --all-targets

.PHONY: build-cpp
build-cpp: $(CPP_BUILD)/CMakeCache.txt ## Build the C++ tree
	cmake --build $(CPP_BUILD)

$(CPP_BUILD)/CMakeCache.txt:
	cmake -S cpp -B $(CPP_BUILD) -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)

# --- test ------------------------------------------------------------------

.PHONY: test
test: test-rust test-cpp test-corruption smoke ## Run every correctness suite

.PHONY: test-rust
test-rust: ## cargo test the workspace
	$(CARGO) test --workspace

.PHONY: test-cpp
test-cpp: build-cpp ## ctest the C++ tree
	ctest --test-dir $(CPP_BUILD) --output-on-failure

.PHONY: test-corruption
test-corruption: ## Prove a one-byte edit to a golden vector fails both suites
	CPP_BUILD=$(CPP_BUILD) scripts/verify-golden-corruption.sh

.PHONY: smoke
smoke: ## End-to-end: engine and handler as separate processes, books reconciled
	scripts/smoke.sh

.PHONY: run-engine
run-engine: ## The engine, exactly as the case study runs it
	cargo run --release --bin matching-engine -- --config configs/local.toml

.PHONY: run-handler
run-handler: ## The handler, exactly as the case study runs it
	cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001

# --- quality ---------------------------------------------------------------

.PHONY: fmt
fmt: ## Format the Rust tree
	$(CARGO) fmt --all

.PHONY: lint
lint: ## Formatting and clippy, both as errors
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: ci
ci: check-generated lint test ## Everything CI runs, in CI's order

# --- housekeeping ----------------------------------------------------------

.PHONY: clean
clean: ## Remove build output from both toolchains
	$(CARGO) clean
	rm -rf $(CPP_BUILD)

# Benchmarks are deliberately not a CI target and deliberately not part of
# `make test`. They must never run on a shared runner: numbers from a noisy
# virtualised host would discredit the honest ones.

.PHONY: hostcheck
hostcheck: ## Say whether this machine may publish a performance number
	$(CARGO) run --release -p bench --bin hostcheck

.PHONY: bench
bench: ## Run the benchmark harness behind the host gate
	scripts/bench.sh all

.PHONY: bench-micro
bench-micro: ## Criterion microbenchmarks only
	scripts/bench.sh micro
