set shell := ["bash", "-c"]
# Default recipe to display help
default:
    @just --list

# Build the project
build:
    cargo build

run:
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    cargo run --bin streamling

sweep:
    cargo sweep -s
    cargo build
    cargo sweep -f

check:
    cargo check

# Cleans cargo cache and git cache
clean:
    cargo cache --autoclean 
    @echo "Cleaning incremental build caches..."
    @bash -lc 'for d in target/debug/incremental target/release/incremental; do if [ -d "$d" ]; then size=$(du -sh "$d" 2>/dev/null | cut -f1); rm -rf "$d"; echo "Deleted $d - $size"; fi; done'

# cleans targets as well
clean-all:
    cargo cache --autoclean
    cargo clean

# Run linting checks (formatting and clippy)
lint:
    cargo fmt
    cargo clippy --all-targets --all-features -- -D warnings --A clippy::result_large_err

# Fix cargo issues
fix:
    cargo fix --allow-dirty --allow-staged

# Run as much test as possible
test:
    cargo test --tests --no-fail-fast

# cargo update all dependencies (including plugins)
update-dependencies:
    cargo update
    cd plugin_examples/basic && cargo update
    cd plugin_examples/low_level && cargo update

# Run postgres e2e tests
[group('test')]
test-pg $RUST_LOG="error,streamling=debug,datafusion_table_providers=debug":
    cargo test test_kafka_to_postgres_sink -- --exact

# If you running into linker issue locally with cargo nextest, add the following to your shell
# `export DYLD_LIBRARY_PATH="$HOME/.rustup/toolchains/1.89.0-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:$DYLD_LIBRARY_PATH"`
# Run postgres e2e tests with nextest
[group('test')]
nextest-pg:
    cargo nextest run -E 'binary_id(streamling::pipeline_postgres_sink)' -P ci --no-capture

# ============================================================================
# Local Dev Environment (k3s-based)
# ============================================================================

# Setup k3s cluster with PostgreSQL, Kafka, ClickHouse, Prometheus
[group('env')]
env-setup:
    ./scripts/k3s-setup.sh
    ./scripts/init-redpanda.sh

# Check k3s cluster status
[group('env')]
env-status:
    ./scripts/k3s-status.sh

# Teardown k3s cluster
[group('env')]
env-teardown:
    ./scripts/k3s-teardown.sh

# Print environment variables (use: eval $(just env-vars))
[group('env')]
env-vars:
    @./scripts/k3s-setup.sh --env-only

# Clean up orphaned test resources (databases, topics)
[group('env')]
env-cleanup-orphans:
    ./scripts/k3s-cleanup-orphans.sh

# ============================================================================
# E2E Tests (requires env-setup first)
# ============================================================================

# Build streamling binary for e2e tests
# Set PROFILE=debug for debug builds (default: release)
[group('e2e')]
e2e-build:
    #!/usr/bin/env bash
    set -euo pipefail
    profile="${PROFILE:-release}"
    if [[ "$profile" == "release" ]]; then
        cargo build --release -p streamling
    else
        cargo build -p streamling
    fi

# Run all e2e tests with nextest (builds binary first, auto-sources env vars)
# Streamling output is shown by default. Set E2E_SHOW_STREAMLING_OUTPUT=0 to disable.
# Set PROFILE=debug for debug builds (default: release)
[group('e2e')]
e2e-test *ARGS: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    profile="${PROFILE:-release}"
    export E2E_STREAMLING_BIN="$(pwd)/target/${profile}/streamling"
    cargo nextest run -p streamling-e2e {{ARGS}}

# Run v2 ethereum dataset e2e tests with one thread for deterministic block_number assertions
[group('e2e')]
e2e-test-v2: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    profile="${PROFILE:-release}"
    export E2E_STREAMLING_BIN="$(pwd)/target/${profile}/streamling"
    cargo test -p streamling-e2e test_ethereum_source_v2 -- --test-threads=1

# Run e2e tests sequentially for debugging (builds binary first, auto-sources env vars)
# Streamling output is shown by default. Set E2E_SHOW_STREAMLING_OUTPUT=0 to disable.
# Set PROFILE=debug for debug builds (default: release)
[group('e2e')]
e2e-test-debug *ARGS: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    profile="${PROFILE:-release}"
    export E2E_STREAMLING_BIN="$(pwd)/target/${profile}/streamling"
    export E2E_SHOW_STREAMLING_OUTPUT=1
    cargo nextest run -p streamling-e2e --no-capture {{ARGS}}

# List available e2e tests
[group('e2e')]
e2e-list:
    cargo nextest list -p streamling-e2e

# Inspect Kafka topics and database tables for a specific test UUID
# Usage: just e2e-test-inspect test_d9b2cb44
[group('e2e')]
e2e-test-inspect *TEST_UUID:
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    cargo run --bin streamling-e2e-inspect -- {{TEST_UUID}}

# ============================================================================
# Benchmarks (requires env-setup first)
# ============================================================================

# Run the end-to-end throughput benchmark (report-only): builds the release
# binary, preloads Kafka, runs kafka -> sql -> blackhole, and compares each
# scenario to its committed baseline. Pass extra flags, e.g.
#   just bench --records 500000 --iterations 3
[group('bench')]
bench *ARGS: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    export E2E_STREAMLING_BIN="$(pwd)/target/release/streamling"
    export E2E_SHOW_STREAMLING_OUTPUT="${E2E_SHOW_STREAMLING_OUTPUT:-0}"
    export BENCH_RUNNER_LABEL="${BENCH_RUNNER_LABEL:-local}"
    cargo run --release -p streamling-bench -- {{ARGS}}

# Re-seed benchmark baselines from a fresh run (writes bench/baselines/<runner>/).
[group('bench')]
bench-update-baseline *ARGS: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(./scripts/k3s-setup.sh --env-only)"
    export E2E_STREAMLING_BIN="$(pwd)/target/release/streamling"
    export E2E_SHOW_STREAMLING_OUTPUT="${E2E_SHOW_STREAMLING_OUTPUT:-0}"
    export BENCH_RUNNER_LABEL="${BENCH_RUNNER_LABEL:-local}"
    cargo run --release -p streamling-bench -- --update-baseline {{ARGS}}
