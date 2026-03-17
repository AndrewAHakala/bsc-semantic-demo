#!/usr/bin/env bash
# Loads .env vars and runs dbt commands in the dbt/ directory.
# Usage: ./scripts/run_dbt.sh build    (or deps, test, run, compile, etc.)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Export .env vars into the shell so dbt's env_var() can read them
set -a
source "$ROOT/.env"
set +a

cd "$ROOT/dbt"
dbt "$@" --profiles-dir .
