#!/usr/bin/env bash
# Lint and unit-test the job shim's Python (src/internal/nixgen/shim).
# Needs uv (uvx); ruff and pytest come from uvx, so nothing is installed.
set -euo pipefail
shim="$(cd "$(dirname "$0")/../src/internal/nixgen/shim" && pwd)"
cd "$shim"
uvx --quiet ruff check --no-cache .
# -p no:cacheprovider: no .pytest_cache in the source tree; the bytecode stays
# out too, so go:embed's *.py globs never meet stray files.
PYTHONDONTWRITEBYTECODE=1 uvx --quiet pytest -q -p no:cacheprovider tests
