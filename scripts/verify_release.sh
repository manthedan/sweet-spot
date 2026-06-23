#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python}"
TOFU_BIN="${TOFU_BIN:-tofu}"

echo "==> Python: $($PYTHON_BIN --version)"

echo "==> Ruff format"
"$PYTHON_BIN" -m ruff format --check .

echo "==> Ruff lint"
"$PYTHON_BIN" -m ruff check .

echo "==> mypy"
"$PYTHON_BIN" -m mypy spotbatch

echo "==> unit tests"
"$PYTHON_BIN" -m unittest discover -s tests -v

if command -v "$TOFU_BIN" >/dev/null 2>&1; then
  echo "==> OpenTofu fmt/init/validate"
  (
    cd infra/opentofu
    "$TOFU_BIN" fmt -check -recursive .
    "$TOFU_BIN" init -backend=false -input=false -lockfile=readonly -no-color
    "$TOFU_BIN" validate -no-color
  )
  git diff --exit-code -- infra/opentofu/.terraform.lock.hcl
else
  echo "WARN: $TOFU_BIN not found; skipping OpenTofu checks" >&2
fi

echo "==> workflow artifact path consistency"
"$PYTHON_BIN" - <<'PY'
from pathlib import Path
workflow = Path('.github/workflows/ci.yml').read_text()
required = [
    'outputs: type=oci,dest=/tmp/spotbatch-worker.oci.tar',
    'input: /tmp/spotbatch-worker.oci.tar',
    'path: /tmp/spotbatch-worker.oci.tar',
]
missing = [needle for needle in required if needle not in workflow]
if missing:
    raise SystemExit('missing workflow artifact invariant(s): ' + ', '.join(missing))
PY

echo "==> release verification complete"
