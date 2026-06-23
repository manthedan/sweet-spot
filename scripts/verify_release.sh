#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  :
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN=python
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  echo "ERROR: could not find python or python3; set PYTHON_BIN=/path/to/python" >&2
  exit 127
fi
TOFU_BIN="${TOFU_BIN:-tofu}"

echo "==> Python: $("$PYTHON_BIN" --version)"

"$PYTHON_BIN" - "$PYTHON_BIN" <<'PY'
import importlib.util
import sys

python_bin = sys.argv[1]
missing = [name for name in ("boto3", "botocore", "ruff", "mypy") if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit(
        "missing release-check dependency module(s): "
        + ", ".join(missing)
        + f"\nInstall them with: {python_bin} -m pip install --constraint requirements-dev.lock -e '.[dev]'"
    )
PY

echo "==> Ruff format"
"$PYTHON_BIN" -m ruff format --check .

echo "==> Ruff lint"
"$PYTHON_BIN" -m ruff check .

echo "==> mypy"
"$PYTHON_BIN" -m mypy sweetspot

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
    'outputs: type=oci,dest=/tmp/sweetspot-worker.oci,tar=false',
    'input: /tmp/sweetspot-worker.oci',
    'path: /tmp/sweetspot-worker.oci',
    'continue-on-error: true',
]
missing = [needle for needle in required if needle not in workflow]
if missing:
    raise SystemExit('missing workflow artifact invariant(s): ' + ', '.join(missing))
PY

echo "==> release verification complete"
