# Contributing

```bash
python -m venv .venv && . .venv/bin/activate
pip install --constraint requirements-dev.lock -e '.[dev]'
ruff format . && ruff check . && mypy sweetspot && python -m unittest discover -s tests -t .
cd infra/opentofu && tofu fmt -check && tofu init -backend=false && tofu validate
```

Tests use moto, so they never touch AWS. Before changing the commit protocol (`sweetspot/protocol.py`) or the
worker loop, read docs/design.md and add a test for the crash or duplicate-delivery case you are changing.

Keep the surface small: nine commands, one task format. A new flag or command needs a user who hit the problem.
