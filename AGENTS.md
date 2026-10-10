# Notes for coding agents

SweetSpot runs a tasks.jsonl file on AWS Batch Spot. Read README.md first; it is the whole user interface.

## Operating runs

- Try tasks locally before spending money: `sweetspot local tasks.jsonl --limit 2`.
- `sweetspot submit RUN tasks.jsonl --dry-run` shows what would happen. Dropping `--dry-run` creates AWS resources and starts paid workers: get the user's go-ahead first, including the worker count and image.
- Don't poll in the foreground for long runs. Check `sweetspot status RUN` (or `--json`) when asked or on a schedule; its `next` field says what to do.
- `finish` publishes READY and deletes the run's queues; `cancel` terminates workers. Both are user decisions.
- Task commands run with the worker role's AWS permissions. Never put untrusted input into a task command.

## Changing the code

```bash
ruff check . && ruff format --check . && mypy sweetspot && python -m unittest discover -s tests -t .
```

- The tests use moto; nothing touches a real AWS account.
- `sweetspot/protocol.py` is the correctness core (done markers, promotion). A change there needs a test that exercises duplicate attempts or crashes at the step you changed.
- Keep the CLI small. Prefer making `submit`, `status` and `finish` smarter over adding commands.
