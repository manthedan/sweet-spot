# SweetSpot

Run thousands of independent commands on AWS Batch Spot instances, cheaply, with retries, and with proof of what finished.

You bring a script that turns an input file into an output file. SweetSpot runs it once per input on the cheapest Spot capacity you allow. It retries failures and work lost to Spot reclaims, never lets a duplicate run corrupt a result, and publishes a manifest when every task is done.

```text
tasks.jsonl ──submit──▶ SQS queue (per run) ──▶ Batch Spot workers ──▶ S3: out/<task>/..., done/<task>.json
                                                                                  │
                                         status / logs / finish ◀─────────────────┘ manifest.jsonl + READY
```

Good fit:
- embarrassingly parallel work: labeling, batch inference, dataset conversion, simulation sweeps, self-play;
- each task can be re-run safely, and reads and writes files;
- you care about the bill and are happy to use Spot.

Not a fit: DAGs or pipelines, exactly-once side effects (charging cards, sending email), untrusted user commands, or interactive distributed compute.

## Try it locally (no AWS)

```bash
uv tool install git+https://github.com/manthedan/sweet-spot.git   # or: pipx install ..., pip install ...

sweetspot tasks --count 3 -- sh -c 'echo "hello from task {i}" > {out}/hello.txt' > tasks.jsonl
sweetspot local tasks.jsonl -j 3
cat sweetspot-local/tasks/out/task-0001/hello.txt
```

`local` uses the same runner and commit protocol as the cloud workers. Use it to check a task before paying for one.

## Tasks

One JSON object per line:

```json
{"id": "shard_0007",
 "command": ["python", "/app/label.py", "--in", "{in}/shard.jsonl.gz", "--out", "{out}/labels.jsonl"],
 "inputs": {"shard.jsonl.gz": "s3://my-bucket/data/shard_0007.jsonl.gz"},
 "env": {"DEPTH": "10"},
 "timeout": 3600,
 "units": 25000}
```

| field | |
|---|---|
| `id` | unique in the run; up to 200 letters, digits, `.`, `_`, `-`, starting with a letter or digit |
| `command` | argv list, no shell unless you call one. `{in}`, `{out}`, `{id}` are replaced by the input directory, output directory and task id |
| `inputs` | files to download into `{in}` before the command runs (`s3://`, `https://` or local paths) |
| `env` | extra environment variables (names starting `AWS_` or `SWEETSPOT_` are reserved) |
| `timeout` | seconds (default 3600, max 11 h) |
| `cwd` | working directory (default: a fresh scratch directory) |
| `outputs` | glob allowlist of files under `{out}` to keep; anything else the command leaves there is ignored |
| `units` | how much work the task is (rows, games, ...), for throughput and $/unit. A command can also write `{"units": N}` to `{out}/_metrics.json` |

Exit 0 and at least one file in `{out}` means success. Everything written to `{out}` becomes the task's output.

You rarely write tasks by hand. `sweetspot tasks` expands a command template:

```bash
# one task per object under a prefix, each split into 4 row ranges
sweetspot tasks --inputs-from s3://my-bucket/data/ --match '\.jsonl\.gz$' --chunks 4 --units 25000 -- \
  python /app/label.py --in {input} --part {chunk} --parts {chunks} --out {out}/labels.jsonl > tasks.jsonl

# N tasks with no inputs
sweetspot tasks --count 1000 -- python /app/selfplay.py --seed {i} --out {out}/games.pgn > tasks.jsonl
```

Template placeholders: `{input}` (the downloaded input's path), `{name}`, `{stem}`, `{i}`, `{chunk}`, `{chunks}`.

## Running on AWS

**1. Stack (once).** `infra/opentofu` creates Batch Spot compute environments and job queues ("lanes", e.g. x86 and ARM), the worker IAM role, a log group and optionally the bucket. See [infra/opentofu/README.md](infra/opentofu/README.md).

```bash
tofu -chdir=infra/opentofu init
tofu -chdir=infra/opentofu apply -var-file=my.tfvars     # my.tfvars lives in infra/opentofu
tofu -chdir=infra/opentofu output -raw sweetspot_config > sweetspot.json
```

**2. Image.** Your code plus SweetSpot, with no `ENTRYPOINT`. The job definition runs `sweetspot worker`.

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir https://github.com/manthedan/sweet-spot/archive/refs/heads/main.tar.gz
COPY label.py /app/label.py
```

Build it for the lane's architecture, push it to ECR, and pin it by digest.

**3. Run.**

```bash
sweetspot submit labels-v1 tasks.jsonl --image 123456789012.dkr.ecr.us-west-2.amazonaws.com/label@sha256:... \
  --vcpus 1 --memory 1536 --workers 200
sweetspot status labels-v1            # progress, failures, ETA, and the next command to run
sweetspot logs labels-v1 shard_0007   # every attempt of a task, and the latest one's stdout/stderr
sweetspot finish labels-v1            # verify, publish manifest.jsonl + READY, delete queues
aws s3 sync s3://my-bucket/sweetspot/labels-v1/out/ ./labels/
```

Run names are lowercase letters, digits and single hyphens. `submit` is safe to repeat. On an existing run it adds workers (`--workers 50`), resumes after `cancel`, and, with `--retry`, sends tasks that failed every attempt back to the queue once you have fixed the cause. A run's task list is fixed when you create it; a different list needs a new run name. If you change the image mid-run, `submit` and `finish` warn you: outputs then come from more than one image, so start a new run when the change affects results.

GPU work: add a lane with `gpu = true` and GPU instance types to the stack, then `submit --lane gpu --gpus 1`.

You can also lend your own machine to a cloud run: `sweetspot worker --run labels-v1`.

## Choosing instance types

Throughput differs by instance family far more than Spot prices suggest. In the Tiny Leela Stockfish canaries ([case study](docs/case_studies/tiny_leela_sf18.md)), `c7g.medium` worked out to about $0.26 per 10M positions and `c6g.medium` to about $0.67. Measure, don't guess:

1. Submit a small run (a few dozen tasks) on a lane that allows several instance types.
2. `sweetspot scout --run small-run` combines each instance type's measured units/s with current Spot prices into $ per million units. Without `--run` it shows $ per worker-hour only.
3. Narrow the lane's `instance_types` to the winners and submit the real run.

## How it stays correct

Each run has its own SQS queue and dead-letter queue, and its state lives under `s3://<prefix>/<run>/`:

```text
run.json  tasks.jsonl          identity: the task list's sha256 (created once, never rewritten)
done/<id>.json                 done marker: which attempt won, its files, sizes and sha256s
out/<id>/...                   outputs
attempts/<id>/<attempt>/       per-attempt summary.json, stdout.txt, stderr.txt (and staged outputs)
failures/<id>/<attempt>.json   one record per failed attempt
manifest.jsonl  READY          published by `finish`
```

- A worker holds a task by renewing its SQS lease while it runs. If the host dies, the task reappears within 5 minutes.
- Outputs are uploaded to the attempt's own prefix first. The done marker is then created with an S3 conditional write, so exactly one attempt wins. Only the winner's files are copied to `out/<id>/`; a duplicate delivery can never mix two attempts' files.
- A failed task backs off and retries. After `--max-attempts` deliveries (default 5) it goes to the DLQ and `status` tells you how to look at it and retry.
- On a Spot interruption notice or SIGTERM the worker kills the task, hands it straight back to the queue, and exits so Batch replaces it.
- Array child 0 stays until no task is in flight anywhere, so retries are never stranded after the other workers leave.
- `finish` checks every marker against the ledger, repairs any interrupted copy into `out/`, and publishes `manifest.jsonl` and `READY` with conditional writes. Outputs are final once `READY` exists and are never rewritten after it. `finish --verify-hashes` also reads every output back against its recorded sha256.

Execution is at-least-once: a task may run more than once, but only one result is kept. Task commands are trusted code with the worker role's permissions.

## Commands

| | |
|---|---|
| `sweetspot tasks` | generate tasks.jsonl from a command template |
| `sweetspot local` | run tasks on this machine |
| `sweetspot submit` | create, resume, scale or retry a run |
| `sweetspot status` | progress, failures, workers, ETA, next step (`--watch`, `--json`) |
| `sweetspot logs` | attempts and output of one task |
| `sweetspot finish` | verify and publish; release queues and job definitions |
| `sweetspot cancel` | stop workers and delete queues; outputs stay, `submit` resumes |
| `sweetspot scout` | Spot prices and measured $/unit by instance type |
| `sweetspot worker` | the worker loop (container entrypoint, or your own machine) |

## Example

[examples/stockfish](examples/stockfish): label chess positions with Stockfish, from a local trial to a cloud run.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
ruff check . && mypy sweetspot && python -m unittest discover -s tests -t .
```

The tests run against [moto](https://github.com/getmoto/moto), so they need no AWS account.

Apache-2.0.
