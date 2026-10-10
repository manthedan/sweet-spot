# Example: Stockfish position labels

`label.py` adds Stockfish evaluations to a JSONL file of chess positions (`{"fen": ...}` per line). It uses only the
standard library and one Stockfish process with `Threads=1`, so one worker = one vCPU. Each output row keeps the
input row and adds `stockfish.bestmove` and `stockfish.pvs` (rank, depth, `{"cp": N}` or `{"mate": N}` from the
side to move, and the PV). It writes the number of rows it labeled to `{out}/_metrics.json`, which `scout` uses.

`sample.jsonl` has 36 positions to try it on.

## 1. Build the image

From the repository root:

```bash
docker build --platform linux/amd64 -f examples/stockfish/Dockerfile -t sweetspot-stockfish .
docker run --rm sweetspot-stockfish python /work/label.py --input /work/sample.jsonl --out /tmp/l.jsonl --depth 6
```

The image has Stockfish 18 (release binary on amd64, built from source with `ARCH=armv8` on arm64), SweetSpot,
and no `ENTRYPOINT`. Stockfish's GPL license and sources are kept under `/opt/stockfish-release`.

## 2. Try it locally

Split the sample into three 12-row tasks and run them inside the image, with no AWS:

```bash
mkdir -p results
docker run --rm -v "$PWD/results:/results" sweetspot-stockfish sh -eu -c '
  echo file:///work/sample.jsonl > /tmp/inputs.txt
  sweetspot tasks --inputs /tmp/inputs.txt --chunks 3 --outputs labels.jsonl -- \
    python /work/label.py --input {input} --out {out}/labels.jsonl --chunk {chunk} --rows 12 --depth 6 --multipv 2 \
    > /results/tasks.jsonl
  sweetspot local /results/tasks.jsonl --prefix /results/local -j 3'
ls results/local/out/*/
```

`--chunks 3` makes three tasks per input with `{chunk}` = 0, 1, 2; `label.py --chunk N --rows K` labels rows
`N*K` to `N*K+K-1`. Pick `chunks = ceil(rows_per_shard / rows)` and a `--rows` that keeps each task to a few
minutes: short tasks lose little work to a Spot reclaim and never approach the timeout.

## 3. Run it on AWS

With the stack applied and `sweetspot.json` written (see [infra/opentofu](../../infra/opentofu/README.md)), push the
image to ECR, upload your shards, and give the workers read access to them (`extra_input_uris`). Then:

```bash
aws s3 ls s3://my-bucket/positions/ | awk '{print "s3://my-bucket/positions/" $4}' > shards.txt
sweetspot tasks --inputs shards.txt --chunks 10 --units 25000 --outputs labels.jsonl -- \
  python /work/label.py --input {input} --out {out}/labels.jsonl --chunk {chunk} --rows 25000 --depth 10 > tasks.jsonl

# a small measuring run first: which instance type is cheapest per position?
head -40 tasks.jsonl > probe.jsonl
sweetspot submit sf-probe probe.jsonl --image ACCOUNT.dkr.ecr.REGION.amazonaws.com/sweetspot-stockfish@sha256:... --workers 20
sweetspot status sf-probe
sweetspot scout --run sf-probe

# then the real thing
sweetspot submit sf-d10 tasks.jsonl --image ...@sha256:... --workers 200
sweetspot status sf-d10
sweetspot finish sf-d10
aws s3 sync s3://my-bucket/sweetspot/sf-d10/out/ labels/
```

## Tiny Leela's own labeler

`scripts/root_stockfish_label_parallel.py` in Tiny Leela runs unchanged. It starts its helper by a relative
`scripts/...` path and leaves part files next to its output, so set the working directory and keep only the result:

```bash
sweetspot tasks --inputs shards.txt --cwd /work --outputs labels.jsonl.zst -- \
  python /work/scripts/root_stockfish_label_parallel.py --input {input} --out {out}/labels.jsonl.zst \
  --workers 1 --depth 10 --multipv 2 --stockfish /usr/local/bin/stockfish > tasks.jsonl
```

Its image needs those scripts, their dependencies (`python-chess`, the `zstd` binary) and Stockfish. It doesn't
write `_metrics.json`; pass `--units N` with the real rows per task if you want `scout` to price it. For several
Stockfish processes per task, raise `--vcpus` and `--memory` to match.
