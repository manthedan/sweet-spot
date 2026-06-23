# Stockfish annotation example

This directory is reserved for a sanitized example showing how a chess-specific Stockfish annotation command can run on top of the generic framework.

Keep project/private details out of this example:

- no personal bucket names
- no account IDs
- no local filesystem paths
- no private model/data paths

The generic task shape should remain:

```json
{
  "schema": "sweetspot.task.v1",
  "run_id": "stockfish-demo",
  "task_id": "shard_0000.rows0_25000.sf_d10_mpv2",
  "command": ["python", "/app/stockfish_task.py"],
  "input_s3": "s3://YOUR_BUCKET/input/shard_0000.jsonl.zst",
  "output_s3": "s3://YOUR_BUCKET/runs/stockfish-demo/shards/shard_0000.rows0_25000.jsonl.zst",
  "summary_s3": "s3://YOUR_BUCKET/runs/stockfish-demo/summaries/shard_0000.rows0_25000.summary.json",
  "done_s3": "s3://YOUR_BUCKET/runs/stockfish-demo/done/shard_0000.rows0_25000.done.json",
  "row_start": 0,
  "max_rows": 25000,
  "depth": 10,
  "multipv": 2
}
```
