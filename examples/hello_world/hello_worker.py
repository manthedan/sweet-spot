#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path


task = json.loads(Path(os.environ["SWEETSPOT_TASK_JSON"]).read_text())
out = Path(os.environ["SWEETSPOT_OUTPUT_PATH"])
out.write_text(
    json.dumps(
        {
            "hello": "world",
            "run_id": os.environ.get("SWEETSPOT_RUN_ID"),
            "task_id": os.environ.get("SWEETSPOT_TASK_ID"),
            "payload": task.get("payload", {}),
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
print(f"wrote {out}")
