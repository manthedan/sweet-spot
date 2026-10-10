#!/usr/bin/env python3
"""Dependency-free, single-threaded Stockfish UCI labels for a JSONL row range."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import subprocess
from itertools import islice
from pathlib import Path
from typing import Any, Generator, Iterator


class Engine:
    def __init__(self, executable: str, multipv: int) -> None:
        self.proc = subprocess.Popen([executable], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8", bufsize=1)
        try:
            self.send("uci")
            self.until("uciok")
            self.send("setoption name Threads value 1")
            self.send(f"setoption name MultiPV value {multipv}")
            self.ready()
        except BaseException:
            self.close()
            raise

    def send(self, command: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    def lines(self) -> Iterator[str]:
        assert self.proc.stdout is not None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"Stockfish closed its UCI output (exit {self.proc.poll()})")
            yield line.strip()

    def until(self, expected: str) -> None:
        for line in self.lines():
            if line == expected:
                return

    def ready(self) -> None:
        self.send("isready")
        self.until("readyok")

    def label(self, fen: str, depth: int, multipv: int) -> dict[str, Any]:
        if not isinstance(fen, str) or "\n" in fen or "\r" in fen or len(fen.split()) != 6:
            raise ValueError("fen must be a single-line, six-field FEN")
        # Reset the hash so labels do not depend on which rows happened to share a worker.
        self.send("ucinewgame")
        self.ready()
        self.send("position fen " + fen)
        self.send(f"go depth {depth}")
        pvs: dict[int, dict[str, Any]] = {}
        for line in self.lines():
            words = line.split()
            if words and words[0] == "bestmove":
                move = words[1] if len(words) > 1 else "(none)"
                return {"depth": depth, "multipv": multipv, "bestmove": None if move in {"(none)", "0000"} else move, "pvs": [pvs[k] for k in sorted(pvs)]}
            if not words or words[0] != "info" or "string" in words or "score" not in words or "pv" not in words:
                continue
            try:
                rank = int(words[words.index("multipv") + 1]) if "multipv" in words else 1
                score_at = words.index("score")
                kind, value = words[score_at + 1], int(words[score_at + 2])
                if kind not in {"cp", "mate"}:
                    continue
                pv = words[words.index("pv") + 1 :]
                item = {"rank": rank, "depth": int(words[words.index("depth") + 1]), "score": {kind: value}, "bestmove": pv[0] if pv else None, "pv": pv}
                if "lowerbound" in words or "upperbound" in words:
                    item["bound"] = "lower" if "lowerbound" in words else "upper"
                if item["depth"] >= pvs.get(rank, {}).get("depth", -1):
                    pvs[rank] = item
            except (ValueError, IndexError):
                continue  # Other optional UCI info lines are not labels.
        raise RuntimeError("Stockfish did not return bestmove")

    def close(self) -> None:
        try:
            if self.proc.poll() is None:
                try:
                    self.send("quit")
                except (BrokenPipeError, OSError):
                    pass
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait()
        finally:
            if self.proc.stdin:
                self.proc.stdin.close()
            if self.proc.stdout:
                self.proc.stdout.close()


def rows(path: Path) -> Generator[str, None, None]:
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as source:
        yield from (line for line in source if line.strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--row-start", type=int, default=0, help="Zero-based nonblank JSONL row")
    parser.add_argument("--rows", type=int, help="Number of rows; default is all remaining rows")
    parser.add_argument("--chunk", type=int, default=0, help="Adds chunk * rows to row-start (for sweetspot tasks --chunks)")
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--multipv", type=int, default=1)
    parser.add_argument("--stockfish", default=os.environ.get("STOCKFISH_BIN", "stockfish"))
    args = parser.parse_args(argv)
    if args.row_start < 0 or args.chunk < 0 or args.rows is not None and args.rows < 0 or args.depth < 1 or args.multipv < 1:
        parser.error("row ranges must be nonnegative; depth and multipv must be positive")
    if args.chunk and args.rows is None:
        parser.error("--chunk requires --rows")
    if not args.input.name.endswith((".jsonl", ".jsonl.gz")):
        parser.error("--input must be .jsonl or .jsonl.gz")
    if args.input.resolve() == args.out.resolve():
        parser.error("input and output must be different files")
    start = args.row_start + args.chunk * (args.rows or 0)
    source = rows(args.input)
    selected = islice(source, start, None if args.rows is None else start + args.rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    engine = Engine(args.stockfish, args.multipv)
    count = 0
    try:
        with args.out.open("w", encoding="utf-8") as out:
            for index, line in enumerate(selected, start=start):
                row = json.loads(line)
                if not isinstance(row, dict) or "fen" not in row:
                    raise ValueError(f"row {index}: expected a JSON object with fen")
                row["stockfish"] = engine.label(row["fen"], args.depth, args.multipv)
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
                count += 1
    finally:
        source.close()
        engine.close()
    if os.environ.get("SWEETSPOT_OUT"):
        metrics = Path(os.environ["SWEETSPOT_OUT"]) / "_metrics.json"
        metrics.parent.mkdir(parents=True, exist_ok=True)
        metrics.write_text(json.dumps({"units": count}) + "\n", encoding="utf-8")
    print(f"labeled {count} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
