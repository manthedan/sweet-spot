"""Offline UCI protocol and row-range tests; no chess library or real engine required."""

import contextlib
import gzip
import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("stockfish_label", Path(__file__).with_name("label.py"))
label = importlib.util.module_from_spec(spec)
spec.loader.exec_module(label)

FAKE_ENGINE = """#!/usr/bin/env python3
import os, sys
for line in sys.stdin:
    command = line.strip()
    with open(os.environ['UCI_LOG'], 'a') as log:
        log.write(command + '\\n')
    if command == 'uci':
        print('id name fake', flush=True)
        print('uciok', flush=True)
    elif command == 'isready':
        print('readyok', flush=True)
    elif command.startswith('go '):
        print('info string ignore score cp 999 pv nonsense', flush=True)
        print('info depth 1 multipv 1 score cp 99 pv d2d4', flush=True)
        print('info depth 4 multipv 1 score cp 25 pv e2e4 e7e5', flush=True)
        print('info depth 4 multipv 2 score mate -3 pv g1f3', flush=True)
        print('info depth 2 multipv 1 score cp 1000 pv a2a3', flush=True)
        print('bestmove e2e4 ponder e7e5', flush=True)
    elif command == 'quit':
        break
"""


class LabelTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.engine = self.root / "stockfish"
        self.engine.write_text(FAKE_ENGINE)
        self.engine.chmod(0o755)
        self.log = self.root / "uci.log"
        self.source = self.root / "input.jsonl"
        self.source.write_text("\n".join(json.dumps({"id": i, "fen": "8/8/8/3k4/8/4K3/8/8 w - - 0 1"}) for i in range(8)) + "\n")
        self.out = self.root / "out" / "labels.jsonl"

    def run_label(self, *flags):
        with patch.dict(os.environ, {"UCI_LOG": str(self.log), "SWEETSPOT_OUT": str(self.out.parent)}), contextlib.redirect_stdout(io.StringIO()):
            rc = label.main(["--input", str(self.source), "--out", str(self.out), "--stockfish", str(self.engine), "--depth", "4", "--multipv", "2", *flags])
        self.assertEqual(rc, 0)
        return [json.loads(line) for line in self.out.read_text().splitlines()]

    def test_scores_latest_depth_pv_and_one_thread_engine(self):
        rows = self.run_label("--row-start", "1", "--rows", "3")
        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        result = rows[0]["stockfish"]
        self.assertEqual(result["bestmove"], "e2e4")
        self.assertEqual(result["pvs"][0]["score"], {"cp": 25})
        self.assertEqual(result["pvs"][0]["pv"], ["e2e4", "e7e5"])
        self.assertEqual(result["pvs"][1]["score"], {"mate": -3})
        self.assertEqual(result["pvs"][1]["bestmove"], "g1f3")
        commands = self.log.read_text().splitlines()
        self.assertEqual(commands.count("uci"), 1)
        self.assertEqual(commands.count("setoption name Threads value 1"), 1)
        self.assertEqual(commands.count("setoption name MultiPV value 2"), 1)
        self.assertEqual(commands.count("ucinewgame"), 3)
        self.assertEqual(commands[-1], "quit")
        self.assertEqual(json.loads((self.out.parent / "_metrics.json").read_text()), {"units": 3})

    def test_gzip_chunk_ranges_and_short_last_chunk(self):
        compressed = self.source.with_suffix(".jsonl.gz")
        with gzip.open(compressed, "wt") as f:
            f.write(self.source.read_text())
        self.source = compressed
        self.assertEqual([row["id"] for row in self.run_label("--chunk", "2", "--rows", "3")], [6, 7])
        self.assertEqual(json.loads((self.out.parent / "_metrics.json").read_text()), {"units": 2})

    def test_empty_range_has_zero_units(self):
        self.assertEqual(self.run_label("--row-start", "100", "--rows", "3"), [])
        self.assertEqual(json.loads((self.out.parent / "_metrics.json").read_text()), {"units": 0})

    def test_bad_rows_and_engine_eof_fail_without_metrics(self):
        self.source.write_text('{"not_fen": 1}\n')
        with self.assertRaisesRegex(ValueError, "row 0"):
            self.run_label()
        self.assertFalse((self.out.parent / "_metrics.json").exists())
        self.engine.write_text("#!/usr/bin/env python3\n")
        with self.assertRaises((RuntimeError, BrokenPipeError)):
            self.run_label()

    def test_fen_cannot_inject_uci_commands(self):
        self.source.write_text(json.dumps({"fen": "8/8/8/3k4/8/4K3/8/8 w - - 0 1\nquit"}) + "\n")
        with self.assertRaisesRegex(ValueError, "single-line"):
            self.run_label()

    def test_argument_errors(self):
        for flags in [("--chunk", "1"), ("--rows", "-1"), ("--depth", "0"), ("--multipv", "0"), ("--out", str(self.source))]:
            with self.subTest(flags=flags), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.run_label(*flags)


if __name__ == "__main__":
    unittest.main()
