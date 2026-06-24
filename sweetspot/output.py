from __future__ import annotations

import json
from typing import Any, Iterable


def format_table_value(value: Any) -> str:
    """Return a terminal-safe single-cell representation for table output."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, sort_keys=True)
    out: list[str] = []
    for ch in str(value):
        code = ord(ch)
        if ch == "\t":
            out.append("\\t")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif code < 32 or code == 127:
            out.append(f"\\x{code:02x}")
        elif 128 <= code <= 159:
            out.append(f"\\u{code:04x}")
        else:
            out.append(ch)
    return "".join(out)


def print_table(title: str, headers: list[str], rows: Iterable[dict[str, Any]]) -> None:
    print(title)
    print("\t".join(headers))
    for row in rows:
        print("\t".join(format_table_value(row.get(header)) for header in headers))


def print_key_values(title: str, values: dict[str, Any]) -> None:
    print(title)
    for key, value in values.items():
        print(f"{key}\t{format_table_value(value)}")
