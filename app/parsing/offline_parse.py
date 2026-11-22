"""
app/parsing/offline_parse.py
============================
Offline log parser script. Parses plain log files in the format:
"YYYY-MM-DD HH:MM:SS,mmm [service] LEVEL message"
without writing to the database.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

from app.parsing.parser import LogParser

LOG_LINE_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s+\[[^\]]+\]\s+[A-Z]+\s+(.*)$"
)


def parse_log_file(file_path: str | Path, limit: int | None = None, top: int = 20) -> None:
    path = Path(file_path)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    # Use in-memory mode (persistence=False) so we do not modify the persistent state or database.
    parser = LogParser(persistence=False)
    line_count = 0
    template_counts: Counter[int] = Counter()
    template_texts: dict[int, str] = {}

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line_count += 1
            match = LOG_LINE_REGEX.match(line)
            message = match.group(1) if match else line

            parsed = parser.parse(message)
            template_counts[parsed.template_id] += 1
            template_texts[parsed.template_id] = parsed.template_text

            if limit is not None and line_count >= limit:
                break

    print(f"Lines parsed: {line_count}")
    print(f"Templates mined: {len(template_counts)}")
    print(f"\nTop {min(top, len(template_counts))} templates by count:")
    print("-" * 75)
    for template_id, count in template_counts.most_common(top):
        print(f"[{count:5d}x] [ID {template_id:3d}] {template_texts[template_id]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Offline Drain3 log parser. Parses log files without writing to DB."
    )
    parser.add_argument("file", help="Path to plain log file")
    parser.add_argument("--limit", type=int, default=None, help="Max lines to parse")
    parser.add_argument(
        "--top", type=int, default=20, help="Number of top templates to show (default: 20)"
    )
    args = parser.parse_args()

    parse_log_file(args.file, limit=args.limit, top=args.top)


if __name__ == "__main__":
    main()
