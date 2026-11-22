"""
eval/offline_detect.py
======================
CLI tool to replay a log file through parser, windowing, and anomaly detector
in event time with NO database writes. Prints per-window score/threshold/flag and summary.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import ClosedWindow, WindowAggregator
from app.parsing.parser import LogParser

# Pattern matching testbed plain log format:
# YYYY-MM-DD HH:MM:SS,mmm [service] LEVEL message
LOG_LINE_REGEX = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:,\d+)?)\s+\[(?P<service>[^\]]+)\]\s+(?P<level>[A-Z]+)\s+(?P<message>.*)$"
)


def _parse_log_line(line: str) -> dict | None:
    match = LOG_LINE_REGEX.match(line.strip())
    if not match:
        return None
    raw_ts = match.group("ts").replace(",", ".")
    try:
        dt = datetime.fromisoformat(raw_ts).replace(tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)

    service = match.group("service")
    level = match.group("level")
    message = match.group("message")
    return {
        "ts": dt,
        "service": service,
        "level": level,
        "message": message,
    }


def run_offline_detect(logfile: Path, window_seconds: int = 10, warmup_windows: int = 30) -> None:
    if not logfile.exists():
        print(f"Error: Log file not found at {logfile}", file=sys.stderr)
        sys.exit(1)

    parser = LogParser(state_path="")
    aggregator = WindowAggregator(
        window_seconds=window_seconds,
        warmup_windows=warmup_windows,
    )
    detector = AnomalyDetector(
        warmup_windows=warmup_windows,
        save_to_db=False,
    )

    total_events = 0
    scored_windows: list[WindowResult] = []

    print(f"=== Replaying {logfile} (window={window_seconds}s, warmup={warmup_windows}) ===")
    print(f"{'Window':<21} | {'Score':<8} | {'Threshold':<9} | {'Flagged':<7} | {'Events':<6}")
    print("-" * 65)

    with logfile.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event = _parse_log_line(line)
            if not event:
                continue
            total_events += 1
            parsed = parser.parse(event["message"])
            closed_windows = aggregator.add(event, parsed)
            for cw in closed_windows:
                res = detector.score_window(cw)
                scored_windows.append(res)
                _print_window(res, cw.event_count)

    # Flush remaining windows
    for cw in aggregator.flush():
        res = detector.score_window(cw)
        scored_windows.append(res)
        _print_window(res, cw.event_count)

    # Summary
    anomalies = [w for w in scored_windows if w.is_anomalous]
    print("-" * 65)
    print(f"Summary:")
    print(f"  Total events parsed:    {total_events}")
    print(f"  Total windows emitted:  {len(scored_windows)}")
    print(f"  Warm-up windows:        {warmup_windows}")
    print(f"  Anomalous windows:      {len(anomalies)}")
    print(f"  Templates discovered:   {parser.template_count}")
    print(f"  Late events dropped:    {aggregator.late_dropped}")


def _print_window(res: WindowResult, event_count: int) -> None:
    w_str = f"{res.window_start:%H:%M:%S} - {res.window_end:%H:%M:%S}"
    score_str = f"{res.score:.4f}"
    thresh_str = f"{res.threshold:.4f}" if res.threshold is not None else "warmup"
    flag_str = "ANOMALY" if res.is_anomalous else "ok"
    print(f"{w_str:<21} | {score_str:<8} | {thresh_str:<9} | {flag_str:<7} | {event_count:<6}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline log anomaly detection replay.")
    parser.add_argument("logfile", type=Path, help="Path to input log file")
    parser.add_argument("--window", type=int, default=10, help="Window size in seconds (default: 10)")
    parser.add_argument("--warmup", type=int, default=30, help="Number of warmup windows (default: 30)")
    args = parser.parse_args()

    run_offline_detect(args.logfile, window_seconds=args.window, warmup_windows=args.warmup)


if __name__ == "__main__":
    main()
