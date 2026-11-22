"""
eval/offline_detect.py
======================
CLI tool to replay a log or jsonl file through parser, windowing, and anomaly detector
in event time with NO database writes. Prints per-window score/threshold/flag/confirmed_at,
summary metrics, and optional fault analysis.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import ClosedWindow, WindowAggregator
from app.parsing.parser import LogParser

# Pattern matching testbed plain log format:
# YYYY-MM-DD HH:MM:SS,mmm [service] LEVEL message
LOG_LINE_REGEX = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:,\d+)?)\s+\[(?P<service>[^\]]+)\]\s+(?P<level>[A-Z]+)\s+(?P<message>.*)$"
)


def _parse_event_line(line: str) -> dict | None:
    line_stripped = line.strip()
    if not line_stripped:
        return None
    if line_stripped.startswith("{") and line_stripped.endswith("}"):
        try:
            data = json.loads(line_stripped)
            raw_ts = data["ts"]
            if isinstance(raw_ts, str):
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            elif isinstance(raw_ts, (int, float)):
                dt = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
            else:
                return None
            return {
                "ts": dt,
                "service": data.get("service", "unknown"),
                "level": data.get("level", "INFO"),
                "message": data.get("message", ""),
            }
        except Exception:
            pass

    match = LOG_LINE_REGEX.match(line_stripped)
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


def run_offline_detect(
    logfile: Path,
    window_seconds: int = 10,
    warmup_windows: int = 30,
    faults_path: Path | None = None,
    hst_seed: int = 42,
) -> None:
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
        seed=hst_seed,
        save_to_db=False,
    )

    total_events = 0
    scored_windows: list[WindowResult] = []

    print(f"=== Replaying {logfile} (window={window_seconds}s, warmup={warmup_windows}, seed={hst_seed}) ===")
    print(f"{'Window':<21} | {'Score':<8} | {'Threshold':<9} | {'Flagged':<7} | {'ConfirmedAt':<11} | {'Events':<6}")
    print("-" * 75)

    with logfile.open("r", encoding="utf-8") as f:
        for line in f:
            event = _parse_event_line(line)
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
    print("-" * 75)
    print("Summary:")
    print(f"  Total events parsed:    {total_events}")
    print(f"  Total windows emitted:  {len(scored_windows)}")
    print(f"  Warm-up windows:        {warmup_windows}")
    print(f"  Anomalous windows:      {len(anomalies)}")
    print(f"  Templates discovered:   {parser.template_count}")
    print(f"  Late events dropped:    {aggregator.late_dropped}")

    if faults_path and faults_path.exists():
        _analyze_faults(scored_windows, faults_path)


def _print_window(res: WindowResult, event_count: int) -> None:
    w_str = f"{res.window_start:%H:%M:%S} - {res.window_end:%H:%M:%S}"
    score_str = f"{res.score:.4f}"
    thresh_str = f"{res.threshold:.4f}" if res.threshold is not None else "warmup"
    flag_str = "ANOMALY" if res.is_anomalous else "ok"
    conf_str = f"{res.confirmed_at:%H:%M:%S}" if res.confirmed_at else "-"
    print(f"{w_str:<21} | {score_str:<8} | {thresh_str:<9} | {flag_str:<7} | {conf_str:<11} | {event_count:<6}")


def _analyze_faults(scored_windows: list[WindowResult], faults_path: Path) -> None:
    with faults_path.open("r", encoding="utf-8") as f:
        raw_faults = json.load(f)

    faults: list[dict] = []
    for rf in raw_faults:
        s_dt = datetime.fromisoformat(rf["started_at"]).astimezone(timezone.utc)
        e_dt = (
            datetime.fromisoformat(rf["ended_at"]).astimezone(timezone.utc)
            if rf.get("ended_at")
            else s_dt + timedelta(seconds=60)
        )
        faults.append(
            {
                "id": rf.get("id"),
                "fault_type": rf["fault_type"],
                "target_service": rf["target_service"],
                "started_at": s_dt,
                "ended_at": e_dt,
            }
        )

    print("\n" + "=" * 80)
    print(f"{'Fault':<15} | {'Service':<16} | {'Flagged':<7} | {'TTD (conf - start)':<18} | {'Peak Score':<10}")
    print("-" * 80)

    for fault in faults:
        f_start = fault["started_at"]
        f_end = fault["ended_at"]

        # Overlapping windows during fault (and immediate recovery window)
        overlap_windows = [
            w for w in scored_windows if w.window_end > f_start and w.window_start < f_end
        ]
        peak_score = max((w.score for w in overlap_windows), default=0.0)
        is_flagged = any(w.is_anomalous for w in overlap_windows)

        # Search for confirmed_at occurring between fault start and end+30s
        conf_window = next(
            (
                w
                for w in scored_windows
                if w.confirmed_at is not None
                and f_start <= w.confirmed_at <= f_end + timedelta(seconds=30)
            ),
            None,
        )

        ttd_str = "N/A"
        if conf_window and conf_window.confirmed_at:
            is_flagged = True
            ttd_sec = (conf_window.confirmed_at - f_start).total_seconds()
            ttd_str = f"+{ttd_sec:.1f}s"
            if conf_window.score > peak_score:
                peak_score = conf_window.score

        flag_str = "YES" if is_flagged else "NO"
        print(
            f"{fault['fault_type']:<15} | {fault['target_service']:<16} | {flag_str:<7} | {ttd_str:<18} | {peak_score:.4f}"
        )

    # Flagged windows outside any fault interval expanded by 30 seconds
    out_of_interval_windows: list[WindowResult] = []
    for w in scored_windows:
        if not w.is_anomalous:
            continue
        in_any_fault = any(
            w.window_end > (f["started_at"] - timedelta(seconds=30))
            and w.window_start < (f["ended_at"] + timedelta(seconds=30))
            for f in faults
        )
        if not in_any_fault:
            out_of_interval_windows.append(w)

    print("-" * 80)
    print(f"Flagged windows outside fault intervals (±30s): {len(out_of_interval_windows)}")
    for w in out_of_interval_windows:
        print(
            f"  [FP] {w.window_start:%H:%M:%S} - {w.window_end:%H:%M:%S} | score={w.score:.4f} | thresh={w.threshold}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline log anomaly detection replay.")
    parser.add_argument("logfile", type=Path, help="Path to input log or jsonl file")
    parser.add_argument("--window", type=int, default=10, help="Window size in seconds (default: 10)")
    parser.add_argument("--warmup", type=int, default=30, help="Number of warmup windows (default: 30)")
    parser.add_argument("--faults", type=Path, default=None, help="Optional path to faults.json")
    parser.add_argument("--hst-seed", type=int, default=42, help="Seed for HalfSpaceTrees (default: 42)")
    args = parser.parse_args()

    run_offline_detect(
        args.logfile,
        window_seconds=args.window,
        warmup_windows=args.warmup,
        faults_path=args.faults,
        hst_seed=args.hst_seed,
    )


if __name__ == "__main__":
    main()
