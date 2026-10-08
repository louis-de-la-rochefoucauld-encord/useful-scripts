#!/usr/bin/env python3
"""Print Cloudflare delivery progress and export every failed or blocked task."""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

WORKER_URL = "https://walden-data-delivery.encord-648.workers.dev"
PROJECT_HASH = "357ed8d1-9beb-40a6-8087-c232cbcfb486"
PROBLEM_STATES = {"failed", "blocked_missing_data", "blocked_conflict"}
CSV_COLUMNS = (
    "data_hash", "client_id", "data_title", "stem", "state", "reason",
    "missing_data_element", "attempts", "runs", "updated_at_utc", "exported_at_utc", "task_url",
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    credentials = parser.add_mutually_exclusive_group(required=True)
    credentials.add_argument("--admin-token-env", help="Environment variable containing the admin token")
    credentials.add_argument("--secrets-file", type=Path, help="JSON file containing ADMIN_TOKEN")
    parser.add_argument("--worker-url", default=WORKER_URL)
    parser.add_argument("--csv", type=Path,
                        default=Path.home() / "Downloads" / "walden_data_delivery_problems.csv")
    parser.add_argument("--watch", action="store_true", help="Refresh the table and CSV repeatedly")
    parser.add_argument("--interval-seconds", type=int, default=60)
    args = parser.parse_args()
    if args.interval_seconds < 1:
        parser.error("--interval-seconds must be positive")
    if not args.worker_url.startswith("https://"):
        parser.error("--worker-url must use HTTPS")
    return args


def load_token(args):
    if args.secrets_file:
        token = json.loads(args.secrets_file.read_text()).get("ADMIN_TOKEN")
    else:
        token = os.environ.get(args.admin_token_env)
    if not isinstance(token, str) or not token.strip():
        raise ValueError("Admin token is missing")
    if any(character in token for character in '\r\n"\\'):
        raise ValueError("Admin token contains invalid characters")
    return token


def get_json(base_url, path, token):
    result = subprocess.run(
        ["curl", "--silent", "--show-error", "--max-time", "60", "--config", "-",
         "--write-out", "\n%{http_code}", "--url", base_url.rstrip("/") + path],
        input=f'header = "Authorization: Bearer {token}"\n',
        capture_output=True, text=True, timeout=65,
    )
    if result.returncode:
        raise RuntimeError(f"{path}: request failed ({result.returncode})")
    body, _, status = result.stdout.rpartition("\n")
    if status == "404" and path == "/problems":
        raise RuntimeError("The Worker needs the read-only /problems endpoint before a complete CSV can be exported")
    if status != "200":
        raise RuntimeError(f"{path}: HTTP {status}")
    return json.loads(body)


def timestamp(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat() if milliseconds else ""


def missing_data_element(state, reason):
    if state != "blocked_missing_data":
        return ""
    detail = re.sub(r"^[0-9a-f-]{36}:\s*", "", reason, flags=re.IGNORECASE)
    match = re.search(r"^(.+?)\s+is missing(?: or mismatched)?[.!]?$", detail, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"\bmissing\s+(?!or\b)(.+)$", detail, re.IGNORECASE)
    return match.group(1) if match else detail


def export_csv(path, snapshot):
    jobs = snapshot["jobs"]
    if snapshot["count"] != len(jobs) or len({job["data_hash"] for job in jobs}) != len(jobs):
        raise ValueError("Incomplete or duplicate problem records; CSV was not replaced")
    rows = []
    for job in jobs:
        if job["state"] not in PROBLEM_STATES:
            raise ValueError("Unexpected problem state; CSV was not replaced")
        reason = job.get("error") or "Reason not recorded by scheduler"
        rows.append({
            **{name: job.get(name, "") for name in ("data_hash", "client_id", "data_title", "stem", "state", "attempts", "runs")},
            "reason": reason,
            "missing_data_element": missing_data_element(job["state"], reason),
            "updated_at_utc": timestamp(job.get("updated_at")),
            "exported_at_utc": timestamp(snapshot["generated_at"]),
            "task_url": f"https://app.encord.com/label_editor/{PROJECT_HASH}/{job['data_hash']}/0",
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8-sig") as output:
            writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return Counter(job["state"] for job in jobs)


def print_table(status):
    counts = status["counts"]
    rows = [
        ("Running", counts.get("running", 0)),
        ("Pending", counts.get("pending", 0)),
        ("Delivered in last hour (tasks/hour)", status["delivered_last_hour"]),
        ("Delivered total", counts.get("delivered", 0)),
        ("Rerouted (Annotate 1 / Archive)", counts.get("rerouted", 0)),
        ("Left Data Delivery", counts.get("left_stage", 0)),
        ("Blocked: missing data", counts.get("blocked_missing_data", 0)),
        ("Blocked: conflicting files", counts.get("blocked_conflict", 0)),
        ("Failed", counts.get("failed", 0)),
    ]
    width = max(len(label) for label, _ in rows)
    print("\nWalden Data Delivery — " + datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d %H:%M:%S %Z"))
    print(f"{'Metric — whole delivery queue':<{width}} | Current")
    print(f"{'-' * width}-+------------")
    for label, value in rows:
        print(f"{label:<{width}} | {value:>10,}")
    settings = status.get("settings") or {}
    print(f"Mode: {settings.get('mode', 'unknown')}; concurrency: {settings.get('concurrency', 'unknown')}")
    poll = status.get("last_poll") or {}
    if poll.get("at"):
        age = max(0, (time.time() * 1000 - poll["at"]) / 60_000)
        print(f"Last stage poll: {age:.1f} minutes ago")
    if status.get("auto_paused"):
        print("Auto-pause: " + json.dumps(status["auto_paused"]))


def main():
    args = parse_args()
    token = load_token(args)
    while True:
        try:
            status = get_json(args.worker_url, "/status", token)
            print_table(status)
            snapshot = get_json(args.worker_url, "/problems", token)
            counts = export_csv(args.csv, snapshot)
            print(f"CSV: {args.csv} ({snapshot['count']:,} tasks; {dict(counts)})", flush=True)
        except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as error:
            print(f"Monitor error: {error}", file=sys.stderr, flush=True)
            if not args.watch:
                return 1
        if not args.watch:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
