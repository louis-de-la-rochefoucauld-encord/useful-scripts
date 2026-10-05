#!/usr/bin/env python3
"""Read-only status for Walden's Hand Pose Agent workflow deployments."""

import argparse
import asyncio
import importlib.util
import json
import math
import re
import shutil
import subprocess
import sys
import textwrap
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from itertools import islice
from pathlib import Path
from zoneinfo import ZoneInfo

CONTROLLER = "walden-hand-pose-controller-v502"
TRACKING = "walden-hand-tracking-agent-v502"
POSE = "walden-hand-pose-worker-v502"
LOCAL_TIMEZONE = ZoneInfo("Europe/London")
EXACT_CACHE = Path.home() / ".cache" / "walden_backfill_status_exact.json"
INVENTORY_AVERAGE_HOURS = 4913.919 / 81963
AUDIT_PATH = Path.home() / ".cache" / "walden_hand_pose_agent_status" / "state_audit.json"
AUDIT_LIMIT = 50000
CONTROL_KEYS = ("enabled", "allowed_hashes", "stage_poll_last", "tracking_last",
                "tracking_reconcile_last", "pose_last", "pose_reconcile_last")
FUNCTIONS = {
    "tracking_worker": (TRACKING, "work_loop"),
    "pose_cpu": (POSE, "work_loop_cpu"),
    "pose_gpu": (POSE, "pose_frames"),
    "poll": (CONTROLLER, "poll_stage"),
    "tracking_feeder": (CONTROLLER, "feed_tracking"),
    "tracking_reconcile": (CONTROLLER, "reconcile_tracking"),
    "pose_feeder": (CONTROLLER, "feed_pose"),
    "pose_reconcile": (CONTROLLER, "reconcile_pose"),
}


def modal_python():
    if importlib.util.find_spec("modal") is not None:
        return sys.executable
    executable = shutil.which("modal")
    if executable:
        first_line = Path(executable).resolve().read_text().splitlines()[0]
        if first_line.startswith("#!/"):
            interpreter = first_line[2:]
            if Path(interpreter).is_file():
                return interpreter
    raise RuntimeError("Cannot locate Modal's Python interpreter; run with a Python that has modal installed")


def read_logs(app, window_start, search=None):
    seconds = max(1, math.ceil(time.time() - window_start))
    command = ["modal", "app", "logs", app, "--since", str(seconds) + "s",
               "--timestamps", "-e", "production"]
    if search:
        command.extend(["--search", search])
    for attempt in range(4):
        process = subprocess.run(command, capture_output=True, text=True, timeout=120)
        if process.returncode == 0:
            return process.stdout
        if attempt < 3 and any(token in process.stderr.lower() for token in (
            "overloaded", "resource exhausted", "temporarily unavailable", "service unavailable",
        )):
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError("Recent logs unavailable")


def pose_worker_counts(log_text, start, end):
    completed = set()
    failed = set()
    for line in log_text.splitlines():
        match = re.match(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[+-]\d\d:\d\d)\s+(.*)$", line)
        if not match or not start <= datetime.fromisoformat(match[1]).timestamp() <= end:
            continue
        message = match[2]
        success = re.match(r"OK (.+?): \d+ hand records at (\S+) fps", message)
        if success and success[2] in {"30", "30.0"} and "DRY RUN" not in message:
            completed.add(success[1])
        failure = re.match(r"FAILED (.+?): ", message)
        if failure:
            failed.add(failure[1])
    return len(completed), len(failed)


def log_reports(log_text, start, end):
    messages = []
    for line in log_text.splitlines():
        match = re.match(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[+-]\d\d:\d\d)\s+(.*)$", line)
        if match and start <= datetime.fromisoformat(match[1]).timestamp() <= end:
            messages.append(match[2])
    reports = []
    decoder = json.JSONDecoder()
    buffer = ""
    for message in messages:
        if not buffer:
            if not message.lstrip().startswith("{"):
                continue
            buffer = message
        else:
            buffer += "\n" + message
        try:
            report, _ = decoder.raw_decode(buffer.lstrip())
        except json.JSONDecodeError:
            if len(buffer) > 100000:
                buffer = ""
            continue
        if isinstance(report, dict):
            reports.append(report)
        buffer = ""
    return reports


def number(value):
    return "unknown" if value is None else "{:,}".format(value)


def setting_status(value):
    return {True: "enabled", False: "disabled"}.get(value, "unknown")


def average_clip_hours():
    try:
        cache = json.loads(EXACT_CACHE.read_text())
        count, hours = int(cache.get("count", 0)), float(cache.get("hours", 0))
        if cache.get("environment") == "production" and count > 0 and math.isfinite(hours) and hours > 0:
            return hours / count, "last exact local scan"
    except (OSError, ValueError, TypeError):
        pass
    return INVENTORY_AVERAGE_HOURS, "historical inventory"


def summary_age(record):
    timestamp = (record or {}).get("updated_at")
    return "unknown" if not timestamp else "{:.1f}m".format(max(0, time.time() - timestamp) / 60)


def draw_table(rows):
    widths = (13, 53, 25, 52)
    for row in [("Step", "Live queue / workers", "Window throughput", "Summary / waiting work")] + rows:
        cells = [sum([textwrap.wrap(line, width=width) or [""] for line in value.split("\n")], [])
                 for value, width in zip(row, widths)]
        for index in range(max(map(len, cells))):
            print("  " + "  ".join((cell[index] if index < len(cell) else "").ljust(width)
                                   for cell, width in zip(cells, widths)))
        print("  " + "  ".join("─" * width for width in widths))


async def collect_snapshot():
    import modal

    result = {"started_at": time.time(), "errors": {}}
    semaphore = asyncio.Semaphore(4)

    async def read(label, operation):
        async with semaphore:
            for attempt in range(4):
                try:
                    return await asyncio.wait_for(operation(), timeout=45)
                except Exception as error:
                    if attempt < 3 and any(word in str(error).lower() for word in (
                        "overloaded", "resource exhausted", "temporarily unavailable",
                        "service unavailable", "too many requests",
                    )):
                        await asyncio.sleep(2 ** attempt)
                        continue
                    result["errors"][label] = type(error).__name__
                    return None

    def store(name):
        return modal.Dict.from_name(name, environment_name="production", create_if_missing=False)

    def function(app, name):
        return modal.Function.from_name(app, name, environment_name="production")

    async def control():
        source = store(CONTROLLER + "-control")
        values = await asyncio.gather(*[
            read(key, lambda key=key: source.get.aio(key)) for key in CONTROL_KEYS
        ])
        return dict(zip(CONTROL_KEYS, values))

    async def counts():
        names = ("state", "tracking-ready", "tracking-pending", "pose-ready", "pose-pending")
        values = await asyncio.gather(*[
            read(name, lambda name=name: store(CONTROLLER + "-" + name).len.aio())
            for name in names
        ])
        result = dict(zip(names, values))
        attempts = await asyncio.gather(*[
            read(key + "_attempts", lambda app=app: store(app + "-attempts").len.aio())
            for key, app in (("tracking", TRACKING), ("pose", POSE))
        ])
        result.update(zip(("tracking-attempts", "pose-attempts"), attempts))
        return result

    async def queues():
        names = {"tracking": TRACKING, "pose": POSE}
        values = await asyncio.gather(*[
            read(key + "_queue", lambda app=app: modal.Queue.from_name(
                app + "-queue", environment_name="production", create_if_missing=False
            ).len.aio(total=True)) for key, app in names.items()
        ])
        return dict(zip(names, values))

    async def stats():
        async def one(key, app, name):
            value = await read(key, lambda: function(app, name).get_current_stats.aio())
            return {} if value is None else {"running": value.num_running_inputs,
                                            "backlog": value.backlog}
        values = await asyncio.gather(*[one(key, *names) for key, names in FUNCTIONS.items()])
        return dict(zip(FUNCTIONS, values))

    async def identities():
        values = await asyncio.gather(*[
            read(key + "_identity", lambda app=app: function(app, "runtime_identity").remote.aio())
            for key, app in (("tracking", TRACKING), ("pose", POSE))
        ])
        return dict(zip(("tracking", "pose"), values))

    async def phase():
        return await read("backfill_phase", lambda: store(
            "walden-hand-1000h-backfill-9a-control"
        ).get.aio("cohort_phase"))

    values = await asyncio.gather(control(), counts(), queues(), stats(), identities(), phase())
    result.update(zip(("control", "counts", "queues", "functions", "identities", "backfill_phase"), values))
    result["finished_at"] = time.time()
    return result


def collect_state_audit():
    import modal

    source = modal.Dict.from_name(CONTROLLER + "-state", environment_name="production", create_if_missing=False)
    expected = source.len()
    if expected > AUDIT_LIMIT:
        raise RuntimeError(f"State has {expected:,} records; audit limit is {AUDIT_LIMIT:,}")
    states = Counter()
    scanned = 0
    for _, record in islice(source.items(), expected + 1):
        states[record.get("state", "<missing>")] += 1
        scanned += 1
    if scanned != expected:
        raise RuntimeError(f"State changed during audit: expected {expected:,}, read {scanned:,}")
    return {"audited_at": time.time(), "record_count": expected,
            "states": dict(states)}


def read_state_audit():
    process = subprocess.run(
        [modal_python(), str(Path(__file__).resolve()), "--state-audit-json"],
        capture_output=True, text=True, timeout=900,
    )
    if process.returncode:
        raise RuntimeError("State audit unavailable; check Modal authentication/connectivity")
    return json.loads(process.stdout)


def load_state_audit():
    try:
        return json.loads(AUDIT_PATH.read_text())
    except (FileNotFoundError, ValueError):
        return None


def save_state_audit(audit):
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = AUDIT_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(audit) + "\n")
    temporary.replace(AUDIT_PATH)


def read_snapshot():
    process = subprocess.run(
        [modal_python(), str(Path(__file__).resolve()), "--snapshot-json"],
        capture_output=True, text=True, timeout=240,
    )
    if process.returncode:
        raise RuntimeError("Live status unavailable; check Modal authentication/connectivity")
    return json.loads(process.stdout)


def worker_logs(app, start, searches):
    with ThreadPoolExecutor(max_workers=len(searches)) as executor:
        futures = [executor.submit(read_logs, app, start, search)
                   for search in searches]
        return "\n".join(future.result() for future in futures)


def tracking_worker_counts(log_text, start, end):
    completed, failed = {}, set()
    for line in log_text.splitlines():
        match = re.match(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[+-]\d\d:\d\d)\s+(.*)$", line)
        if not match or not start <= datetime.fromisoformat(match[1]).timestamp() <= end:
            continue
        message = match[2]
        success = re.match(r"(.+?) both=(\d+(?:\.\d+)?)% usable=.*? slots=(\d+) ", message)
        if success and "DRY RUN" not in message:
            completed[success[1]] = float(success[2]) >= 70
        failure = re.match(r"FAILED (.+?): ", message)
        if failure:
            failed.add(failure[1])
    return len(completed), len(failed), sum(completed.values())


def product(identity, first, second):
    identity = identity or {}
    values = (identity.get(first), identity.get(second))
    return values[0] * values[1] if all(isinstance(value, int) for value in values) else None


def render(snapshot, logs, errors, start, end, verbose=False, audit=None):
    n = number
    control, counts, functions = (snapshot[key] for key in ("control", "counts", "functions"))
    tracking = snapshot["identities"].get("tracking")
    pose = snapshot["identities"].get("pose")
    limits = {"tracking_worker": product(tracking, "containers", "concurrency"),
              "pose_cpu": product(pose, "cpu_drivers", "cpu_concurrency"),
              "pose_gpu": product(pose, "gpu_containers", "gpu_concurrency")}
    window_hours = (end - start) / 3600
    clip_hours, average_source = average_clip_hours()
    tracking_counts = tracking_worker_counts(logs["tracking"], start, end) if "tracking" in logs else (None, None, None)
    pose_counts = pose_worker_counts(logs["pose"], start, end) if "pose" in logs else (None, None)

    def workers(key):
        return f"{n(functions[key].get('running'))}/{n(limits[key])}"

    def throughput(values):
        completed, failed = values[:2]
        if completed is None:
            return "Unavailable: worker logs"
        return (f"{n(completed)} completed; {n(failed)} failed\n"
                f"{completed / window_hours:.1f} videos/h\n"
                f"~{completed * clip_hours / window_hours:.1f} video-h/h")

    def waiting(key):
        return n(functions[key].get("backlog"))

    enabled = control.get("enabled")
    allowed = control.get("allowed_hashes")
    selection = "all stage tasks" if allowed == ["all"] else (
        f"{len(allowed):,} selected tasks" if isinstance(allowed, list) else "unknown selection")
    timestamp = lambda value: datetime.fromtimestamp(value, LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    audited_states = (audit or {}).get("states", {})
    audit_age = f"{max(0, (snapshot['finished_at'] - audit['audited_at']) / 60):.0f}m old" if audit else ""
    poll = control.get("stage_poll_last") or {}
    poll_time = timestamp(poll["updated_at"]) if poll.get("updated_at") else "unknown"

    def retained(state, attempt_key):
        if audit:
            return f"{n(audited_states.get(state, 0))} {state} (audit {audit_age})"
        return f"Confirmed failures unavailable; {n(counts.get(attempt_key))} attempt records"

    prior_summary = (f"{n(audited_states.get('prior_failure'))} prior_failure (audit {audit_age})\n"
                     if audit else "Prior failures unavailable\n")
    print(f"\nWalden Hand Pose Agent status — {timestamp(snapshot['finished_at'])}")
    print(f"Production. Controller {setting_status(enabled)}; {selection}. "
          f"{n(counts['state'])} registered tasks.")
    if snapshot.get("backfill_phase") != "Complete":
        print(f"Feeding waits for backfill phase Complete; current phase: {snapshot.get('backfill_phase') or 'unknown'}.")
    print(f"Window: {timestamp(start)}–{timestamp(end)} ({window_hours:g} hours). "
          f"Live reads took {snapshot['finished_at'] - snapshot['started_at']:.1f}s.\n")
    rows = [
        ("Tracking", f"{n(snapshot['queues']['tracking'])} queued; {workers('tracking_worker')} running/max inputs; "
         f"{waiting('tracking_worker')} worker calls waiting", throughput(tracking_counts),
         f"{n(counts['tracking-ready'])} ready to queue (live index)\n"
         f"{n(counts['tracking-pending'])} awaiting result checks\n"
         f"{retained('tracking_failed', 'tracking-attempts')}\n"
         f"Window: {n(tracking_counts[2])} passed the 70% gate"),
        ("Pose", f"{n(snapshot['queues']['pose'])} queued; CPU {workers('pose_cpu')} and GPU {workers('pose_gpu')} "
         f"running/max inputs; {waiting('pose_gpu')} GPU calls waiting", throughput(pose_counts),
         f"{n(counts['pose-ready'])} ready to queue (live index)\n"
         f"{n(counts['pose-pending'])} awaiting result / routing checks\n"
         f"{retained('pose_failed', 'pose-attempts')}"),
        ("Other", "—", "—", prior_summary
         + f"Stage poll: {n(poll.get('scanned'))} active at {poll_time}"),
    ]
    draw_table(rows)
    if audit:
        blocked = {key: value for key, value in audited_states.items()
                   if key not in {"pose_complete", "archived", "tracking_failed", "pose_failed", "prior_failure"} and value}
        print(f"Audit at {timestamp(audit['audited_at'])} "
              f"({n(audit['record_count'])} controller records): "
              + ("other states " + "; ".join(f"{key} {n(value)}" for key, value in sorted(blocked.items())) if blocked else "no other states") + ".")
    else:
        print("Exact failed/blocked state counts unavailable; run --refresh-state-audit for a bounded read-only audit.")
    print("Controller calls waiting: " + "; ".join(
        f"{label} refill {waiting(step + '_feeder')}, result checks {waiting(step + '_reconcile')}"
        for step, label in (("tracking", "Tracking"), ("pose", "Pose"))) + f"; stage polls {waiting('poll')}.")
    print("Last updates: stage poll " + summary_age(control.get("stage_poll_last"))
          + "; Tracking " + summary_age(control.get("tracking_last"))
          + "; Pose " + summary_age(control.get("pose_last")) + ".")
    print("Throughput counts worker finishes; ~ hours use the " + average_source + " average clip duration.")
    print("Pose routes to Data Delivery. Cloudflare delivery is separate and is not included in these counts.")
    if verbose:
        reports = log_reports(logs.get("controller", ""), start, end)
        tracking_reports = [report for report in reports if "passed" in report and "archived" in report]
        pose_reports = [report for report in reports if "route_pending" in report and "completed" in report]
        for name, reports, keys in (("Tracking reconciled", tracking_reports, ("passed", "archived", "failed")),
                                    ("Pose reconciled", pose_reports, ("completed", "failed", "route_pending"))):
            print(name + ": " + ", ".join(f"{key} {sum(report.get(key, 0) for report in reports):,}" for key in keys)
                  if "controller" in logs else name + ": unavailable")
    all_errors = {**snapshot["errors"], **errors}
    if all_errors:
        print("Unavailable reads: " + ", ".join(f"{key} ({value})" for key, value in all_errors.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-hours", type=float, default=1)
    parser.add_argument("--fast", action="store_true", help="Compatibility flag; this monitor always uses bounded live reads")
    parser.add_argument("--verbose", action="store_true", help="Also show controller reconciliation totals")
    parser.add_argument("--env", choices=("production",), default="production")
    parser.add_argument("--snapshot-json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--state-audit-json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--refresh-state-audit", action="store_true",
                        help="Read up to 50,000 controller records once and cache exact state counts locally")
    args = parser.parse_args()
    if not 0 < args.window_hours <= 24:
        parser.error("--window-hours must be greater than 0 and at most 24")
    if args.snapshot_json:
        print(json.dumps(asyncio.run(collect_snapshot())))
        return
    if args.state_audit_json:
        print(json.dumps(collect_state_audit()))
        return
    print("Reading Hand Pose Agent status from Modal (read-only)…", flush=True)
    if args.refresh_state_audit:
        print("Auditing controller states (bounded, read-only)…", flush=True)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(read_state_audit)
                while not future.done():
                    wait((future,), timeout=30)
                    if not future.done():
                        print("  Still auditing controller states…", flush=True)
                save_state_audit(future.result())
        except Exception as error:
            print(f"  State audit unavailable ({type(error).__name__}); showing the previous audit if present.", flush=True)
    end = time.time()
    start = end - args.window_hours * 3600
    jobs = {"snapshot": read_snapshot,
            "tracking": lambda: worker_logs(TRACKING, start, ("both=", "FAILED ")),
            "pose": lambda: worker_logs(POSE, start, ("OK ", "FAILED "))}
    if args.verbose:
        jobs["controller"] = lambda: read_logs(CONTROLLER, start)
    values, errors = {}, {}
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {executor.submit(operation): key for key, operation in jobs.items()}
        pending = set(futures)
        while pending:
            done, pending = wait(pending, timeout=30)
            if not done:
                print("  Still reading live status/logs…", flush=True)
            for future in done:
                key = futures[future]
                try:
                    values[key] = future.result()
                    print(f"  Loaded {key}…", flush=True)
                except Exception as error:
                    errors[key] = type(error).__name__
    if "snapshot" not in values:
        print("Live status unavailable; check Modal connectivity/authentication.", file=sys.stderr)
        return 1
    render(values.pop("snapshot"), values, errors, start, end, args.verbose, load_state_audit())
    return 0


if __name__ == "__main__":
    sys.exit(main())
