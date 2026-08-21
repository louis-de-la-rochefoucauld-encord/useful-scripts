"""Bulk-move workflow tasks to another stage, recording a revert CSV first.

Reads data hashes from a file (one per line), finds each task's current stage,
writes a CSV with everything needed to revert or reprocess the move (initial
stage, destination stage, storage item UUID, and optionally the label hash of
the same data unit in a second project), then executes the moves.

The CSV is written before any task is moved, and rewritten as each origin-stage
batch completes, so an interrupted run still records where every task started.
Status values: pending (not yet moved), moved, not_found (no task with that
data hash in any stage), already_in_destination.

    uv run helpers/move_tasks.py \\
        --project-hash 00000000-0000-0000-0000-000000000000 \\
        --data-hashes-file hashes.txt \\
        --destination-stage "Annotate 1" \\
        --label-project-hash 11111111-1111-1111-1111-111111111111 \\
        --output-csv moves.csv \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api.encord.com

Note: a workflow router is not a stage and cannot be a destination. Moving a
task directly to a stage bypasses any router in between; to reproduce a
percentage router's split, divide the input hashes into per-stage files and run
the script once per destination stage.

Workflow stages and task actions (get_tasks, move):
https://docs.encord.com/sdk-documentation/projects-sdk/sdk-workflows-stages
"""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from utils import add_connection_args, load_client  # noqa: E402

GET_TASKS_CHUNK = 100


def log(msg: str) -> None:
    print(f"[move_tasks] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-hash", required=True, help="Project whose tasks to move")
    parser.add_argument(
        "--data-hashes-file",
        required=True,
        type=Path,
        help="Text file with one data hash per line",
    )
    parser.add_argument(
        "--destination-stage",
        required=True,
        help="Stage to move the tasks to, by title or UUID",
    )
    parser.add_argument(
        "--label-project-hash",
        help="Also record each data unit's label hash in this project (extra CSV column source_label_hash)",
    )
    parser.add_argument("--output-csv", required=True, type=Path, help="Where to write the revert CSV")
    add_connection_args(parser)
    return parser.parse_args()


def resolve_stage(workflow, name_or_uuid: str):
    for stage in workflow.stages:
        if stage.title == name_or_uuid or str(stage.uuid) == name_or_uuid:
            return stage
    available = ", ".join(f"{s.title} ({s.uuid})" for s in workflow.stages)
    raise SystemExit(f"No stage named {name_or_uuid!r}. Available stages: {available}")


def label_rows_by_data_hash(project, data_hashes: list) -> dict:
    """Label row metadata per data hash, fetched in chunks to keep requests small.

    Label rows: https://docs.encord.com/sdk-documentation/sdk-labels/sdk-working-with-labels
    """
    rows = {}
    for i in range(0, len(data_hashes), GET_TASKS_CHUNK):
        for row in project.list_label_rows_v2(data_hashes=data_hashes[i : i + GET_TASKS_CHUNK]):
            rows[row.data_hash] = row
    return rows


def write_csv(path: Path, records: list) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    args = parse_args()
    data_hashes = list(dict.fromkeys(args.data_hashes_file.read_text().split()))
    if not data_hashes:
        raise SystemExit(f"No data hashes found in {args.data_hashes_file}")
    log(f"{len(data_hashes)} data hash(es) to move")

    client = load_client(args.ssh_key_env, args.domain)
    project = client.get_project(args.project_hash)
    destination = resolve_stage(project.workflow, args.destination_stage)
    log(f"destination stage: {destination.title} ({destination.uuid})")

    tasks = {}
    for stage in project.workflow.stages:
        for i in range(0, len(data_hashes), GET_TASKS_CHUNK):
            for task in stage.get_tasks(data_hash=data_hashes[i : i + GET_TASKS_CHUNK]):
                tasks[str(task.data_hash)] = (task, stage)
        log(f"scanned {stage.title}: {len(tasks)} of {len(data_hashes)} located so far")

    item_rows = label_rows_by_data_hash(project, data_hashes)
    source_rows = {}
    if args.label_project_hash:
        source_rows = label_rows_by_data_hash(client.get_project(args.label_project_hash), data_hashes)

    records = []
    for data_hash in data_hashes:
        task, stage = tasks.get(data_hash, (None, None))
        if task is None:
            status = "not_found"
        elif stage.uuid == destination.uuid:
            status = "already_in_destination"
        else:
            status = "pending"
        item_row = item_rows.get(data_hash)
        record = {
            "data_hash": data_hash,
            "initial_stage": stage.title if stage else "",
            "initial_stage_uuid": str(stage.uuid) if stage else "",
            "destination_stage": destination.title,
            "destination_stage_uuid": str(destination.uuid),
            "storage_item_uuid": str(item_row.backing_item_uuid) if item_row else "",
            "status": status,
        }
        if args.label_project_hash:
            source_row = source_rows.get(data_hash)
            record["source_label_hash"] = source_row.label_hash if source_row else ""
        records.append(record)
    write_csv(args.output_csv, records)
    log(f"wrote revert CSV to {args.output_csv} before moving anything")

    by_origin = {}
    for record in records:
        if record["status"] == "pending":
            task, stage = tasks[record["data_hash"]]
            by_origin.setdefault(stage.uuid, []).append((task, record))
    for origin_uuid, group in by_origin.items():
        with project.create_bundle() as bundle:
            for task, _ in group:
                task.move(destination_stage_uuid=destination.uuid, bundle=bundle)
        for _, record in group:
            record["status"] = "moved"
        write_csv(args.output_csv, records)
        log(f"moved {len(group)} task(s) from {group[0][1]['initial_stage']} to {destination.title}")

    counts = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    log(f"done: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
