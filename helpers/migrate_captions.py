"""Copy the text of an archived classification onto a new classification, per task.

Reads the per-task file written by ``stage_ontology_counts.py --per-task-jsonl`` and
selects tasks that have at least one source instance and no target instance. For each
selected task, every source instance's text answer is copied onto a new target instance
covering the whole video, and the label row is saved. Source instances are left in
place. Tasks that already have a target instance are skipped, so re-running is safe.

Select tasks with ``--limit`` (first N from the file), ``--data-hashes`` (specific
tasks), or both. One of the two is required — there is no implicit "migrate
everything".

Prints one row per selected task: data hash, workflow stage, what was done, and the
label editor link for before/after checks.

    uv run helpers/migrate_captions.py \\
        --per-task-jsonl output/tasks.jsonl \\
        --project-hash 00000000-0000-0000-0000-000000000000 \\
        --source-classification "Caption" \\
        --target-classification "High Level Caption" \\
        --limit 3 \\
        --migrated-log output/migrated_captions.jsonl \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api.encord.com

Tasks are migrated in chunks of 200 across 8 parallel threads, each downloading its
own chunk in one bundled call, then saving each row in that chunk individually.
Bundled saves were tested and do not scale: a 10-row save bundle did not return
within 120 seconds, while individual saves reliably take 2-7 seconds each. So reads
are bundled but writes are not. The terminal output order can differ from
processing order — the printed table is sorted back into file order.

Every task actually migrated is appended, as one JSON line, to ``--migrated-log``.
Later runs append to the same file rather than overwriting it, so it accumulates
a record of every task migrated across all runs — this is the input a separate
verification script will check against.

Reading and adding classification instances:
https://docs.encord.com/sdk-documentation/sdk-labels/sdk-working-with-labels
Bundle reference (batches the label downloads and saves):
https://docs.encord.com/sdk-documentation/sdk-references/http.bundle
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from encord.objects import Classification  # noqa: E402
from encord.objects.frames import Range  # noqa: E402

from utils import add_connection_args, load_client  # noqa: E402


def log(msg: str) -> None:
    print(f"[migrate_captions] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-hash", required=True, help="Project containing the tasks")
    parser.add_argument(
        "--per-task-jsonl",
        required=True,
        help="Per-task file written by stage_ontology_counts.py --per-task-jsonl",
    )
    parser.add_argument(
        "--source-classification",
        required=True,
        help="Name of the classification to copy text from, e.g. 'Caption'",
    )
    parser.add_argument(
        "--target-classification",
        required=True,
        help="Name of the classification to copy text onto, e.g. 'High Level Caption'",
    )
    parser.add_argument("--limit", type=int, help="Migrate at most this many tasks, in file order")
    parser.add_argument("--data-hashes", nargs="+", help="Only migrate these tasks")
    parser.add_argument("--skip-stages", nargs="+", help="Skip tasks in these workflow stages, e.g. Complete Archive")
    parser.add_argument(
        "--migrated-log",
        required=True,
        help="JSONL file to append one record to per task actually migrated. Shared across runs "
        "as the record of what has been migrated so far.",
    )
    parser.add_argument(
        "--app-domain",
        default="https://app.encord.com",
        help="Encord web app domain, used for the printed links. Default: %(default)s",
    )
    add_connection_args(parser)
    args = parser.parse_args()
    if args.limit is None and not args.data_hashes:
        parser.error("pass --limit and/or --data-hashes; migrating every task requires an explicit --limit")
    return args


def select_tasks(args: argparse.Namespace) -> list:
    tasks = []
    with open(args.per_task_jsonl) as f:
        for line in f:
            record = json.loads(line)
            if record["items"].get(args.source_classification, 0) == 0:
                continue
            if record["items"].get(args.target_classification, 0) > 0:
                continue
            if args.skip_stages and record["stage"] in args.skip_stages:
                continue
            tasks.append(record)
    if args.data_hashes:
        by_hash = {t["data_hash"]: t for t in tasks}
        missing = [h for h in args.data_hashes if h not in by_hash]
        if missing:
            raise SystemExit(
                f"Not eligible for migration (not in the file, no source instance, or target "
                f"already present): {', '.join(missing)}"
            )
        tasks = [by_hash[h] for h in args.data_hashes]
    if args.limit is not None:
        tasks = tasks[: args.limit]
    return tasks


def migrate_row(row, source_name: str, target_cls: Classification) -> tuple:
    """Copy each source instance's text onto a new target instance. Returns (status, instances migrated)."""
    if any(ci.classification_name == target_cls.attributes[0].name for ci in row.get_classification_instances()):
        return "skipped: target already present", 0
    sources = [ci for ci in row.get_classification_instances() if ci.classification_name == source_name]
    if not sources:
        return "skipped: no source instance", 0
    migrated = 0
    for source in sources:
        text = source.get_answer(attribute=source.ontology_item.attributes[0])
        if not text:
            continue
        # Source instances are global (range-only) classifications with no frame
        # annotations, so the target is placed over the whole video instead.
        target = target_cls.create_instance()
        target.set_answer(text, attribute=target_cls.attributes[0])
        target.set_for_frames(Range(start=0, end=row.number_of_frames - 1))
        row.add_classification_instance(target)
        migrated += 1
    if migrated == 0:
        return "skipped: source has no text", 0
    return f"migrated {migrated} instance(s)", migrated


def main() -> int:
    args = parse_args()
    project = load_client(args.ssh_key_env, args.domain).get_project(args.project_hash)

    target_cls = next(
        (c for c in project.ontology_structure.classifications if c.attributes[0].name == args.target_classification),
        None,
    )
    if target_cls is None:
        raise SystemExit(f"No classification named {args.target_classification!r} in the project ontology.")

    tasks = select_tasks(args)
    total = len(tasks)
    log(f"{total} task(s) selected for migration")
    needed_hashes = {t["data_hash"] for t in tasks}
    rows_by_hash = {row.data_hash: row for row in project.list_label_rows_v2() if row.data_hash in needed_hashes}

    app_domain = args.app_domain.rstrip("/")
    # Read bundles scale fine at 200; saves are unbundled (see module docstring) and
    # run sequentially within a chunk, so a smaller chunk keeps progress logging and
    # cross-thread load balancing granular despite that.
    chunk_size = 50
    # (index, row) pairs, so chunk order can be undone after threads complete out of order.
    indexed_rows = [(i, rows_by_hash[t["data_hash"]]) for i, t in enumerate(tasks)]
    chunks = [indexed_rows[start : start + chunk_size] for start in range(0, total, chunk_size)]
    rows_by_hash = None

    def migrate_chunk(chunk: list) -> list:
        rows = [row for _, row in chunk]
        with project.create_bundle(bundle_size=chunk_size) as bundle:
            for row in rows:
                row.initialise_labels(bundle=bundle, include_archived=True)
        stages, statuses, counts = [], [], []
        for row in rows:
            # The file's stage is a snapshot; tasks move between stages, so the
            # skip is re-checked against the live stage.
            stage = row.workflow_graph_node.title if row.workflow_graph_node else "(no stage)"
            stages.append(stage)
            if args.skip_stages and stage in args.skip_stages:
                statuses.append(f"skipped: now in {stage}")
                counts.append(0)
            else:
                status, count = migrate_row(row, args.source_classification, target_cls)
                statuses.append(status)
                counts.append(count)
        # Bundled saves do not scale (see module docstring), so each row saves
        # individually rather than through project.create_bundle().
        for row, status in zip(rows, statuses):
            if status.startswith("migrated"):
                row.save()
        chunk_results = [
            (
                index,
                row.data_hash,
                row.data_title,
                stage,
                status,
                count,
                f"{app_domain}/label_editor/{args.project_hash}/{row.data_hash}/0",
            )
            for (index, row), stage, status, count in zip(chunk, stages, statuses, counts)
        ]
        chunk.clear()
        return chunk_results

    done = 0
    results = []
    with open(args.migrated_log, "a") as log_file, ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(migrate_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunk_results = future.result()
            results.extend(chunk_results)
            for _, data_hash, data_title, stage, status, count, _ in chunk_results:
                if count > 0:
                    record = {
                        "data_hash": data_hash,
                        "data_title": data_title,
                        "stage": stage,
                        "source_classification": args.source_classification,
                        "target_classification": args.target_classification,
                        "instances_migrated": count,
                        "migrated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    log_file.write(json.dumps(record) + "\n")
            log_file.flush()
            done += len(chunk_results)
            log(f"{done}/{total} processed")

    results.sort(key=lambda r: r[0])
    rows_table = [(data_hash, stage, status, link) for _, data_hash, _, stage, status, _, link in results]
    header = ("Data hash", "Stage", "Status", "Link")
    widths = [max(len(str(r[i])) for r in [header] + rows_table) for i in range(4)]
    for line in [header] + rows_table:
        print("  ".join(str(cell).ljust(width) for cell, width in zip(line, widths)).rstrip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
