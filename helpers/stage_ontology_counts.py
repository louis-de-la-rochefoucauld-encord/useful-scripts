"""Count tasks per Workflow stage, broken down by the ontology items in their labels.

Prints one row per stage: total tasks, then one column per ontology object or
classification, counting tasks whose labels contain at least one instance of it.
Instances of archived ontology items are included. Stages passed via
``--skip-stages`` are dropped before any label download, so their tasks cost
nothing to skip.

    uv run helpers/stage_ontology_counts.py \\
        --project-hash 00000000-0000-0000-0000-000000000000 \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api.encord.com

Label rows and instances reference:
https://docs.encord.com/sdk-documentation/sdk-labels/sdk-working-with-labels
Workflow stages reference:
https://docs.encord.com/sdk-documentation/projects-sdk/sdk-workflows-stages
Bundle reference (batches the label downloads):
https://docs.encord.com/sdk-documentation/sdk-references/http.bundle
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from utils import add_connection_args, load_client  # noqa: E402


def log(msg: str) -> None:
    print(f"[stage_ontology_counts] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-hash", required=True, help="Project to inspect")
    parser.add_argument(
        "--per-task-jsonl",
        help="Also write one JSON line per task (data_hash, title, stage, instance count "
        "per ontology item) to this file, for aggregations beyond the printed table.",
    )
    parser.add_argument(
        "--skip-stages",
        nargs="+",
        help="Workflow stages to exclude before downloading labels, e.g. Complete Archive",
    )
    add_connection_args(parser)
    return parser.parse_args()


def print_table(stage_order: list, stage_totals: Counter, counts: dict, item_names: list) -> None:
    header = ["Stage", "Tasks"] + item_names
    rows = [header]
    for stage in stage_order:
        rows.append(
            [stage, str(stage_totals[stage])] + [str(counts[stage].get(name, 0)) for name in item_names]
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for i, row in enumerate(rows):
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip())
        if i == 0:
            print("  ".join("-" * width for width in widths))


def main() -> int:
    args = parse_args()
    project = load_client(args.ssh_key_env, args.domain).get_project(args.project_hash)

    label_rows = project.list_label_rows_v2()
    log(f"{project.title}: {len(label_rows)} task(s)")

    if args.skip_stages:
        skip = set(args.skip_stages)
        label_rows = [
            row
            for row in label_rows
            if (row.workflow_graph_node.title if row.workflow_graph_node else "(no stage)") not in skip
        ]
        log(f"{len(label_rows)} task(s) outside skipped stage(s) {sorted(skip)}")

    stage_totals: Counter = Counter()
    counts: dict = defaultdict(Counter)
    item_names: set = set()
    for row in label_rows:
        stage = row.workflow_graph_node.title if row.workflow_graph_node else "(no stage)"
        stage_totals[stage] += 1

    # A row without a label_hash has never been labelled, and initialising it would
    # create a label row server-side — skip those, they have nothing to count.
    to_initialise = [row for row in label_rows if row.label_hash is not None]
    total = len(to_initialise)
    log(f"Downloading labels for {total} task(s)...")
    # 200 rows per API call: the server can exceed the SDK's 180s read timeout on
    # larger batches for projects of this size. Chunks are downloaded on parallel
    # threads and tallied as they complete, so at most a few chunks of label data
    # are in memory at once — keeping all of them exhausts memory on large projects.
    chunk_size = 200
    chunks = [to_initialise[start : start + chunk_size] for start in range(0, total, chunk_size)]
    label_rows = None
    to_initialise = None

    def tally_chunk(chunk: list) -> list:
        with project.create_bundle(bundle_size=chunk_size) as bundle:
            for row in chunk:
                row.initialise_labels(bundle=bundle, include_archived=True)
        tallies = []
        for row in chunk:
            stage = row.workflow_graph_node.title if row.workflow_graph_node else "(no stage)"
            instances = Counter(instance.object_name for instance in row.get_object_instances())
            instances.update(instance.classification_name for instance in row.get_classification_instances())
            tallies.append((row.data_hash, row.data_title, stage, instances))
        # The chunks list in main() also references these rows; clear so the
        # downloaded labels can be garbage-collected once tallied.
        chunk.clear()
        return tallies

    jsonl_file = open(args.per_task_jsonl, "w") if args.per_task_jsonl else None
    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(tally_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            tallies = future.result()
            for data_hash, data_title, stage, instances in tallies:
                for name in instances:
                    counts[stage][name] += 1
                item_names |= instances.keys()
                if jsonl_file:
                    record = {"data_hash": data_hash, "data_title": data_title, "stage": stage, "items": instances}
                    jsonl_file.write(json.dumps(record) + "\n")
            done += len(tallies)
            if done % 2000 < chunk_size or done == total:
                log(f"{done}/{total} downloaded")
    if jsonl_file:
        jsonl_file.close()
        log(f"Per-task breakdown written to {args.per_task_jsonl}")

    stage_order = [stage.title for stage in project.workflow.stages if stage.title in stage_totals]
    stage_order += [stage for stage in stage_totals if stage not in stage_order]

    print_table(stage_order, stage_totals, counts, sorted(item_names))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
