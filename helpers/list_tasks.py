"""Print each Workflow stage in a project with the tasks queued there (data hash + title).

Use this to find the data hash to pass to apply_pre_labels.py, and to check a task is in
an Agent stage before writing to it.

    uv run helpers/list_tasks.py \\
        --project-hash 00000000-0000-0000-0000-000000000000 \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api-encord.optum.com \\
        > tasks.json

Workflow stages reference:
https://docs.encord.com/sdk-documentation/projects-sdk/sdk-workflows-stages
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from utils import add_connection_args, case_id_for_title, load_client  # noqa: E402


def log(msg: str) -> None:
    print(f"[list_tasks] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-hash", required=True, help="Project to inspect")
    add_connection_args(parser)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project = load_client(args.ssh_key_env, args.domain).get_project(args.project_hash)

    stages = {}
    for stage in project.workflow.stages:
        tasks = [
            {
                "data_hash": str(task.data_hash),
                "data_title": task.data_title,
                "case_id": case_id_for_title(task.data_title),
            }
            for task in stage.get_tasks()
        ]
        log(f"{stage.title} ({stage.stage_type.value}): {len(tasks)} task(s)")
        stages[str(stage.uuid)] = {
            "name": stage.title,
            "stage_type": stage.stage_type.value,
            "tasks": tasks,
        }
    # print(json.dumps(stages, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())