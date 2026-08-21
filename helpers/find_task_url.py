#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["encord"]
# ///
"""Print the label editor URL for a task, looked up by its data title.

Takes a task id — the data title of a file in an Encord project, for
cloud-registered files usually the object-store key (e.g.
``DataQC/videos/.../<name>_merged.mp4``) — finds the matching data unit in the
project, and prints ``<domain>/label_editor/<project_hash>/<data_hash>``.

The title is matched exactly first; if nothing matches and the task id
contains ``/``, the trailing file name is tried, since cloud-registered files
default to being titled by file name.
"""

import argparse
import os
import sys

from encord.user_client import EncordUserClient


def load_client(env_var: str, domain: str) -> EncordUserClient:
    value = os.environ.get(env_var)
    if not value:
        raise SystemExit(
            f'Environment variable {env_var} is not set. Export it with your Encord SSH '
            f'private key contents, e.g. {env_var}="$(cat key.ed25519)".'
        )
    return EncordUserClient.create_with_ssh_private_key(ssh_private_key=value, domain=domain)


def find_rows(project, task_id: str):
    """Return (label rows matching the task id by title, title that matched).

    Title filtering is documented at
    https://docs.encord.com/sdk-documentation/sdk-references/project
    (``list_label_rows_v2``, ``data_title_eq``).
    """
    rows = project.list_label_rows_v2(data_title_eq=task_id)
    if rows:
        return rows, task_id
    file_name = task_id.rsplit("/", 1)[-1]
    if file_name != task_id:
        rows = project.list_label_rows_v2(data_title_eq=file_name)
        if rows:
            return rows, file_name
    return [], task_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ssh-key-env",
        required=True,
        help="Name of the environment variable holding your Encord SSH private key contents.",
    )
    parser.add_argument(
        "--project-hash",
        required=True,
        help="Hash of the project containing the task.",
    )
    parser.add_argument(
        "--task-id",
        required=True,
        help="Data title of the task's file, e.g. its object-store key.",
    )
    parser.add_argument(
        "--domain",
        default="https://app.encord.com",
        help="Encord web app domain, used for the printed URL. Default: %(default)s",
    )
    parser.add_argument(
        "--api-domain",
        default="https://api.encord.com",
        help="Encord API domain, used for the SDK connection. Default: %(default)s",
    )
    args = parser.parse_args()

    domain = args.domain.rstrip("/")
    client = load_client(args.ssh_key_env, args.api_domain)
    project = client.get_project(args.project_hash)

    rows, matched_title = find_rows(project, args.task_id)
    if not rows:
        raise SystemExit(
            f"No data unit titled {args.task_id!r} (or its file name) in project {project.title!r}."
        )
    if matched_title != args.task_id:
        print(f"No exact title match; matched file name {matched_title!r}.", file=sys.stderr)
    for row in rows:
        url = f"{domain}/label_editor/{args.project_hash}/{row.data_hash}/0"
        print(f"\n\033]8;;{url}\033\\{url}\033]8;;\033\\")


if __name__ == "__main__":
    main()
