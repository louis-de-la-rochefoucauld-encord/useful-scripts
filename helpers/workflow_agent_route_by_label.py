"""Poll an Advanced custom workflow agent stage and route each task to a pathway
based on its labels or on the issues raised against it.

Ontology- and project-agnostic: the mapping from label title or issue tag to
pathway name is supplied via --label-pathway-map or --issue-pathway-map, so the
same script routes any project, workflow, or ontology combination without code
changes. Exactly one of the two maps must be given, and it selects the mode:

- --label-pathway-map: a task matches an entry when any of its labels carries
  that title — an object instance's title, or a selected option on a
  classification or object attribute (nested attributes included).
- --issue-pathway-map: a task matches an entry when any of its unresolved
  issues carries that issue tag. Resolved issues are ignored.

A task matching no map entry stays on the stage, as does a task matching
entries pointing at different pathways — routing it either way would be a
guess, so it is left for a human to resolve.

    uv run helpers/workflow_agent_route_by_label.py \\
        --project-hash d110c425-d84c-4ea8-9468-f4aaf628bd31 \\
        --agent-stage-name "Label Router" \\
        --label-pathway-map route_by_label.example.json \\
        --poll-interval-seconds 10 \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api.encord.com

Routing on issue tags instead, with a map like {"blurry": "Other"}:

    uv run helpers/workflow_agent_route_by_label.py \\
        --project-hash d110c425-d84c-4ea8-9468-f4aaf628bd31 \\
        --agent-stage-name "Label Router" \\
        --issue-pathway-map issue_routes.json \\
        --poll-interval-seconds 10 \\
        --ssh-key-env ENCORD_SDK_KEY \\
        --domain https://api.encord.com

Pass --once to route the tasks currently on the stage and exit, instead of
polling forever.

Task agents and pathways: https://docs.encord.com/sdk-documentation/projects-sdk/sdk-task-agents
Workflow stages (get_stage, get_tasks): https://docs.encord.com/sdk-documentation/projects-sdk/sdk-workflows-stages
Label rows, instances and answers: https://docs.encord.com/sdk-documentation/sdk-labels/sdk-working-with-labels
Issues and issue tags on tasks (task.issues.list()):
https://docs.encord.com/sdk-documentation/projects-sdk/sdk-projects-issues-comments
Bundle reference (batches the label downloads and pathway actions):
https://docs.encord.com/sdk-documentation/sdk-references/http.bundle
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from utils import add_connection_args, load_client  # noqa: E402

from encord.objects.common import Option
from encord.workflow import AgentStage

CHUNK = 100


def log(msg: str) -> None:
    print(f"[{Path(__file__).stem}] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-hash", required=True, help="Project holding the agent stage")
    parser.add_argument(
        "--agent-stage-name",
        required=True,
        help="Title of the Advanced custom workflow agent stage to poll",
    )
    map_group = parser.add_mutually_exclusive_group(required=True)
    map_group.add_argument(
        "--label-pathway-map",
        type=Path,
        help='JSON file mapping a label title to the pathway it should take, '
        'e.g. {"Stop Sign": "Red", "One Way": "Blue"}',
    )
    map_group.add_argument(
        "--issue-pathway-map",
        type=Path,
        help='JSON file mapping an issue tag to the pathway it should take, '
        'e.g. {"blurry": "Other", "occluded": "Red"}. Only unresolved issues count.',
    )
    parser.add_argument(
        "--poll-interval-seconds",
        required=True,
        type=float,
        help="Seconds to wait between polls; ignored with --once",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Route the tasks currently on the stage once, then exit, instead of polling forever",
    )
    add_connection_args(parser)
    return parser.parse_args()


def resolve_agent_stage(workflow, name: str) -> AgentStage:
    try:
        return workflow.get_stage(name=name, type_=AgentStage)
    except ValueError:
        available = ", ".join(stage.title for stage in workflow.stages if isinstance(stage, AgentStage))
        raise SystemExit(f"No agent stage named {name!r}. Available agent stages: {available}")


def answered_option_titles(instance) -> set:
    """Titles of every option selected on an instance's attributes, walking into
    attributes nested under selected options.
    """
    titles = set()

    def walk(attributes):
        for attribute in attributes:
            answer = instance.get_answer(attribute=attribute)
            for option in answer if isinstance(answer, list) else [answer]:
                if isinstance(option, Option):
                    titles.add(option.title)
                    walk(option.attributes)

    walk(instance.ontology_item.attributes)
    return titles


def pathways_from_labels(row, pathway_map: dict) -> set:
    titles = set()
    for instance in row.get_classification_instances():
        titles |= answered_option_titles(instance)
    for instance in row.get_object_instances():
        titles.add(instance.object_name)
        titles |= answered_option_titles(instance)
    return {pathway_map[title] for title in titles if title in pathway_map}


def pathways_from_issues(task, pathway_map: dict) -> set:
    """Pathways matching the issue tags on the task's unresolved issues.

    An issue's resolution can be toggled, so the latest entry in its resolution
    history decides whether it currently counts.
    """
    tags = set()
    for issue in task.issues.list():
        history = issue.resolution_history
        if history and max(history, key=lambda r: r.created_at).is_resolved:
            continue
        tags.update(tag.name for tag in issue.tags)
    return {pathway_map[tag] for tag in tags if tag in pathway_map}


def route_once(project, agent_stage: AgentStage, pathway_map: dict, by_issues: bool, skips_logged: set) -> int:
    def skip(task, reason: str) -> None:
        key = (str(task.data_hash), reason)
        if key not in skips_logged:
            skips_logged.add(key)
            log(f"skipping {task.data_title} ({task.data_hash}): {reason}")

    tasks = list(agent_stage.get_tasks())
    if not tasks:
        return 0

    rows = {}
    if not by_issues:
        data_hashes = [str(task.data_hash) for task in tasks]
        for i in range(0, len(data_hashes), CHUNK):
            for row in project.list_label_rows_v2(data_hashes=data_hashes[i : i + CHUNK]):
                rows[row.data_hash] = row
        # A row without a label_hash has never been labelled, and initialising it
        # would create a label row server-side — leave those untouched on the stage.
        labelled = [row for row in rows.values() if row.label_hash is not None]
        for i in range(0, len(labelled), CHUNK):
            with project.create_bundle() as bundle:
                for row in labelled[i : i + CHUNK]:
                    row.initialise_labels(bundle=bundle)

    to_route = []
    for task in tasks:
        if by_issues:
            pathways = pathways_from_issues(task, pathway_map)
            no_match_reason = "no unresolved issue tag matches the issue-pathway map"
        else:
            row = rows.get(str(task.data_hash))
            if row is None or row.label_hash is None:
                skip(task, "not labelled yet")
                continue
            pathways = pathways_from_labels(row, pathway_map)
            no_match_reason = "no label matches the label-pathway map"
        if not pathways:
            skip(task, no_match_reason)
        elif len(pathways) > 1:
            skip(task, f"matches multiple pathways: {', '.join(sorted(pathways))}")
        else:
            to_route.append((task, pathways.pop()))

    for i in range(0, len(to_route), CHUNK):
        group = to_route[i : i + CHUNK]
        with project.create_bundle() as bundle:
            for task, pathway in group:
                task.proceed(pathway_name=pathway, bundle=bundle)
        for task, pathway in group:
            log(f"{task.data_title}: routed to {pathway!r}")
    return len(to_route)


def main() -> int:
    args = parse_args()
    by_issues = args.issue_pathway_map is not None
    pathway_map = json.loads((args.issue_pathway_map or args.label_pathway_map).read_text())

    project = load_client(args.ssh_key_env, args.domain).get_project(args.project_hash)
    agent_stage = resolve_agent_stage(project.workflow, args.agent_stage_name)

    pathway_names = {pathway.name for pathway in agent_stage.pathways}
    unknown = set(pathway_map.values()) - pathway_names
    if unknown:
        raise SystemExit(
            f"The pathway map routes to pathway(s) not on {agent_stage.title!r}: "
            f"{', '.join(sorted(unknown))}. Available pathways: {', '.join(sorted(pathway_names))}"
        )

    skips_logged: set = set()
    if args.once:
        routed = route_once(project, agent_stage, pathway_map, by_issues, skips_logged)
        log(f"done: routed {routed} task(s)")
        return 0

    log(f"polling {agent_stage.title!r} every {args.poll_interval_seconds}s (Ctrl+C to stop)")
    while True:
        routed = route_once(project, agent_stage, pathway_map, by_issues, skips_logged)
        if routed:
            log(f"routed {routed} task(s)")
        time.sleep(args.poll_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
