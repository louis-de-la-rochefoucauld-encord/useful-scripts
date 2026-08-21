"""Compare tasks from chosen pt2 workflow stages against their stage in the walden project.

    python3 compare_tasks.py --stages "Review 1" "Complete"
"""

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

DOWNLOADS = Path.home() / "Downloads"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stages",
        nargs="+",
        default=["Complete", "Archive"],
        help="pt2 stage names whose tasks to look up in walden (default: 'Review 1' 'Complete')",
    )
    parser.add_argument("--pt2", type=Path, default=DOWNLOADS / "walden_production_pt2.json")
    parser.add_argument("--walden", type=Path, default=DOWNLOADS / "walden_production.json")
    parser.add_argument("--out", type=Path, default=DOWNLOADS / "walden_stage_comparison.csv")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pt2 = json.loads(args.pt2.read_text())
    walden = json.loads(args.walden.read_text())

    pt2_stage_names = {s["name"] for s in pt2.values()}
    unknown = set(args.stages) - pt2_stage_names
    if unknown:
        print(f"Unknown pt2 stage(s): {sorted(unknown)}. Available: {sorted(pt2_stage_names)}", file=sys.stderr)
        return 1

    by_hash = defaultdict(list)
    by_title = defaultdict(list)
    for stage in walden.values():
        for task in stage["tasks"]:
            by_hash[task["data_hash"]].append(stage["name"])
            by_title[task["data_title"]].append(stage["name"])

    source = [
        (stage["name"], task)
        for stage in pt2.values()
        if stage["name"] in args.stages
        for task in stage["tasks"]
    ]
    if not source:
        print("No tasks in the selected stages.", file=sys.stderr)
        return 1

    hash_hits = sum(1 for _, t in source if t["data_hash"] in by_hash)
    match_key = "data_hash" if hash_hits >= len(source) / 2 else "data_title"
    lookup = by_hash if match_key == "data_hash" else by_title
    print(f"match key: {match_key} (data_hash matched {hash_hits}/{len(source)})")

    crosstab = Counter()
    rows = []
    for pt2_stage, task in source:
        key = task["data_hash"] if match_key == "data_hash" else task["data_title"]
        stages = lookup.get(key, [])
        if not stages:
            label = "NOT FOUND"
        elif len(set(stages)) == 1:
            label = stages[0]
        else:
            label = "AMBIGUOUS: " + " | ".join(sorted(set(stages)))
        crosstab[(pt2_stage, label)] += 1
        rows.append(
            {
                "data_hash": task["data_hash"],
                "data_title": task["data_title"],
                "case_id": task["case_id"],
                "pt2_stage": pt2_stage,
                "walden_stage": label,
                "match_key": match_key,
            }
        )

    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nwrote {len(rows)} rows to {args.out}\n")
    print(f"{'pt2 stage':<12} {'walden stage':<40} count")
    for (p, w), n in sorted(crosstab.items(), key=lambda x: (-x[1], x[0])):
        print(f"{p:<12} {w:<40} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
