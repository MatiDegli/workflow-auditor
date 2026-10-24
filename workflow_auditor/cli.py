from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import argparse
import json
import sys

from .adapters import ingest, validate_config
from .labels import read_labels
from .model import timestamp
from .query import Selection, compare_periods, report, series
from .store import Store


def load(path: Path) -> dict:
    result = json.loads(path.read_text())
    if not isinstance(result, dict):
        raise ValueError("JSON document must be an object")
    return result


def selection(args: argparse.Namespace) -> Selection:
    groups = () if args.command == "series" and args.group_by == "" else tuple(args.group_by.split(","))
    labels = read_labels(args.labels) if getattr(args, "labels", None) else None
    return Selection(timestamp(args.since), timestamp(args.until), tuple(args.repo or []), tuple(args.provider or []),
                     tuple(args.role or []), tuple(args.agent or []), tuple(args.model or []), args.descendants,
                     groups, args.activity, args.bucket_seconds, labels, getattr(args, "label", None))


def markdown(result: dict) -> str:
    def display(value):
        return "unknown" if value is None else f"{Decimal(str(value)):.6f}"

    def costs(row, hourly=False):
        lo, hi = ("per_agent_hour_min", "per_agent_hour_max") if hourly else ("known_min", "known_max")
        zero = "no events" if not row["events"] else "0 (zero usage)" if row["zero_usage_events"] and not row["unpriced_events"] else "unpriced"
        return "; ".join(f"{q['unit']} {display(q[lo])}–{display(q[hi])}" for q in row['list_estimates']) or zero

    if result.get("schema") == "workflow_auditor.compare.v1":
        return "# Period comparison\n\n## Before\n\n" + markdown(result["before"]) + "\n## After\n\n" + markdown(result["after"]) + "\n" + result["interpretation"] + "\n"
    if result.get("schema") == "workflow_auditor.series.v1":
        grouped = bool(result["periods"]) and "groups" in result["periods"][0]
        head = (["Group"] if grouped else []) + ["Local start", "Status", "Events", "Agent-hours",
                                                "Input tokens", "Output tokens", "List estimate"]
        lines = ["# Workflow accounting by period", "",
                 f"Zone: {result['tz']}; every {result['every']}. Registered sources only; estimates are not billing.", "",
                 f"UTC range: [{result['since_utc']}, {result['until_exclusive_utc']}). Each period is the report over that window; edge periods are clipped to the range.", "",
                 "| " + " | ".join(head) + " |",
                 "| " + " | ".join("---:" if column in ("Events", "Agent-hours", "Input tokens", "Output tokens") else "---"
                                   for column in head) + " |"]
        for period in result["periods"]:
            # Group entries carry their metrics inline; a period with no group keeps one row, with
            # an empty group column, so its status still shows.
            for row in (period["groups"] or [None]) if grouped else [None]:
                summary = row if row is not None else period["summary"]
                cells = ([", ".join(f"{k}={v}" for k, v in row["group"].items()).replace("|", "\\|") if row is not None else ""]
                         if grouped else []) + [
                    period["start_local"] + ("*" if period["clipped"] else ""), period["status"], str(summary["events"]),
                    display(summary["activity"]["agent_hours"]), str(summary["tokens"]["input_total"]),
                    str(summary["tokens"]["output_total"]), costs(summary)]
                lines.append("| " + " | ".join(cells) + " |")
        lines += ["", "`*` marks a period clipped to the range.",
                  "For field-level coverage and provenance, use the JSON series report."]
        return "\n".join(lines) + "\n"
    if "summary" not in result:
        return "```json\n" + json.dumps(result, indent=2) + "\n```\n"
    summary = result["summary"]
    hours = summary["activity"]
    lines = ["# Workflow accounting", "", f"Status: {result['status']}. Registered sources only; estimates are not billing.", "",
             f"UTC window: [{result['selection']['since_utc']}, {result['selection']['until_exclusive_utc']}). Unbounded endpoints are null.", "",
             f"Activity method: {hours['method']}. Parallel agents add to agent-hours.",
             f"Agent-hours: {display(hours['agent_hours'])}; known subtotal: {hours['known_agent_hours']:.6f}.",
             f"Project activity hours: {display(hours['project_active_hours'])}; parallelism: {display(hours['parallelism'])}.", "",
             "| Group | Events | Agent-hours | Input tokens | Output tokens | List estimate | List estimate / agent-hour |",
             "| --- | ---: | ---: | ---: | ---: | --- | --- |"]
    for row in result["groups"]:
        label = ", ".join(f"{k}={v}" for k, v in row["group"].items()).replace("|", "\\|")
        lines.append(f"| {label} | {row['events']} | {display(row['activity']['agent_hours'])} | {row['tokens']['input_total']} | {row['tokens']['output_total']} | {costs(row)} | {costs(row, True)} |")
    lines += ["", "List estimates:", ""]
    for estimate in summary["list_estimates"]:
        lines.append(f"- {estimate['unit']}: {display(estimate['known_min'])}–{display(estimate['known_max'])}; per agent-hour: {display(estimate['per_agent_hour_min'])}–{display(estimate['per_agent_hour_max'])}.")
    if not summary["list_estimates"]:
        lines.append(f"- {costs(summary)}.")
    lines += [f"- Unpriced events: {summary['unpriced_events']}; zero-usage events priced at 0: {summary['zero_usage_events']}.",
              f"- Aggregates/agents without activity evidence: {len(hours['agents_or_aggregates_missing_activity'])}.",
              f"- Omitted by time/boundary/filter: {json.dumps(result['coverage']['omitted'])}.",
              f"- Unavailable primary sources: {result['coverage']['unavailable_primary_sources']}; source issues: {len(result['coverage']['source_issues'])}; conflicts: {len(result['coverage']['conflicts'])}; copied responses reattributed: {result['coverage']['copied_responses_reattributed']}.",
              f"- Copies that disagree on speed or geo, unpriced: {len(result['coverage']['speed_geo_mismatches'])}.",
              f"- Group activity totals additive for this selection: {result['group_activity_totals_are_additive']}. Use the global summary when false."]
    labels = result["coverage"].get("labels")
    if labels is not None:
        lines.append(f"- Labels: {labels['unlabeled_events']} unlabeled "
                     f"{'event' if labels['unlabeled_events'] == 1 else 'events'}; "
                     f"{labels['overlapping_events']} matched more than one label.")
    lines += ["", "For field-level coverage and provenance, use the JSON report."]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline multi-provider workflow accounting. No provider calls.")
    commands = parser.add_subparsers(dest="command", required=True)
    imp = commands.add_parser("ingest", help="Import registered local sources as accounting metadata")
    imp.add_argument("--config", type=Path, required=True)
    imp.add_argument("--db", type=Path, required=True)
    init = commands.add_parser("init-config", help="Write an editable source configuration; does not import")
    init.add_argument("--output", type=Path, required=True)
    for command in ("report", "compare", "series"):
        sub = commands.add_parser(command)
        sub.add_argument("--db", type=Path, required=True)
        sub.add_argument("--since", help="Inclusive ISO timestamp, explicit UTC offset required")
        sub.add_argument("--until", help="Exclusive ISO timestamp, explicit UTC offset required")
        for option in ("repo", "provider", "role", "agent", "model"):
            sub.add_argument("--" + option, action="append")
        sub.add_argument("--descendants", action="store_true", help="Include known descendants of --agent")
        sub.add_argument("--group-by", default="" if command == "series" else "repo,provider,role",
                         help="Comma-separated: repo,provider,role,agent,model,client,run,label (label needs --labels)")
        sub.add_argument("--activity", choices=("buckets", "observed"), default="buckets")
        sub.add_argument("--bucket-seconds", type=int, default=600)
        sub.add_argument("--prices", type=Path, help="Explicit revaluation using another dated price book")
        sub.add_argument("--format", choices=("json", "markdown"), default="json")
        sub.add_argument("--output", type=Path)
        if command in ("report", "series"):
            sub.add_argument("--labels", type=Path, help="Per-user task labels file (workflow_auditor.labels.v1)")
            sub.add_argument("--label", help="Keep only events assigned this task label; requires --labels")
        if command == "series":
            sub.add_argument("--every", help="Period unit: <n>min with n dividing 60, <n>h with n dividing 24, day, week or month")
            sub.add_argument("--tz", default="UTC",
                             help="IANA zone name or fixed offset +HH:MM / -HH:MM; write a negative offset attached: --tz=-03:00")
        if command == "compare":
            for option in ("before-start", "before-end", "after-start", "after-end"):
                sub.add_argument("--" + option, required=True)
    args = parser.parse_args(argv)
    store = None
    try:
        if args.command == "init-config":
            if args.output.exists():
                raise ValueError("Configuration already exists")
            result = {"sources": [
                {"name": "claude-wsl", "kind": "claude", "roots": ["~/.claude/projects"]},
                {"name": "codex-wsl", "kind": "codex", "roots": ["~/.codex/sessions"]}],
                "repo_aliases": {}, "agent_roles": {}, "type_roles": {},
                "price_book": str(Path(__file__).resolve().parent.parent / "prices-20261022.json"),
                "notes": "Add explicit Windows roots and GLM report/router sources. Unclassified roles stay unknown. Router is corroborating by default."}
        elif args.command == "ingest":
            config = load(args.config)
            book = config.get("price_book")
            if book:
                path = Path(book).expanduser()
                config["prices"] = load(path if path.is_absolute() else args.config.parent / path)
            validate_config(config)
            store = Store(args.db)
            result = ingest(store, config)
        else:
            store = Store(args.db, readonly=True)
            sel = selection(args)
            prices = load(args.prices) if args.prices else None
            if args.command == "report":
                result = report(store, sel, prices)
            elif args.command == "series":
                if not args.every:
                    raise ValueError("--every is required for series")
                result = series(store, sel, args.every, args.tz, prices)
            else:
                before = replace(sel, since=timestamp(args.before_start), until=timestamp(args.before_end))
                after = replace(sel, since=timestamp(args.after_start), until=timestamp(args.after_end))
                result = compare_periods(store, before, after, prices)
        text = markdown(result) if getattr(args, "format", "json") == "markdown" else json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        output = getattr(args, "output", None)
        if output:
            output.write_text(text)
        else:
            sys.stdout.write(text)
        return 0
    except (ValueError, OSError, TypeError, KeyError) as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)
        return 2
    finally:
        if store:
            store.close()
