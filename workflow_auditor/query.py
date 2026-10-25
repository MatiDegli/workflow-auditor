from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import bisect
import math
import re

from .labels import UNLABELED, Labels
from .model import INFORMATIONAL, MISMATCH, Event, TOKEN_FIELDS, merge_intervals, money
from .store import Store


@dataclass(frozen=True)
class Selection:
    since: float | None = None
    until: float | None = None
    repos: tuple[str, ...] = ()
    providers: tuple[str, ...] = ()
    roles: tuple[str, ...] = ()
    agents: tuple[str, ...] = ()
    models: tuple[str, ...] = ()
    descendants: bool = False
    group_by: tuple[str, ...] = ("repo", "provider", "role")
    activity: str = "buckets"
    bucket_seconds: int = 600
    labels: Labels | None = None  # A per-user labels file; it only groups and filters.
    label: str | None = None  # Keep only events assigned this label name.

    def __post_init__(self) -> None:
        if self.since is not None and self.until is not None and self.since >= self.until:
            raise ValueError("since must be before until")
        if self.activity not in ("buckets", "observed"):
            raise ValueError("Unsupported activity method")
        if type(self.bucket_seconds) is not int or not 1 <= self.bucket_seconds <= 3600:
            raise ValueError("Bucket length must be an integer from 1 to 3600 seconds")
        if any(x not in ("repo", "provider", "role", "agent", "model", "client", "run", "label") for x in self.group_by):
            raise ValueError("Unsupported grouping")
        if "label" in self.group_by and self.labels is None:
            raise ValueError("--group-by label requires --labels")
        if self.label is not None and self.labels is None:
            raise ValueError("--label requires --labels")
        if self.label is not None:
            if not self.label:
                raise ValueError("--label needs a label name from the file, or 'unlabeled'")
            if self.label != UNLABELED and self.label not in {l.name for l in self.labels.labels}:
                raise ValueError(f"--label names no label in the file: {self.label!r}")
        if self.descendants and not self.agents:
            raise ValueError("Descendant selection requires an agent")


def agent_matches(value: str | None, query: str) -> bool:
    return value is not None and (value == query or value.endswith(":" + query))


def hierarchy(events: list[Event]) -> tuple[dict[str, set[str]], list[dict]]:
    children: dict[str, set[str]] = defaultdict(set)
    parents: dict[str, str | None] = {}
    issues = []
    for e in events:
        if not e.agent:
            continue
        if e.agent in parents and parents[e.agent] != e.parent:
            issues.append({"code": "agent_parent_conflict", "agent": e.agent})
        parents[e.agent] = e.parent
        if e.parent:
            children[e.parent].add(e.agent)
    cyclic = set()
    for agent in parents:
        path: set[str] = set()
        cursor: str | None = agent
        while cursor:
            if cursor in path:
                cyclic.add(cursor)
                break
            path.add(cursor)
            cursor = parents.get(cursor)
    issues.extend({"code": "hierarchy_cycle", "agent": x} for x in sorted(cyclic))
    return children, issues


def resolve_agents(events: list[Event], children: dict[str, set[str]], selection: Selection) -> set[str]:
    agents: set[str] = set()
    if selection.agents:
        candidates = {e.agent for e in events if e.agent} | set(children)
        agents = {a for a in candidates if any(agent_matches(a, query) for query in selection.agents)}
        if selection.descendants:
            pending = list(agents)
            while pending:
                for child in children.get(pending.pop(), set()):
                    if child not in agents:
                        agents.add(child)
                        pending.append(child)
    return agents


def excluded(event: Event, selection: Selection, agents: set[str]) -> bool:
    return bool((selection.repos and event.repo not in selection.repos) or
                (selection.providers and event.provider not in selection.providers) or
                (selection.roles and event.role not in selection.roles) or
                (selection.models and event.model not in selection.models) or
                (selection.agents and event.agent not in agents))


def select(events: list[Event], selection: Selection, assignment=None) -> tuple[list[Event], dict, list[dict]]:
    children, issues = hierarchy(events)
    agents = resolve_agents(events, children, selection)
    if assignment is None and selection.labels is not None:
        assignment = selection.labels.assign(events)  # Once per event, on the deduplicated events.
    selected = []
    omitted = {"outside_window": 0, "unknown_time": 0, "aggregate_crosses_boundary": 0, "filters": 0}
    for e in events:
        if excluded(e, selection, agents) or (selection.label is not None and assignment.of(e) != selection.label):
            omitted["filters"] += 1
            continue
        if selection.since is not None or selection.until is not None:
            if e.time_kind == "point":
                if e.at is None:
                    omitted["unknown_time"] += 1
                    continue
                if (selection.since is not None and e.at < selection.since) or (selection.until is not None and e.at >= selection.until):
                    omitted["outside_window"] += 1
                    continue
            else:
                if e.start is None or e.end is None:
                    omitted["unknown_time"] += 1
                    continue
                if (selection.since is not None and e.end <= selection.since) or (selection.until is not None and e.start >= selection.until):
                    omitted["outside_window"] += 1
                    continue
                if (selection.since is not None and e.start < selection.since) or (selection.until is not None and e.end > selection.until):
                    omitted["aggregate_crosses_boundary"] += 1
                    continue
        selected.append(e)
    return selected, omitted, issues


def activity(events: list[Event], selection: Selection, trustworthy: bool = True) -> dict:
    # Totals need every selected event's activity and a trustworthy selection; known subtotals always show.
    intervals: dict[str, list[tuple[float, float]]] = defaultdict(list)
    missing: set[str] = set()
    for e in events:
        interval = None
        if e.agent and selection.activity == "observed" and e.activity_basis == "own" and e.activity_start is not None:
            interval = (e.activity_start, e.activity_end)
        elif e.agent and selection.activity == "buckets" and e.time_kind == "point" and e.at is not None:
            start = math.floor(e.at / selection.bucket_seconds) * selection.bucket_seconds
            interval = (start, start + selection.bucket_seconds)
        if interval is None:
            missing.add(e.agent or "aggregate:" + e.run)
            continue
        start, end = interval
        start = max(start, selection.since) if selection.since is not None else start
        end = min(end, selection.until) if selection.until is not None else end
        if end > start:
            intervals[e.agent].append((start, end))
    merged = {a: merge_intervals(values) for a, values in intervals.items()}
    seconds = {a: sum(end - start for start, end in values) for a, values in merged.items()}
    agent_hours = sum(seconds.values()) / 3600
    project = sum(b - a for a, b in merge_intervals([i for values in merged.values() for i in values])) / 3600
    complete = trustworthy and not missing
    return {"method": "occupied_request_buckets_v1" if selection.activity == "buckets" else "observed_own_intervals_v1",
            "bucket_seconds": selection.bucket_seconds if selection.activity == "buckets" else None,
            "estimated": selection.activity == "buckets", "known_agent_hours": agent_hours,
            "agent_hours": agent_hours if complete else None,
            "project_active_hours": project if complete else None,
            "known_project_active_hours": project, "agents_with_activity": len(merged),
            "agents_or_aggregates_missing_activity": sorted(missing),
            "parallelism": agent_hours / project if project and complete else None}


def value(event: Event, book: dict) -> tuple[str, Decimal, Decimal] | None:
    spec = book.get("models", {}).get(event.model)
    if spec is None or event.tokens.fresh is None or event.tokens.output_total is None or MISMATCH in (event.speed, event.geo):
        return None
    tiers = spec["tiers"]
    if event.time_kind != "aggregate":
        tiers = [p for p in tiers if p.get("min_context", 0) <= event.tokens.input_total <= p.get("max_context", float("inf"))]
    if not tiers:
        return None
    t = event.tokens
    costs = []
    for p in tiers:
        w5, w1 = t.write_5m or 0, t.write_1h or 0
        unknown = (t.cache_write or 0) - w5 - w1
        parts = [(t.fresh, p["input"]), (t.cache_read or 0, p["read"]), (t.output_total, p["output"]),
                 (w5, p.get("write_5m", p["input"])), (w1, p.get("write_1h", p["input"]))]
        base = sum(Decimal(n) * Decimal(money(rate)) for n, rate in parts)
        wrates = [Decimal(money(p.get(k, p["input"]))) for k in ("write_5m", "write_1h")]
        factor = Decimal(2 if event.speed == "fast" else 1) if event.client == "claude_code" else Decimal(1)
        if event.client == "claude_code" and event.geo == "us":
            factor *= Decimal("1.1")
        costs += [(base + Decimal(unknown) * min(wrates)) / 1000000 * factor,
                  (base + Decimal(unknown) * max(wrates)) / 1000000 * factor]
    return spec["unit"], min(costs), max(costs)


def metrics(events: list[Event], selection: Selection, book: dict, trustworthy: bool) -> dict:
    totals, known, missing_counts = {}, {}, {}
    for field_name in TOKEN_FIELDS:
        values = [getattr(e.tokens, field_name) for e in events]
        known[field_name] = sum(v for v in values if v is not None)
        missing_counts[field_name] = sum(v is None for v in values)
        totals[field_name] = known[field_name] if not missing_counts[field_name] and trustworthy else None
    hours = activity(events, selection, trustworthy)
    denominator = hours["agent_hours"]
    prices: dict[str, list[Decimal]] = defaultdict(lambda: [Decimal(0), Decimal(0), Decimal(0)])
    zero, unpriced = 0, 0
    for e in events:
        # Known zero usage costs 0 whatever the model; reasoning and TTL splits are
        # bounded by these counters. Such events are counted once, in zero_usage_events, and in no
        # unit's priced_events. Any unknown counter keeps the event unpriced.
        if all(getattr(e.tokens, f) == 0 for f in ("input_total", "cache_read", "cache_write", "output_total")):
            zero += 1
            continue
        quote = value(e, book)
        if quote is None:
            unpriced += 1
        else:
            unit, low, high = quote
            prices[unit][0] += low
            prices[unit][1] += high
            prices[unit][2] += 1
    quotes = [{"unit": unit, "known_min": str(values[0]), "known_max": str(values[1]),
               "priced_events": int(values[2]), "complete_for_selection": int(values[2]) + zero == len(events) and not unpriced and trustworthy,
               "per_agent_hour_min": str(values[0] / Decimal(str(denominator))) if denominator and not unpriced and trustworthy and len(prices) == 1 else None,
               "per_agent_hour_max": str(values[1] / Decimal(str(denominator))) if denominator and not unpriced and trustworthy and len(prices) == 1 else None}
              for unit, values in sorted(prices.items())]
    cli = [Decimal(e.cli_usd) for e in events if e.cli_usd is not None]
    return {"events": len(events), "event_units": dict(Counter(e.unit for e in events)),
            "agents": len({e.agent for e in events if e.agent}),
            "runs": len({e.run for e in events}), "tokens": totals, "known_token_subtotals": known,
            "events_missing_token_fields": missing_counts, "activity": hours,
            "input_tokens_per_agent_hour": totals["input_total"] / denominator if totals["input_total"] is not None and denominator else None,
            "output_tokens_per_agent_hour": totals["output_total"] / denominator if totals["output_total"] is not None and denominator else None,
            "cache_read_fraction": totals["cache_read"] / totals["input_total"] if totals["cache_read"] is not None and totals["input_total"] else None,
            "list_estimates": quotes, "unpriced_events": unpriced, "zero_usage_events": zero,
            "cli_recorded_usd_estimate": {"known_total": str(sum(cli)), "events": len(cli),
                                          "complete_for_selection": bool(events) and len(cli) == len(events) and trustworthy},
            "billed_cash": None, "subscription_quota_deduction": None,
            "accepted_outcomes": None, "cost_per_accepted_outcome": None}


def report(store: Store, selection: Selection, price_book: dict | None = None) -> dict:
    with store.read_snapshot():
        return _report(store, selection, price_book)


def _report(store: Store, selection: Selection, price_book: dict | None = None) -> dict:
    loaded = _load(store)
    book = price_book if price_book is not None else loaded.config.get("prices", {})
    assignment = selection.labels.assign(loaded.events) if selection.labels is not None else None
    selected, omitted, tree_issues = select(loaded.events, selection, assignment)
    return _view(loaded, selection, book, selected, omitted, tree_issues, price_book is not None, assignment)


@dataclass(frozen=True)
class _Loaded:
    """Everything a report reads from the store, loaded once and shared by every period."""
    events: list
    conflicts: list
    reattributed: set
    mismatched: dict
    config: dict
    sources: list
    source_issues: list
    bad_sources: list
    import_state: dict
    excluded_history: list
    rotated: dict
    inactive_history: list


def _load(store: Store) -> _Loaded:
    events, issues = store.events()
    conflicts = [i for i in issues if i["code"] not in INFORMATIONAL]
    reattributed = {i["event"] for i in issues if i["code"] == "copied_response_reattributed"}
    mismatched: dict[str, set[str]] = defaultdict(set)  # Response key -> its source files.
    for i in issues:
        if i["code"] == "speed_geo_mismatch":
            mismatched[i["event"]].update(i.get("sources", []))
    sources = store.sources()
    primary = [s for s in sources if s.get("primary", True)]
    from .retention import snapshot_problem
    config = store.config()
    registrations = {s["name"]: s for s in config.get("sources", [])}
    problems, counts = {}, {}
    for s in sources:
        if s["status"] != "imported" and ("receipt" in s or s["status"] == "rotated" or s.get("retention")):
            counts[s["id"]] = store.db.execute("SELECT count(*) FROM observations WHERE source_id=?", (s["id"],)).fetchone()[0]
            problem = snapshot_problem(s, registrations[s["registration"]], config, counts[s["id"]])
            if problem:
                problems[s["id"]] = problem
    rotated = [s for s in sources if s["status"] == "rotated"]
    rotation = dict(files=len(rotated), primary_files=sum(s.get("primary", True) for s in rotated),
                    observations=sum(counts[s["id"]] for s in rotated), reason="source_disappeared_after_snapshot",
                    attribution_mismatches=sum(p == "attribution_mismatch" for p in problems.values()),
                    adapter_version_mismatches=sum(p == "adapter_version_mismatch" for p in problems.values()),
                    unverified_receipts=sum(p not in ("attribution_mismatch", "adapter_version_mismatch") for p in problems.values()))
    excluded = [{"registration": s["registration"], "source_id": s["id"],
                 "reason": s.get("membership_exclusion", {}).get("reason", "unknown_legacy_retirement"),
                 **({"restore_refused": s["membership_exclusion"]["restore_refused"]}
                    if s.get("membership_exclusion", {}).get("restore_refused") else {}),
                 "observations": store.db.execute("SELECT count(*) FROM observations WHERE source_id=?", (s["id"],)).fetchone()[0]}
                for s in store.sources(include_retired=True) if s["status"] == "retired" and s["sha256"] is not None and s.get("primary", True)]
    return _Loaded(events, conflicts, reattributed, mismatched, store.config(), sources,
                   [{"source": s["path"], **issue} for s in primary for issue in s.get("issues", []) + s.get("collection_issues", [])
                    + ([{"code": problems[s["id"]]}] if s["id"] in problems else [])],
                   [s for s in primary if s["status"] != "imported" and not (s["status"] == "rotated"
                    and s["id"] not in problems and not s.get("collection_issues"))], store.import_state(),
                   sorted(excluded, key=lambda s: (s["registration"], s["source_id"])), rotation if rotated or problems else {},
                   store.inactive_registrations())


def _view(loaded: _Loaded, selection: Selection, book: dict, selected: list[Event],
          omitted: dict, tree_issues: list[dict], revalued: bool = False, assignment=None) -> dict:
    labels_entry = ({"file_sha256": selection.labels.sha256, "labels": len(selection.labels.labels),
                     **assignment.counts(selected)} if selection.labels is not None else None)
    # A run aggregate and its detailed requests are different views, not additive costs.
    # Without a verified join, withhold the ambiguous run rather than choosing a cheaper view.
    granularity: dict[tuple, set[str]] = defaultdict(set)
    for e in selected:
        granularity[(e.provider, e.run, e.model)].add(e.time_kind)
    overlap = {key for key, kinds in granularity.items() if kinds == {"point", "aggregate"}}
    withheld = [e for e in selected if (e.provider, e.run, e.model) in overlap]
    selected = [e for e in selected if (e.provider, e.run, e.model) not in overlap]
    trust = not (loaded.conflicts or loaded.source_issues or loaded.bad_sources or omitted["unknown_time"] or
                 omitted["aggregate_crosses_boundary"] or (selection.descendants and tree_issues) or
                 loaded.import_state["state"] != "complete" or overlap)
    grouped: dict[tuple, list[Event]] = defaultdict(list)
    for e in selected:
        grouped[tuple(assignment.of(e) if key == "label" else getattr(e, key) for key in selection.group_by)].append(e)
    groups = [{"group": dict(zip(selection.group_by, key)), **metrics(rows, selection, book, trust)}
              for key, rows in sorted(grouped.items(), key=lambda pair: str(pair[0]))]
    roots = [s for s in loaded.config.get("sources", [])]
    summary = metrics(selected, selection, book, trust)
    metric_limits = summary["activity"]["agent_hours"] is None or bool(summary["unpriced_events"])
    return {"schema": "workflow_auditor.report.v1", "status": ("ready_with_missing_metrics" if metric_limits else "ready") if trust else "partial",
            "selection": {"since_utc_epoch": selection.since, "until_exclusive_utc_epoch": selection.until,
                          "since_utc": datetime.fromtimestamp(selection.since, timezone.utc).isoformat() if selection.since is not None else None,
                          "until_exclusive_utc": datetime.fromtimestamp(selection.until, timezone.utc).isoformat() if selection.until is not None else None,
                          "repos": selection.repos, "providers": selection.providers, "roles": selection.roles,
                          "agents": selection.agents, "models": selection.models, "include_descendants": selection.descendants,
                          **({"label": selection.label} if selection.labels is not None else {})},
            "coverage": {"scope": "registered source roots only; not a global provider census", "registrations": len(roots),
                         "source_files": len(loaded.sources), "unavailable_primary_sources": len(loaded.bad_sources),
                         "corroborating_sources_excluded_from_sums": sum(not s.get("primary", True) for s in loaded.sources),
                         "omitted": omitted,
                         **({"retained_history_excluded": loaded.excluded_history} if loaded.excluded_history else {}),
                         **({"rotated_sources": loaded.rotated} if loaded.rotated else {}),
                         **({"inactive_registrations": loaded.inactive_history} if loaded.inactive_history else {}),
                         **({"labels": labels_entry} if labels_entry is not None else {}),
                         "conflicts": loaded.conflicts, "source_issues": loaded.source_issues,
                         "copied_responses_reattributed": sum(e.key in loaded.reattributed for e in selected),
                         "speed_geo_mismatches": [{"event": k, "sources": sorted(loaded.mismatched[k])}
                                                  for k in sorted({e.key for e in selected} & loaded.mismatched.keys())],
                         "hierarchy_issues": tree_issues, "import_state": loaded.import_state,
                         "aggregate_detail_overlaps": [list(x) for x in sorted(overlap)],
                         "withheld_overlap_events": len(withheld),
                         "rate_denominators_complete": trust and summary["activity"]["agent_hours"] is not None,
                         "unclassified_role_events": sum(e.role == "unknown" for e in selected),
                         "unattributed_repo_events": sum(e.repo == "unattributed" for e in selected)},
            "price_basis": {"as_of": book.get("as_of"), "source": book.get("source"),
                            "revaluation_override": revalued,
                            "is_billing": False},
            "summary": summary, "groups": groups,
            "group_activity_totals_are_additive": trust and all(g["activity"]["agent_hours"] is not None for g in groups) and
                math.isclose(sum(g["activity"]["known_agent_hours"] for g in groups), summary["activity"]["known_agent_hours"], abs_tol=1e-9),
            "provider_attribution": sorted({(e.provider, e.provider_basis) for e in selected}),
            "notes": ["Parallel agents add to agent-hours; each agent's overlapping intervals are merged.",
                      "Parent-inclusive wall durations are never imported as own active work.",
                      "Request buckets estimate activity, not human work or measured CPU time.",
                      "Activity breakdowns may overlap within one agent bucket (models, providers, roles or repos); use the recomputed global summary.",
                      "Aggregates crossing a date boundary are withheld, never prorated.",
                      "List estimates, CLI estimates, cash and subscription quota are separate measures."]}


MAX_PERIODS = 10000


def parse_every(spec: str | None) -> tuple[str, int]:
    if spec in ("day", "week", "month"):
        return (spec, 1)
    for pattern, unit, limit in ((r"([0-9]+)min", "min", 60), (r"([0-9]+)h", "hour", 24)):
        match = re.fullmatch(pattern, spec or "")
        if match and int(match.group(1)) and limit % int(match.group(1)) == 0:
            return (unit, int(match.group(1)))
    raise ValueError(f"Unsupported period unit: {spec}")


def parse_zone(name: str | None):
    if not name or name == "UTC":
        return timezone.utc
    match = re.fullmatch(r"([+-])(\d{2}):(\d{2})", name)
    if match:
        offset = timedelta(hours=int(match.group(2)), minutes=int(match.group(3)))
        if offset >= timedelta(days=1) or int(match.group(3)) >= 60:
            raise ValueError(f"Unknown time zone: {name}")
        return timezone(offset if match.group(1) == "+" else -offset)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:  # Also on systems without a time-zone database.
        raise ValueError(f"Unknown time zone: {name}") from exc


def _instants(wall: datetime, zone) -> set[float]:
    """The boundary instants of a local wall time: fold=0 always, plus fold=1 when that instant
    maps back to the same wall time, the only case where the wall time really occurs twice."""
    instants = {wall.replace(tzinfo=zone, fold=0).timestamp()}
    repeated = wall.replace(tzinfo=zone, fold=1).timestamp()
    if datetime.fromtimestamp(repeated, zone).replace(tzinfo=None) == wall:
        instants.add(repeated)
    return instants


def period_windows(kind: str, step: int, zone, since: float, until: float) -> list[tuple[float, float, bool]]:
    """Half-open local periods [b_i, b_{i+1}) over [since, until), clipped and flagged at the range edges.

    A repeated local hour makes two boundaries, so it is two periods. A wall time skipped at a
    spring-forward transition resolves to the transition's instant, where the next real boundary
    also lands, so it yields no period of its own; a gap of another length still leaves the day
    boundaries on midnights (Santiago's skipped midnight starts the day at 01:00)."""
    try:
        start_local = datetime.fromtimestamp(since, zone).replace(tzinfo=None)
        if kind == "min":
            first = start_local.replace(second=0, microsecond=0) - timedelta(minutes=start_local.minute % step)
        elif kind == "hour":
            first = start_local.replace(minute=0, second=0, microsecond=0) - timedelta(hours=start_local.hour % step)
        elif kind == "week":
            first = start_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=start_local.weekday())
        else:  # day, month
            first = start_local.replace(day=1 if kind == "month" else start_local.day, hour=0, minute=0, second=0, microsecond=0)
        wall = {"min": timedelta(minutes=step), "hour": timedelta(hours=step),
                "day": timedelta(days=step), "week": timedelta(weeks=step)}.get(kind)
        cursor, walls = first, [first]
        while min(_instants(cursor, zone)) < until:
            if kind == "month":
                cursor = cursor.replace(year=cursor.year + cursor.month // 12, month=cursor.month % 12 + 1)
            else:
                cursor += wall
            walls.append(cursor)
            if len(walls) > 2 * MAX_PERIODS:
                break  # Enough boundaries to decide; the exact check on periods is below.
    except (OverflowError, ValueError) as exc:
        raise ValueError("Range too large for this unit") from exc
    boundaries = sorted({t for w in walls for t in _instants(w, zone)})
    windows = [(b0, b1) for b0, b1 in zip(boundaries, boundaries[1:]) if b1 > since and b0 < until]
    if len(windows) > MAX_PERIODS:
        raise ValueError(f"More than {MAX_PERIODS} periods; choose a coarser --every")
    return [(max(b0, since), min(b1, until), b0 < since or b1 > until) for b0, b1 in windows]


def series(store: Store, selection: Selection, every: str, tz: str | None, price_book: dict | None = None) -> dict:
    with store.read_snapshot():
        return _series(store, selection, every, tz, price_book)


def _series(store: Store, selection: Selection, every: str, tz: str | None, price_book: dict | None = None) -> dict:
    if selection.since is None or selection.until is None:
        raise ValueError("--since and --until are required for series")
    kind, step = parse_every(every)
    zone = parse_zone(tz)
    windows = period_windows(kind, step, zone, selection.since, selection.until)
    loaded = _load(store)
    book = price_book if price_book is not None else loaded.config.get("prices", {})
    assignment = selection.labels.assign(loaded.events) if selection.labels is not None else None
    children, tree_issues = hierarchy(loaded.events)
    agents = resolve_agents(loaded.events, children, selection)
    # Period-independent filters run once; per period, point events come from one binary search
    # and only aggregates and untimed events are scanned. Labels are assigned once, then each
    # period groups within itself.
    passing, omitted_filters = [], 0
    for position, e in enumerate(loaded.events):
        if excluded(e, selection, agents) or (selection.label is not None and assignment.of(e) != selection.label):
            omitted_filters += 1
        else:
            passing.append((position, e))
    timed = sorted(((p, e) for p, e in passing if e.time_kind == "point" and e.at is not None), key=lambda pair: pair[1].at)
    times = [e.at for _, e in timed]
    rest = [(p, e) for p, e in passing if not (e.time_kind == "point" and e.at is not None)]
    unknown = sum(1 for _, e in rest
                  if (e.at is None if e.time_kind == "point" else e.start is None or e.end is None))

    def window_events(start: float, end: float) -> tuple[list[Event], dict]:
        lo, hi = bisect.bisect_left(times, start), bisect.bisect_left(times, end)
        chosen = list(timed[lo:hi])
        omitted = {"outside_window": len(timed) - (hi - lo), "unknown_time": unknown,
                   "aggregate_crosses_boundary": 0, "filters": omitted_filters}
        for position, e in rest:
            if e.time_kind == "point":
                continue  # Untimed point events are counted above, once for every period.
            if e.start is None or e.end is None:
                continue  # Aggregates without a full interval are the unknown_time counted above.
            if e.end <= start or e.start >= end:
                omitted["outside_window"] += 1
            elif e.start < start or e.end > end:
                omitted["aggregate_crosses_boundary"] += 1
            else:
                chosen.append((position, e))
        chosen.sort(key=lambda pair: pair[0])
        return [e for _, e in chosen], omitted

    def period_view(start: float, end: float) -> dict:
        events, omitted = window_events(start, end)
        return _view(loaded, replace(selection, since=start, until=end), book, events, omitted, tree_issues,
                     price_book is not None, assignment)

    coverage = period_view(selection.since, selection.until)
    periods = []
    for start, end, clipped in windows:
        view = period_view(start, end)
        period = {"start_utc": datetime.fromtimestamp(start, timezone.utc).isoformat(),
                  "end_utc": datetime.fromtimestamp(end, timezone.utc).isoformat(),
                  "start_local": datetime.fromtimestamp(start, zone).isoformat(),
                  "end_local": datetime.fromtimestamp(end, zone).isoformat(),
                  "clipped": clipped, "status": view["status"], "summary": view["summary"]}
        if selection.group_by:
            period["groups"] = view["groups"]
        period["omitted"] = view["coverage"]["omitted"]
        periods.append(period)
    return {"schema": "workflow_auditor.series.v1", "every": every, "tz": tz or "UTC",
            "since_utc": coverage["selection"]["since_utc"],
            "until_exclusive_utc": coverage["selection"]["until_exclusive_utc"],
            "selection": coverage["selection"], "price_basis": coverage["price_basis"],
            "coverage": coverage["coverage"], "periods": periods}


def compare_periods(store: Store, before: Selection, after: Selection, price_book: dict | None = None) -> dict:
    with store.read_snapshot():
        return _compare_periods(store, before, after, price_book)


def _compare_periods(store: Store, before: Selection, after: Selection, price_book: dict | None = None) -> dict:
    if (before.activity, before.bucket_seconds) != (after.activity, after.bucket_seconds):
        raise ValueError("Comparison activity definitions must match")
    left, right = report(store, before, price_book), report(store, after, price_book)
    changes = {}
    for key in ("input_tokens_per_agent_hour", "output_tokens_per_agent_hour"):
        a, b = left["summary"][key], right["summary"][key]
        changes[key] = {"before": a, "after": b, "change_percent": (b / a - 1) * 100 if a and b is not None else None}
    for key in ("input_total", "output_total", "cache_read"):
        a, b = left["summary"]["tokens"][key], right["summary"]["tokens"][key]
        changes[key] = {"before": a, "after": b, "change_percent": (b / a - 1) * 100 if a and b is not None else None}
    before_quotes = {q["unit"]: q for q in left["summary"]["list_estimates"]}
    after_quotes = {q["unit"]: q for q in right["summary"]["list_estimates"]}
    for unit in sorted(set(before_quotes) | set(after_quotes)):
        a = before_quotes.get(unit, {}).get("per_agent_hour_min")
        b = after_quotes.get(unit, {}).get("per_agent_hour_min")
        changes["list_cost_per_agent_hour:" + unit] = {
            "before_min": a, "after_min": b,
            "before_max": before_quotes.get(unit, {}).get("per_agent_hour_max"),
            "after_max": after_quotes.get(unit, {}).get("per_agent_hour_max"),
            "change_percent_of_minima": float((Decimal(b) / Decimal(a) - 1) * 100) if a and Decimal(a) and b is not None else None}
    return {"schema": "workflow_auditor.compare.v1", "before": left, "after": right,
            "changes": changes, "interpretation": "Activity-normalized consumption; outcome efficiency requires accepted-result evidence."}
