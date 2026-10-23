from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
import math

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

    def __post_init__(self) -> None:
        if self.since is not None and self.until is not None and self.since >= self.until:
            raise ValueError("since must be before until")
        if self.activity not in ("buckets", "observed"):
            raise ValueError("Unsupported activity method")
        if type(self.bucket_seconds) is not int or not 1 <= self.bucket_seconds <= 3600:
            raise ValueError("Bucket length must be an integer from 1 to 3600 seconds")
        if any(x not in ("repo", "provider", "role", "agent", "model", "client", "run") for x in self.group_by):
            raise ValueError("Unsupported grouping")
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


def select(events: list[Event], selection: Selection) -> tuple[list[Event], dict, list[dict]]:
    children, issues = hierarchy(events)
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
    selected = []
    omitted = {"outside_window": 0, "unknown_time": 0, "aggregate_crosses_boundary": 0, "filters": 0}
    for e in events:
        if ((selection.repos and e.repo not in selection.repos) or
            (selection.providers and e.provider not in selection.providers) or
            (selection.roles and e.role not in selection.roles) or
            (selection.models and e.model not in selection.models) or
            (selection.agents and e.agent not in agents)):
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
        # Known zero usage costs 0 whatever the model (as the ledger); reasoning and TTL splits are
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
    events, issues = store.events()
    conflicts = [i for i in issues if i["code"] not in INFORMATIONAL]
    reattributed = {i["event"] for i in issues if i["code"] == "copied_response_reattributed"}
    mismatched: dict[str, set[str]] = defaultdict(set)  # Response key -> its source files.
    for i in issues:
        if i["code"] == "speed_geo_mismatch":
            mismatched[i["event"]].update(i.get("sources", []))
    config = store.config()
    book = price_book if price_book is not None else config.get("prices", {})
    selected, omitted, tree_issues = select(events, selection)
    # A run aggregate and its detailed requests are different views, not additive costs.
    # Without a verified join, withhold the ambiguous run rather than choosing a cheaper view.
    granularity: dict[tuple, set[str]] = defaultdict(set)
    for e in selected:
        granularity[(e.provider, e.run, e.model)].add(e.time_kind)
    overlap = {key for key, kinds in granularity.items() if kinds == {"point", "aggregate"}}
    withheld = [e for e in selected if (e.provider, e.run, e.model) in overlap]
    selected = [e for e in selected if (e.provider, e.run, e.model) not in overlap]
    sources = store.sources()
    primary = [s for s in sources if s.get("primary", True)]
    source_issues = [{"source": s["path"], **issue} for s in primary for issue in s.get("issues", [])]
    bad_sources = [s for s in primary if s["status"] != "imported"]
    import_state = store.import_state()
    trust = not (conflicts or source_issues or bad_sources or omitted["unknown_time"] or omitted["aggregate_crosses_boundary"] or
                 (selection.descendants and tree_issues) or import_state["state"] != "complete" or overlap)
    grouped: dict[tuple, list[Event]] = defaultdict(list)
    for e in selected:
        grouped[tuple(getattr(e, key) for key in selection.group_by)].append(e)
    groups = [{"group": dict(zip(selection.group_by, key)), **metrics(rows, selection, book, trust)}
              for key, rows in sorted(grouped.items(), key=lambda pair: str(pair[0]))]
    roots = [s for s in config.get("sources", [])]
    summary = metrics(selected, selection, book, trust)
    metric_limits = summary["activity"]["agent_hours"] is None or bool(summary["unpriced_events"])
    return {"schema": "workflow_auditor.report.v1", "status": ("ready_with_missing_metrics" if metric_limits else "ready") if trust else "partial",
            "selection": {"since_utc_epoch": selection.since, "until_exclusive_utc_epoch": selection.until,
                          "since_utc": datetime.fromtimestamp(selection.since, timezone.utc).isoformat() if selection.since is not None else None,
                          "until_exclusive_utc": datetime.fromtimestamp(selection.until, timezone.utc).isoformat() if selection.until is not None else None,
                          "repos": selection.repos, "providers": selection.providers, "roles": selection.roles,
                          "agents": selection.agents, "models": selection.models, "include_descendants": selection.descendants},
            "coverage": {"scope": "registered source roots only; not a global provider census", "registrations": len(roots),
                         "source_files": len(sources), "unavailable_primary_sources": len(bad_sources),
                         "corroborating_sources_excluded_from_sums": sum(not s.get("primary", True) for s in sources),
                         "omitted": omitted, "conflicts": conflicts, "source_issues": source_issues,
                         "copied_responses_reattributed": sum(e.key in reattributed for e in selected),
                         "speed_geo_mismatches": [{"event": k, "sources": sorted(mismatched[k])}
                                                  for k in sorted({e.key for e in selected} & mismatched.keys())],
                         "hierarchy_issues": tree_issues, "import_state": import_state,
                         "aggregate_detail_overlaps": [list(x) for x in sorted(overlap)],
                         "withheld_overlap_events": len(withheld),
                         "rate_denominators_complete": trust and summary["activity"]["agent_hours"] is not None,
                         "unclassified_role_events": sum(e.role == "unknown" for e in selected),
                         "unattributed_repo_events": sum(e.repo == "unattributed" for e in selected)},
            "price_basis": {"as_of": book.get("as_of"), "source": book.get("source"),
                            "revaluation_override": price_book is not None, "is_billing": False},
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
