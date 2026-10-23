from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from typing import Callable, Literal

TOKEN_FIELDS = ("input_total", "cache_read", "cache_write", "output_total", "reasoning")


def number(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Token counts must be nonnegative integers")
    return value


def optional_number(value: object) -> int | None:
    return None if value is None else number(value)


def money(value: object) -> str | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid monetary estimate") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("Invalid monetary estimate")
    return str(result)


def timestamp(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Timestamp must be ISO 8601 with timezone")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include timezone")
    return parsed.timestamp()


def identifier(*values: object) -> str:
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Tokens:
    input_total: int | None
    cache_read: int | None
    cache_write: int | None
    output_total: int | None
    reasoning: int | None = None
    write_5m: int | None = None
    write_1h: int | None = None

    def __post_init__(self) -> None:
        for field in self.__dataclass_fields__:
            optional_number(getattr(self, field))
        if self.input_total is not None:
            if sum(v or 0 for v in (self.cache_read, self.cache_write)) > self.input_total:
                raise ValueError("Cache components exceed input")
        if self.output_total is not None and (self.reasoning or 0) > self.output_total:
            raise ValueError("Reasoning exceeds output")
        if self.cache_write is not None and (self.write_5m or 0) + (self.write_1h or 0) > self.cache_write:
            raise ValueError("Cache TTL components exceed cache writes")

    @property
    def fresh(self) -> int | None:
        if any(x is None for x in (self.input_total, self.cache_read, self.cache_write)):
            return None
        return self.input_total - self.cache_read - self.cache_write  # type: ignore[operator]


@dataclass(frozen=True, slots=True)
class Event:
    key: str
    client: str
    provider: str
    provider_basis: str
    model: str
    repo: str
    run: str
    agent: str | None
    parent: str | None
    role: str
    role_basis: str
    tokens: Tokens
    time_kind: Literal["point", "aggregate"]
    at: float | None = None
    start: float | None = None
    end: float | None = None
    activity_start: float | None = None
    activity_end: float | None = None
    activity_basis: str | None = None
    cli_usd: str | None = None
    speed: str = "unknown"
    geo: str = "unknown"
    unit: str = "response"

    def __post_init__(self) -> None:
        identities = (self.key, self.client, self.provider, self.provider_basis, self.model,
                      self.repo, self.run, self.role, self.role_basis)
        if any(not isinstance(value, str) or not value for value in identities):
            raise ValueError("Event identity and scope are required")
        if any(value is not None and (not isinstance(value, str) or not value) for value in (self.agent, self.parent)):
            raise ValueError("Agent identifiers must be nonempty strings or null")
        if self.time_kind not in ("point", "aggregate"):
            raise ValueError("Unsupported time kind")
        for value in (self.at, self.start, self.end, self.activity_start, self.activity_end):
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ValueError("Invalid timestamp")
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("Reversed event interval")
        if (self.activity_start is None) != (self.activity_end is None):
            raise ValueError("Activity requires both endpoints")
        if self.activity_start is not None and self.activity_end <= self.activity_start:
            raise ValueError("Empty/reversed activity interval")
        object.__setattr__(self, "cli_usd", money(self.cli_usd))

    def data(self) -> dict:
        return asdict(self)

    @classmethod
    def from_data(cls, data: dict) -> Event:
        data = dict(data)
        data["tokens"] = Tokens(**data["tokens"])
        return cls(**data)


# Reconciliation codes that describe how copies merged; they leave trust intact. Only
# speed_geo_mismatch changes a total: its event is unpriced.
INFORMATIONAL = frozenset({"copied_response_reattributed", "speed_geo_mismatch"})
ATTRIBUTION = ("repo", "run", "agent", "parent", "role")
INTERVALS = ("start", "end", "activity_start", "activity_end", "activity_basis")
OWNED = ATTRIBUTION + ("role_basis", "at") + INTERVALS
PRICED_FIELDS = ("input_total", "cache_read", "cache_write", "output_total")
SERVICE = ("speed", "geo")  # Claude service options that change the price.
MISMATCH = "mismatch"  # Copies disagree on a known speed or geo: the price is unknown.


def reconcile(events: list[Event], copies: bool = False,
              own_start: Callable[[str, frozenset[str]], float | None] | None = None,
              copied: Callable[[str, str], bool] | None = None) -> tuple[Event, list[str]]:
    """One request, potentially observed progressively and in several copied files.

    One observation owns the request: its time, attribution and intervals. Usage comes from the most
    complete cumulative observation. copies says every observation comes from Claude transcripts,
    where resumed sessions re-stamp copied responses with their own session: there a differing
    attribution of point events is informational when the priced counters match or run, agent and
    parent match (one agent's blocks under two cwds). Any other difference in attribution or
    intervals is an attribution_conflict; a client, provider or model that differs is an
    identity_conflict. A known speed or geo survives a copy without it; two known values that differ
    keep the event, its tokens and its time, but price it nowhere: the field becomes MISMATCH and the
    visible, non-trust-breaking speed_geo_mismatch is returned (the ledger's rule L2).

    The owner is (1) the earliest observation. Re-stamped copies tie on time, so then: (2) for copies,
    a tied run that holds every response another tied run holds, and more, copied it and loses;
    copied(a, b) answers that over all Claude observations: b's set is a strict subset of a's and
    every response a adds is at or after b's latest response, each response taken at its earliest
    observation. If a adds an earlier one, b is a tail copy of a, and a stays. Because each response
    has a single time, a run dropped at (2) is also dropped by every run that dropped its dropper, so
    it does not matter whether dropped runs count as evidence. (3) the contending run (tied at (1),
    not dropped at (2)) whose own activity started first: own_start(run, rivals) gives run's earliest
    timed response that none of rivals, the other contending runs, holds; a run without one never
    wins this step; (4) the smallest attribution and interval tuple. Two runs holding the same
    responses (a copy that added nothing) are undecidable from the logs and fall to (4). Neither file
    nor source order can decide."""
    if len(events) == 1:
        return events[0], []
    when = lambda e: (e.at is None, e.at or 0.0)
    first = min(map(when, events))
    tied = sorted({e.run for e in events if when(e) == first})
    copier = {r for r in tied if any(copied(r, o) for o in tied if o != r)} if copies and copied and len(tied) > 1 else set()
    contending = frozenset(tied) - copier
    own: dict[str, float] = {}
    for run in contending if own_start and len(contending) > 1 else ():
        start = own_start(run, contending - {run})
        if start is not None:
            own[run] = start
    ordered = sorted(events, key=lambda e: (*when(e), e.run in copier, e.run not in own, own.get(e.run, 0.0),
                                            e.run, e.agent or "", e.repo, e.parent or "", e.role, e.role_basis,
                                            *(repr(getattr(e, k)) for k in INTERVALS)))
    owner = usage = ordered[0]
    issues: list[str] = []
    service = {k: getattr(owner, k) for k in SERVICE}
    for new in ordered[1:]:
        if (owner.client, owner.provider, owner.model) != (new.client, new.provider, new.model):
            issues.append("identity_conflict")
            continue
        pairs = [(getattr(usage.tokens, f), getattr(new.tokens, f)) for f in TOKEN_FIELDS]
        newer = all(a is None or (b is not None and b >= a) for a, b in pairs)
        older = all(b is None or (a is not None and a >= b) for a, b in pairs)
        if not newer and not older:
            issues.append("non_cumulative_usage_conflict")
            continue
        if newer and not older:
            usage = new
        for k in SERVICE:
            value = getattr(new, k)
            if value not in ("unknown", service[k]):
                if service[k] != "unknown" or value == MISMATCH:
                    issues.append("speed_geo_mismatch")
                service[k] = value if service[k] == "unknown" else MISMATCH
        if any(getattr(new, k) != getattr(owner, k) for k in ATTRIBUTION + INTERVALS):
            copy = copies and owner.time_kind == new.time_kind == "point" and (
                all(getattr(new.tokens, f) == getattr(owner.tokens, f) for f in PRICED_FIELDS) or
                all(getattr(new, k) == getattr(owner, k) for k in ("run", "agent", "parent")))
            issues.append("copied_response_reattributed" if copy else "attribution_conflict")
    chosen = usage if usage is owner else replace(usage, **{k: getattr(owner, k) for k in OWNED})
    if any(getattr(chosen, k) != v for k, v in service.items()):
        chosen = replace(chosen, **service)
    return chosen, sorted(set(issues))


def merge_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result
