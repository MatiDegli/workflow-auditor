from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json

from .model import Event, timestamp

SCHEMA = "workflow_auditor.labels.v1"
UNLABELED = "unlabeled"
FILTERS = ("repos", "providers", "roles", "agents", "runs", "models")


@dataclass(frozen=True)
class Label:
    """A task: a half-open window plus exact-match filters. The file order decides ties."""
    name: str
    since: float
    until: float
    repos: tuple[str, ...] | None = None
    providers: tuple[str, ...] | None = None
    roles: tuple[str, ...] | None = None
    agents: tuple[str, ...] | None = None
    runs: tuple[str, ...] | None = None
    models: tuple[str, ...] | None = None

    def covers(self, event: Event) -> bool:
        # A missing filter matches anything; a point event matches by its at, an aggregate
        # only when its whole [start, end) lies inside the window.
        for field, names in zip(("repo", "provider", "role", "agent", "run", "model"),
                                (self.repos, self.providers, self.roles, self.agents, self.runs, self.models)):
            if names is not None and getattr(event, field) not in names:
                return False
        if event.time_kind == "point":
            return event.at is not None and self.since <= event.at < self.until
        return (event.start is not None and event.end is not None
                and self.since <= event.start and event.end <= self.until)


@dataclass(frozen=True)
class Assignment:
    """One label name per deduplicated event; overlapping holds the keys that matched several."""
    names: dict[str, str]
    overlapping: frozenset[str]

    def of(self, event: Event) -> str:
        return self.names.get(event.key, UNLABELED)

    def counts(self, events: list[Event]) -> dict:
        return {"unlabeled_events": sum(self.of(e) == UNLABELED for e in events),
                "overlapping_events": sum(e.key in self.overlapping for e in events)}


@dataclass(frozen=True)
class Labels:
    """A parsed labels file; assignment only groups and filters, it never changes a figure."""
    sha256: str
    labels: tuple[Label, ...]

    def assign(self, events: list[Event]) -> Assignment:
        names: dict[str, str] = {}
        overlapping: set[str] = set()
        for event in events:  # The first matching label, in file order, wins.
            hits = [label.name for label in self.labels if label.covers(event)]
            names[event.key] = hits[0] if hits else UNLABELED
            if len(hits) > 1:
                overlapping.add(event.key)
        return Assignment(names, frozenset(overlapping))


def _label(row: object, position: int, seen: set[str]) -> Label:
    where = f"label #{position}"
    if not isinstance(row, dict):
        raise ValueError(f"{where} must be an object")
    unknown = set(row) - {"name", "since", "until", *FILTERS}
    if unknown:
        raise ValueError(f"{where} has an unknown key: {sorted(unknown)[0]!r}")
    for key in ("name", "since", "until"):
        if key not in row:
            raise ValueError(f"{where} is missing {key!r}")
    name = row["name"]
    if not isinstance(name, str) or not name:
        raise ValueError(f"{where} needs a nonempty name")
    if name == UNLABELED:
        raise ValueError(f"{where}: name {UNLABELED!r} is reserved")
    if name in seen:
        raise ValueError(f"duplicate label name: {name!r}")
    seen.add(name)
    for key in ("since", "until"):
        if not isinstance(row[key], str):
            raise ValueError(f"{where}: {key!r} must be an ISO 8601 timestamp with an explicit offset")
    since, until = timestamp(row["since"]), timestamp(row["until"])
    if since >= until:
        raise ValueError(f"{where}: since must be before until")
    filters: dict[str, tuple[str, ...]] = {}
    for key in FILTERS:
        value = row.get(key)
        if value is None:
            continue
        if not isinstance(value, list) or any(not isinstance(x, str) or not x for x in value):
            raise ValueError(f"{where}: {key!r} must be a list of nonempty strings")
        filters[key] = tuple(value)
    return Label(name, since, until, **filters)


def read_labels(path: Path) -> Labels:
    """Parse and validate a workflow_auditor.labels.v1 file; anything wrong is one ValueError."""
    raw = Path(path).read_bytes()
    try:
        document = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError(f"Labels file is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("Labels file must be a JSON object")
    unknown = set(document) - {"schema", "labels"}
    if unknown:
        raise ValueError(f"Labels file has an unknown key: {sorted(unknown)[0]!r}")
    if document.get("schema") != SCHEMA:
        raise ValueError(f"Labels file schema must be {SCHEMA}")
    rows = document.get("labels")
    if not isinstance(rows, list):
        raise ValueError("Labels file needs a list under 'labels'")
    seen: set[str] = set()
    return Labels(hashlib.sha256(raw).hexdigest(), tuple(_label(row, i, seen) for i, row in enumerate(rows)))
