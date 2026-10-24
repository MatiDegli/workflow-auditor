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
class Rule:
    """A half-open window plus exact-match filters; one way a label can match."""
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
class Label:
    """A task: a name plus one or more rules, flat or under any_of. File order decides ties."""
    name: str
    rules: tuple[Rule, ...]

    def covers(self, event: Event) -> bool:
        return any(rule.covers(event) for rule in self.rules)


@dataclass(frozen=True)
class Assignment:
    """One label name per deduplicated event; overlapping holds the keys that matched several."""
    names: dict[str, str]
    overlapping: frozenset[str]
    label_names: tuple[str, ...] = ()

    def of(self, event: Event) -> str:
        return self.names.get(event.key, UNLABELED)

    def counts(self, events: list[Event]) -> dict:
        per_label = dict.fromkeys(self.label_names, 0)  # A label that matched nothing still shows.
        unlabeled = overlapping = 0
        for e in events:
            name = self.names.get(e.key, UNLABELED)
            if name == UNLABELED:
                unlabeled += 1
            else:
                per_label[name] += 1
            overlapping += e.key in self.overlapping
        return {"unlabeled_events": unlabeled, "overlapping_events": overlapping, "per_label": per_label}


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
        return Assignment(names, frozenset(overlapping), tuple(l.name for l in self.labels))


def _no_duplicates(pairs: list[tuple[str, object]]) -> dict:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        repeated = sorted({key for key in keys if keys.count(key) > 1})
        raise ValueError(f"duplicate JSON key: {repeated[0]!r}")
    return dict(pairs)


def _timestamp(value: object, where: str, key: str) -> float:
    if not isinstance(value, str):
        raise ValueError(f"{where}: {key}: must be an ISO 8601 timestamp with an explicit offset")
    try:
        return timestamp(value)
    except ValueError as exc:
        raise ValueError(f"{where}: {key}: {exc}") from exc


def _rule(row: object, where: str) -> Rule:
    if not isinstance(row, dict):
        raise ValueError(f"{where} must be an object")
    unknown = set(row) - {"since", "until", *FILTERS}
    if unknown:
        raise ValueError(f"{where} has an unknown key: {sorted(unknown)[0]!r}")
    for key in ("since", "until"):
        if key not in row:
            raise ValueError(f"{where} is missing {key!r}")
    since, until = _timestamp(row["since"], where, "since"), _timestamp(row["until"], where, "until")
    if since >= until:
        raise ValueError(f"{where}: since must be before until")
    filters: dict[str, tuple[str, ...]] = {}
    for key in FILTERS:
        if key not in row:
            continue  # A missing key matches anything; null, like "since": null, is an error.
        value = row[key]
        if not isinstance(value, list) or not value or any(not isinstance(x, str) or not x for x in value):
            raise ValueError(f"{where}: {key!r} must be a nonempty list of nonempty strings")
        filters[key] = tuple(value)
    return Rule(since, until, **filters)


def _label(row: object, position: int, seen: set[str]) -> Label:
    where = f"label #{position}"
    if not isinstance(row, dict):
        raise ValueError(f"{where} must be an object")
    if "any_of" in row:
        others = set(row) - {"name", "any_of"}
        if others:
            raise ValueError(f"{where}: 'any_of' cannot be combined with {sorted(others)[0]!r}")
    unknown = set(row) - {"name", "since", "until", "any_of", *FILTERS}
    if unknown:
        raise ValueError(f"{where} has an unknown key: {sorted(unknown)[0]!r}")
    if "name" not in row:
        raise ValueError(f"{where} is missing 'name'")
    name = row["name"]
    if not isinstance(name, str) or not name:
        raise ValueError(f"{where} needs a nonempty name")
    if name.lower() == UNLABELED:
        raise ValueError(f"{where}: name {UNLABELED!r} is reserved")
    if name in seen:
        raise ValueError(f"duplicate label name: {name!r}")
    seen.add(name)
    where = f"label {name!r}"  # From here on the label can name itself in errors.
    if "any_of" in row:
        rules = row["any_of"]
        if not isinstance(rules, list) or not rules:
            raise ValueError(f"{where}: 'any_of' must be a nonempty list of rules")
        return Label(name, tuple(_rule(r, f"{where} rule #{i}") for i, r in enumerate(rules)))
    return Label(name, (_rule({k: v for k, v in row.items() if k != "name"}, where),))


def parse_labels(document: dict, digest: str) -> Labels:
    """Validate a parsed workflow_auditor.labels.v1 document; anything wrong is one ValueError."""
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
    return Labels(digest, tuple(_label(row, i, seen) for i, row in enumerate(rows)))


def read_labels(path: Path) -> Labels:
    """Read and validate a workflow_auditor.labels.v1 file; anything wrong is one ValueError."""
    raw = Path(path).read_bytes()
    try:
        document = json.loads(raw, object_pairs_hook=_no_duplicates)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError(f"Labels file is not valid JSON: {exc}") from exc
    return parse_labels(document, hashlib.sha256(raw).hexdigest())
