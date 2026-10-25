from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
import hashlib
import json
import re

if TYPE_CHECKING:
    from .query import _Loaded

SCHEMA = "workflow_auditor.acknowledgments.v1"
CODES = frozenset(("invalid_json", "json_record_not_object", "invalid_accounting_record",
                   "missing_response_identity", "missing_counter_identity", "router_request_id_missing"))
FIELDS = ("registration", "path", "sha256", "code", "line")


@dataclass(frozen=True)
class Acknowledgment:
    registration: str
    path: str
    sha256: str
    code: str
    line: int


@dataclass(frozen=True)
class Acknowledgments:
    """Validated identities and the digest of the original bytes; reasons are discarded."""
    sha256: str
    entries: tuple[Acknowledgment, ...]


def _no_duplicates(pairs: list[tuple[str, object]]) -> dict:
    if len({key for key, _ in pairs}) != len(pairs):
        raise ValueError("acknowledgments: duplicate JSON key")
    return dict(pairs)


def parse_acknowledgments(raw: bytes) -> Acknowledgments:
    """Parse UTF-8 JSON bytes without exposing supplied values in validation errors."""
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("acknowledgments: invalid UTF-8 JSON or duplicate JSON key") from None
    if not isinstance(document, dict):
        raise ValueError("acknowledgments: document must be an object")
    if set(document) - {"schema", "acknowledgments"}:
        raise ValueError("acknowledgments: unknown field")
    if document.get("schema") != SCHEMA:
        raise ValueError("acknowledgments: 'schema' must be workflow_auditor.acknowledgments.v1")
    rows = document.get("acknowledgments")
    if not isinstance(rows, list):
        raise ValueError("acknowledgments: 'acknowledgments' must be a list")
    entries, seen = [], set()
    for position, row in enumerate(rows, 1):
        where = f"acknowledgments entry {position}"
        if not isinstance(row, dict):
            raise ValueError(f"{where}: must be an object")
        if set(row) - {*FIELDS, "reason"}:
            raise ValueError(f"{where}: unknown field")
        for key in FIELDS:
            if key not in row:
                raise ValueError(f"{where}: missing '{key}'")
        for key in ("registration", "path", "code"):
            if not isinstance(row[key], str) or not row[key]:
                raise ValueError(f"{where}: '{key}' must be a nonempty string")
        if not isinstance(row["sha256"], str) or not re.fullmatch(r"[0-9a-fA-F]{64}", row["sha256"]):
            raise ValueError(f"{where}: 'sha256' must be 64 hexadecimal characters")
        if type(row["line"]) is not int or row["line"] <= 0:
            raise ValueError(f"{where}: 'line' must be a positive integer")
        if "reason" in row and not isinstance(row["reason"], str):
            raise ValueError(f"{where}: 'reason' must be a string")
        if row["code"] not in CODES:
            raise ValueError(f"{where}: 'code' is not acknowledgeable")
        entry = Acknowledgment(row["registration"], row["path"], row["sha256"].lower(), row["code"], row["line"])
        if entry in seen:
            raise ValueError(f"{where}: duplicate identity tuple")
        seen.add(entry)
        entries.append(entry)
    return Acknowledgments(hashlib.sha256(raw).hexdigest(), tuple(entries))


def eligible_source(source: dict, loaded: _Loaded) -> bool:
    """An active, non-retired primary snapshot that _load has not marked bad is eligible.

    Store.sources already excludes retired rows and inactive registrations. Imported rows
    qualify; rotated rows qualify only with verified retention and no collection issues.
    """
    return source.get("primary", True) and all(source["id"] != bad["id"] for bad in loaded.bad_sources)


def match_acknowledgments(acknowledgments: Acknowledgments, loaded: _Loaded) -> tuple[list[dict], set[int]]:
    """Return dispositions in file order and matched indexes into loaded.source_issues.

    Each disposition has disposition/explanation, plus source_issue_index when matched,
    or stored_registration/stored_sha256 when lapsed or issue_absent. No store or file I/O.
    """
    candidates, issues = {}, {}
    for source in loaded.sources:
        if eligible_source(source, loaded):
            candidates.setdefault((source["registration"], source["path"]), []).append(source)
    for index, origin in enumerate(loaded.source_issue_origins):
        issues.setdefault(origin, []).append((index, loaded.source_issues[index]))
    dispositions, matched = [], set()
    for entry in acknowledgments.entries:
        rows = candidates.get((entry.registration, entry.path), [])
        result = {"disposition": "unmatched"}
        if len(rows) > 1:
            explanation = "identity_ambiguous"
        elif not rows:
            unavailable = any(s.get("primary", True) and s["registration"] == entry.registration
                              and s["path"] == entry.path for s in loaded.bad_sources)
            explanation = "snapshot_unavailable" if unavailable else "source_not_found"
        else:
            source = rows[0]
            same_code = [(i, issue) for i, issue in issues.get(source["id"], []) if issue.get("code") == entry.code]
            exact = [(i, issue) for i, issue in same_code if issue.get("line") == entry.line]
            if not source.get("sha256") or len(loaded.source_issue_origins) != len(loaded.source_issues):
                explanation = "identity_incomplete"
            elif len(exact) > 1:
                explanation = "identity_ambiguous"
            elif any(issue.get("line") is None for _, issue in same_code):
                explanation = "identity_incomplete"
            elif source["sha256"] != entry.sha256 and same_code:
                explanation = "digest_changed"
                result["disposition"] = "lapsed"
            elif source["sha256"] == entry.sha256 and exact:
                explanation = "matched"
                result.update(disposition="matched", source_issue_index=exact[0][0])
                matched.add(exact[0][0])
            else:
                explanation = "issue_absent"
            if explanation in ("digest_changed", "issue_absent"):
                result.update(stored_registration=source["registration"], stored_sha256=source["sha256"])
        result["explanation"] = explanation
        dispositions.append(result)
    return dispositions, matched
