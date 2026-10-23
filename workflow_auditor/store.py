from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3

from .model import INFORMATIONAL, Event, reconcile


TABLES = {"metadata", "sources", "observations"}


class Store:
    def __init__(self, path: str | Path, readonly: bool = False):
        """Readers open an existing auditor database read-only; writers create a missing one but never
        adopt an existing file without the auditor schema."""
        self._snapshot_active = False
        self._snapshot_events = None
        path = Path(path)
        existing = path.exists()
        if readonly and not existing:
            raise ValueError(f"Database does not exist; ingest first: {path}")
        if existing and not path.is_file():
            raise ValueError(f"Database path is not a file: {path}")
        try:
            self.db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro" if readonly else path, uri=readonly)
        except sqlite3.Error as exc:
            raise ValueError(f"Cannot open database: {path}") from exc
        try:
            if existing:
                tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                version = self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone() if TABLES <= tables else None
                if version is None:
                    raise ValueError(f"Not a workflow-auditor database: {path}")
                if version[0] != "1":
                    raise ValueError("Unsupported database schema")
            if not readonly:
                self._initialize()
        except sqlite3.OperationalError as exc:  # Before DatabaseError, its base class.
            self.db.close()
            name = getattr(exc, "sqlite_errorname", "")
            if name == "SQLITE_READONLY_ROLLBACK":
                raise ValueError(f"Database has an interrupted transaction; run ingest to recover it: {path}") from exc
            if name in ("SQLITE_BUSY", "SQLITE_LOCKED"):
                raise ValueError(f"Database is locked by another process; retry when it finishes: {path}") from exc
            raise ValueError(f"Cannot open database ({exc}): {path}") from exc
        except sqlite3.DatabaseError as exc:
            self.db.close()
            raise ValueError(f"Not an SQLite database: {path}") from exc
        except ValueError:
            self.db.close()
            raise

    def _initialize(self) -> None:
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sources (
          id TEXT PRIMARY KEY, registration TEXT NOT NULL, path TEXT NOT NULL,
          sha256 TEXT, status TEXT NOT NULL, detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS observations (
          source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
          event_key TEXT NOT NULL, payload TEXT NOT NULL,
          PRIMARY KEY(source_id,event_key));
        CREATE INDEX IF NOT EXISTS event_lookup ON observations(event_key);
        """)
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('schema_version','1')")
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def read_snapshot(self):
        """All report reads see one committed SQLite version, including import state."""
        owned = not self.db.in_transaction
        if owned:
            self.db.execute("BEGIN")
            self._snapshot_active = True
            self._snapshot_events = None
        try:
            yield
        finally:
            if owned:
                self.db.rollback()
                self._snapshot_active = False
                self._snapshot_events = None

    def put_config(self, config: dict, import_state: dict | None = None) -> None:
        # Persist only the accounting configuration, never unrelated environment/auth fields.
        source_fields = {"name", "kind", "roots", "pattern", "exclude", "repo", "provider", "role",
                         "agent", "accounting_role", "exclude_agents", "since", "until"}
        config = {k: config[k] for k in ("sources", "repo_aliases", "agent_roles", "type_roles", "prices") if k in config}
        config["sources"] = [{k: v for k, v in s.items() if k in source_fields} for s in config.get("sources", [])]
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('config',?)", (json.dumps(config),))
            if import_state is not None:
                self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('import_state',?)", (json.dumps(import_state),))

    def config(self) -> dict:
        row = self.db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
        return json.loads(row[0]) if row else {}

    def set_import_state(self, state: str, details: dict | None = None) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('import_state',?)",
                            (json.dumps({"state": state, **(details or {})}),))

    def import_state(self) -> dict:
        row = self.db.execute("SELECT value FROM metadata WHERE key='import_state'").fetchone()
        return json.loads(row[0]) if row else {"state": "unverified"}

    def replace_source(self, source_id: str, registration: str, path: str, digest: str,
                       events: list[Event], detail: dict, primary: bool = True) -> None:
        # Replacement and all its observations commit atomically. No raw text is stored.
        # A Claude response seen under several runs in one file is stored once per run (event_key
        # "key:run"): only events() sees every source and so ranks runs by their own activity.
        claude = detail.get("kind") == "claude"
        local: dict[tuple[str, str], list[Event]] = defaultdict(list)
        for event in events:
            local[(event.key, event.run if claude else "")].append(event)
        runs = Counter(key for key, _ in local)
        observations = []
        for (key, run), values in local.items():
            event, issues = reconcile(values, claude)
            for code in issues:
                where = {"sources": [path]} if code == "speed_geo_mismatch" else {}
                detail.setdefault("notes" if code in INFORMATIONAL else "issues", []).append({"code": code, "event": key, **where})
            observations.append((source_id, key if runs[key] == 1 else f"{key}:{run}", json.dumps(event.data(), allow_nan=False)))
        detail["primary"] = primary
        detail["observations"] = len(observations)
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO sources VALUES (?,?,?,?,?,?)",
                            (source_id, registration, path, digest, "imported", json.dumps(detail)))
            self.db.executemany("INSERT INTO observations VALUES (?,?,?)", observations)

    def failed_source(self, source_id: str, registration: str, path: str, code: str, primary: bool | None = None) -> None:
        # Keep the previous receipt and observations; explicitly mark them stale. The accounting role
        # comes from the registration when known, else from the previous receipt.
        with self.db:
            row = self.db.execute("SELECT detail FROM sources WHERE id=?", (source_id,)).fetchone()
            detail = json.loads(row[0]) if row else {}
            detail.setdefault("issues", []).append({"code": code})
            if primary is not None:
                detail["primary"] = primary
            self.db.execute("INSERT INTO sources VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,detail=excluded.detail",
                            (source_id, registration, path, None, "unavailable", json.dumps(detail)))

    def sources(self) -> list[dict]:
        active = {s["name"] for s in self.config().get("sources", [])}
        return [{"id": r[0], "registration": r[1], "path": r[2], "sha256": r[3],
                 "status": r[4], **json.loads(r[5])}
                for r in self.db.execute("SELECT id,registration,path,sha256,status,detail FROM sources ORDER BY id")
                if (not active or r[1] in active) and r[4] != "retired"]

    def finish_registration(self, registration: str, seen: set[str], primary: bool | None = None) -> None:
        for row in self.db.execute("SELECT id,path,sha256 FROM sources WHERE registration=?", (registration,)).fetchall():
            sid, path, digest = row
            if sid not in seen:
                if digest is None or (path and Path(path).exists()):
                    with self.db:
                        self.db.execute("UPDATE sources SET status='retired' WHERE id=?", (sid,))
                else:
                    self.failed_source(sid, registration, path, "source_disappeared_after_snapshot", primary)

    def _paths(self, key: str, paths: dict[str, str]) -> list[str]:
        # The primary source files of one response, stored under event_key "key" or "key:run".
        rows = self.db.execute("SELECT source_id,payload FROM observations WHERE event_key=? OR (event_key>? AND event_key<?)",
                               (key, key + ":", key + ";"))
        return sorted({paths[s] for s, payload in rows if s in paths and json.loads(payload)["key"] == key})

    def events(self) -> tuple[list[Event], list[dict]]:
        if self._snapshot_active and self._snapshot_events is not None:
            return self._snapshot_events
        # Issues include informational codes (model.INFORMATIONAL), also those noted at import.
        primary = [s for s in self.sources() if s.get("primary", True)]
        kinds = {s["id"]: s.get("kind") for s in primary}
        paths = {s["id"]: s["path"] for s in primary}
        grouped: dict[str, list[Event]] = defaultdict(list)
        claude: dict[str, bool] = {}  # Copy rules apply only when every observation is from a Claude transcript.
        held: dict[str, set[str]] = defaultdict(set)  # Per run, the Claude responses it holds.
        for source_id, payload in self.db.execute("SELECT source_id,payload FROM observations ORDER BY source_id,event_key"):
            if source_id in kinds:
                event = Event.from_data(json.loads(payload))
                grouped[event.key].append(event)
                claude[event.key] = claude.get(event.key, True) and kinds[source_id] == "claude"
                if kinds[source_id] == "claude":
                    held[event.run].add(event.key)
        contains: dict[tuple[str, str], bool] = {}
        times: dict[str, float | None] = {}
        owns: dict[tuple[str, frozenset[str]], float | None] = {}

        def first(key: str) -> float | None:  # A response's owner time: its earliest observation.
            if key not in times:
                times[key] = min((e.at for e in grouped[key] if e.at is not None), default=None)
            return times[key]

        def copied(run: str, other: str) -> bool:
            # run holds all of other and more, and adds nothing before other's latest response: run
            # copied other and continued. An earlier extra makes other a tail copy of run instead.
            # Asked only for runs tied on one copied response; each pair of runs is compared once.
            if (run, other) not in contains:
                mine, theirs, result = held[run], held[other], False
                if theirs < mine:  # Untimed responses decide nothing.
                    shared, added = [first(k) for k in theirs], [first(k) for k in mine - theirs]
                    result = None not in shared + added and min(added) >= max(shared)
                contains[run, other] = result
            return contains[run, other]

        def own_start(run: str, rivals: frozenset[str]) -> float | None:
            # run's earliest timed Claude response that no rival still contending for a response holds.
            # Asked only when several runs contend; each run and rival set is computed once.
            if (run, rivals) not in owns:
                mine = (first(k) for k in held[run] if not any(k in held[o] for o in rivals))
                owns[run, rivals] = min((t for t in mine if t is not None), default=None)
            return owns[run, rivals]
        events, issues = [], [note for s in primary for note in s.get("notes", [])]
        for key, values in grouped.items():
            event, conflicts = reconcile(values, claude[key], own_start, copied)
            events.append(event)
            issues.extend({"code": code, "event": key, **({"sources": self._paths(key, paths)} if code == "speed_geo_mismatch" else {})}
                          for code in conflicts)
        if self._snapshot_active:
            self._snapshot_events = (events, issues)
        return events, issues
