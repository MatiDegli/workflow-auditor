from dataclasses import replace
from pathlib import Path
from unittest import mock
import hashlib
import json
import tempfile
import unittest

from workflow_auditor.acknowledgments import SCHEMA, parse_acknowledgments, match_acknowledgments
from workflow_auditor.adapters import ingest
from workflow_auditor.model import Event, Tokens
from workflow_auditor.query import _load
from workflow_auditor.store import Store

MARKER = "PRIVATE_VALUE_DO_NOT_ECHO"
ALLOWED = ("invalid_json", "json_record_not_object", "invalid_accounting_record",
           "missing_response_identity", "missing_counter_identity", "router_request_id_missing")


class AcknowledgmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.logs = self.root/"logs"; self.logs.mkdir()
        self.path = self.logs/"bad.jsonl"
        self.source = {"name": "test", "kind": "normalized", "roots": [str(self.logs)]}
        self.config = {"sources": [self.source]}
        self.store = Store(self.root/"test.sqlite"); self.addCleanup(self.store.close)
        event = Event("one", "test", "test", "fixture", "test", "repo", "run", "agent", None,
                      "builder", "fixture", Tokens(10, 0, 0, 2), "point", at=1792490400)
        self.good = json.dumps(event.data())+"\n"
        self.capture(self.good+"{bad\n")
        self.entry = {k: self.row()[k] for k in ("registration", "path", "sha256")}
        self.entry.update(code="invalid_json", line=2, reason=MARKER)

    def row(self):
        return next(s for s in self.store.sources(True) if s["registration"] == "test" and s["path"] == str(self.path))

    def capture(self, text):
        self.logs.mkdir(exist_ok=True); self.path.write_text(text)
        ingest(self.store, self.config)

    def save(self, **changes):
        row = {**self.row(), **changes}
        detail = {k: v for k, v in row.items() if k not in ("id", "registration", "path", "sha256", "status")}
        with self.store.db:
            self.store.db.execute("UPDATE sources SET sha256=?,status=?,detail=? WHERE id=?",
                                  (row["sha256"], row["status"], json.dumps(detail), row["id"]))

    def raw(self, rows=None):
        return json.dumps({"schema": SCHEMA, "acknowledgments": [self.entry] if rows is None else rows}).encode()

    def check(self, explanation, entry=None, loaded=None):
        parsed = parse_acknowledgments(self.raw([self.entry if entry is None else entry]))
        dispositions, indexes = match_acknowledgments(parsed, _load(self.store) if loaded is None else loaded)
        result, = dispositions
        self.assertEqual(result["explanation"], explanation)
        state = "matched" if explanation == "matched" else "lapsed" if explanation == "digest_changed" else "unmatched"
        self.assertEqual(result["disposition"], state)
        self.assertEqual(indexes, {result["source_issue_index"]} if state == "matched" else set())
        self.assertNotIn(MARKER, json.dumps(result)); self.assertNotIn("reason", result)
        if explanation in ("digest_changed", "issue_absent"):
            self.assertEqual(result["stored_registration"], "test")
            self.assertEqual(result["stored_sha256"], self.row()["sha256"])
        return result

    def test_01_parser(self):
        """Fault: invalid JSON, shapes, fields or types pass, or errors expose supplied text."""
        document = {"schema": SCHEMA, "acknowledgments": [self.entry]}
        documents = [None, [], MARKER, True, 1, 1.0, {}, {**document, "schema": MARKER},
                     {**document, MARKER: MARKER}, {"schema": SCHEMA},
                     {"acknowledgments": []}]
        documents += [{**document, "acknowledgments": v} for v in (None, {}, MARKER, True, 1)]
        rows = [None, [], MARKER, True, 1, 1.0, {**self.entry, MARKER: MARKER}]
        rows += [{k: v for k, v in self.entry.items() if k != field} for field in self.entry if field != "reason"]
        bad = {"registration": [None, True, 1, [], {}, "", MARKER + "\ud800"],
               "path": [None, True, 1, [], {}, "", MARKER + "\ud800"],
               "code": [None, True, 1, [], {}, "", MARKER],
               "sha256": [None, True, 1, [], {}, "", MARKER, "g"*64, "a"*63, "a"*65],
               "line": [None, True, False, 0, -1, 1.0, MARKER, [], {}],
               "reason": [None, True, 1, [], {}, MARKER + "\ud800"]}
        rows += [{**self.entry, field: value} for field, values in bad.items() for value in values]
        documents += [{**document, "acknowledgments": [{**self.entry, "registration": "prefix"}, row]} for row in rows]
        invalid = [b"", b"\xff", self.raw().decode().encode("utf-16"), ("{"+MARKER).encode(), b"NaN",
                   ('{"schema":"'+SCHEMA+'","'+MARKER+'":1,"'+MARKER+'":2}').encode(),
                   self.raw().replace(b'"line": 2', b'"line": 2, "line": 2')]
        invalid += [json.dumps(d).encode() for d in documents]
        for raw in invalid:
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError) as caught:
                    parse_acknowledgments(raw)
                self.assertNotIn(MARKER, str(caught.exception))
                for leak in ("\ud800", "ud800"): self.assertNotIn(leak, str(caught.exception))
        with self.assertRaisesRegex(ValueError, "entry 3: 'sha256' must be 64 hexadecimal characters"):
            parse_acknowledgments(self.raw([self.entry, {**self.entry, "line": 3}, {**self.entry, "sha256": MARKER}]))
        for reason in ("", MARKER):
            parse_acknowledgments(self.raw([{**self.entry, "reason": reason}]))
        parse_acknowledgments(self.raw([{k: v for k, v in self.entry.items() if k != "reason"}]))
        self.assertEqual(parse_acknowledgments(self.raw([])).entries, ())

    def test_02_duplicates_and_bytes(self):
        """Fault: hashes are not normalized before duplicate detection, or file bytes are reserialized."""
        entry = {**self.entry, "sha256": "abcdef"*10+"abcd"}
        for digest in (entry["sha256"], entry["sha256"].upper()):
            with self.subTest(digest=digest), self.assertRaisesRegex(ValueError, "entry 2: duplicate identity tuple"):
                parse_acknowledgments(self.raw([entry, {**entry, "sha256": digest, "reason": "other"}]))
        parsed = parse_acknowledgments(self.raw([{**entry, "sha256": entry["sha256"].upper()}]))
        self.assertEqual(parsed.entries[0].sha256, entry["sha256"])
        raw = self.raw(); self.assertEqual(parse_acknowledgments(raw).sha256, hashlib.sha256(raw).hexdigest())
        self.assertNotEqual(parse_acknowledgments(raw).sha256, parse_acknowledgments(raw+b"\n").sha256)

    def test_03_allowlist(self):
        """Fault: unsupported codes pass, or eligible issues from real adapters fail to match."""
        for code in ALLOWED:
            with self.subTest(code=code):
                self.assertEqual(parse_acknowledgments(self.raw([{**self.entry, "code": code}])).entries[0].code, code)
        for code in ("identity_conflict", "counter_reset_unresolved", "stream_requires_claude_stream_adapter",
                     "adapter_version_mismatch", "final_result_missing", "source_unavailable", "registered_root_missing", MARKER):
            with self.subTest(code=code), self.assertRaises(ValueError) as caught:
                parse_acknowledgments(self.raw([{**self.entry, "code": code}]))
            self.assertNotIn(code, str(caught.exception))
        self.check("matched", entry={**self.entry, "sha256": self.entry["sha256"].upper()})
        self.source.update(kind="router", accounting_role="primary")
        self.capture('{"model":"test","inputTokens":1}\n')
        entry = {**self.entry, "sha256": self.row()["sha256"], "code": "router_request_id_missing", "line": 1}
        self.check("matched", entry=entry)

    def test_04_complete_identity(self):
        """Fault: any tuple component is ignored, or shared paths cross registration origins."""
        for field, value, explanation in (("registration", "other", "source_not_found"),
                ("path", str(self.logs/"."/"absent.jsonl"), "source_not_found"),
                ("sha256", "0"*64, "digest_changed"), ("code", "json_record_not_object", "issue_absent"),
                ("line", 1, "issue_absent")):
            with self.subTest(field=field):
                self.check(explanation, entry={**self.entry, field: value})
        self.check("source_not_found", entry={**self.entry, "path": str(self.logs)+"/./bad.jsonl"})
        self.config["sources"].append({**self.source, "name": "other"}); ingest(self.store, self.config)
        self.save(issues=[{"code": "json_record_not_object", "line": 2}])
        self.check("issue_absent")
        self.check("matched", entry={**self.entry, "registration": "other"})
        loaded = _load(self.store); before = self.store.db.total_changes
        self.path.unlink()
        with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("file I/O")):
            self.check("matched", entry={**self.entry, "registration": "other"}, loaded=loaded)
        self.assertEqual(self.store.db.total_changes, before)

    def test_09_wrong_source(self):
        """Fault: issues attributed to the wrong source match the first primary file."""
        other = self.logs/"clean.jsonl"; other.write_text(self.good)
        ingest(self.store, self.config)
        first, second = self.store.sources()
        Path(first["path"]).write_text(self.good)
        Path(second["path"]).write_text(self.good+"{bad\n")
        ingest(self.store, self.config)
        first, second = self.store.sources(); loaded = _load(self.store)
        entries = [{**{k: row[k] for k in ("registration", "path", "sha256")}, "code": "invalid_json", "line": 2}
                   for row in (first, second)]
        dispositions, indexes = match_acknowledgments(parse_acknowledgments(self.raw(entries)), loaded)
        self.assertEqual(dispositions[0]["explanation"], "issue_absent")
        self.assertEqual(dispositions[1]["disposition"], "matched")
        index = loaded.source_issues.index({"source": second["path"], "code": "invalid_json", "line": 2})
        self.assertEqual(dispositions[1]["source_issue_index"], index)
        self.assertEqual(indexes, {index})

    def test_05_lapse_and_repair(self):
        """Fault: changed bytes or moved issues still match, or repaired issues are called lapsed."""
        self.capture(self.good+"{changed\n"); self.check("digest_changed")
        self.capture(self.good+self.good+"{changed\n"); self.check("digest_changed")
        self.capture(self.good); self.check("issue_absent")
        self.capture(self.good+"{bad\n")
        self.check("issue_absent", entry={**self.entry, "line": 3})

    def test_06_stale_snapshots(self):
        """Fault: failed replacements, missing roots or unverified retained snapshots match old identities."""
        for fault in ("failed", "root", "receipt", "count", "digest", "collection", "rotated"):
            with self.subTest(fault=fault):
                self.capture(self.good+"{bad\n")
                if fault == "failed":
                    row = self.row()
                    self.store.failed_source(row["id"], "test", str(self.path), "source_import_failed:ValueError")
                    self.assertEqual(self.row()["sha256"], self.entry["sha256"])
                elif fault == "digest":
                    self.save(sha256=None)
                else:
                    self.path.unlink()
                    if fault == "root": self.logs.rmdir()
                    ingest(self.store, self.config)
                    if fault == "receipt": self.save(receipt={})
                    if fault == "count":
                        self.save(observations=2, receipt={**self.row()["receipt"], "observations": 2})
                    if fault == "collection": self.save(collection_issues=[{"code": "known_path_stat_failed"}])
                explanation = "matched" if fault == "rotated" else "identity_incomplete" if fault == "digest" else "snapshot_unavailable"
                self.check(explanation)
                if fault == "rotated": self.assertEqual(self.row()["status"], "rotated")

    def test_07_ineligible_sources(self):
        """Fault: retired, inactive or corroborating sources become eligible acknowledgment candidates."""
        self.save(status="retired"); self.check("source_not_found")
        self.capture(self.good+"{bad\n")
        self.store.put_config({"sources": [{**self.source, "name": "active"}]})
        self.check("source_not_found")
        self.source["accounting_role"] = "corroborating"; self.capture(self.good+"{bad\n")
        self.assertFalse(self.row()["primary"]); self.check("source_not_found")
        loaded = _load(self.store)
        # Even a stray flattened issue must not make a corroborating source eligible.
        self.check("source_not_found", loaded=replace(loaded, source_issues=[{"code": "invalid_json", "line": 2}],
                   source_issue_origins=[self.row()["id"]]))

    def test_08_incomplete_and_ambiguous(self):
        """Fault: missing origins/line, duplicate candidates or duplicate diagnostics permit a match."""
        loaded = _load(self.store); row = self.row()
        self.check("matched", loaded=replace(loaded, sources=[{k: v for k, v in row.items() if k != "primary"}]))
        self.check("identity_ambiguous", loaded=replace(loaded, sources=[row, {**row, "id": "duplicate"}]))
        self.check("identity_ambiguous", loaded=replace(loaded, source_issues=loaded.source_issues*2,
                   source_issue_origins=loaded.source_issue_origins*2))
        self.check("identity_incomplete", loaded=replace(loaded, source_issue_origins=[]))
        self.save(issues=[{"code": "invalid_json"}]); self.check("identity_incomplete")
        self.save(issues=[{"code": "invalid_json", "line": 2}, {"code": "invalid_json", "line": 3}])
        parsed = parse_acknowledgments(self.raw([self.entry, {**self.entry, "line": 3}, {**self.entry, "path": "absent"}]))
        dispositions, indexes = match_acknowledgments(parsed, _load(self.store))
        self.assertEqual([d["explanation"] for d in dispositions], ["matched", "matched", "source_not_found"])
        self.assertEqual([d["source_issue_index"] for d in dispositions[:2]], [0, 1])
        self.assertEqual(indexes, {0, 1})
        self.assertEqual(match_acknowledgments(parse_acknowledgments(self.raw([])), _load(self.store)), ([], set()))


if __name__ == "__main__":
    unittest.main()
