from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from io import StringIO
from pathlib import Path
from unittest import mock
import hashlib
import json
import tempfile
import unittest

from workflow_auditor.acknowledgments import SCHEMA, parse_acknowledgments
from workflow_auditor.cli import main
from workflow_auditor.query import Selection, report
from workflow_auditor.store import Store
from test_crossing_activity import START, BOOK, point, aggregate

MARKER = "PRIVATE_REASON_DO_NOT_ECHO"
ISSUE = {"code": "invalid_json", "line": 2}
WINDOW = ["--since", "2026-10-20T10:00:00Z", "--until", "2026-10-20T11:00:00Z"]
BASELINES = {  # Captured from ack/4-1a before reporting changes; replace only the temporary root.
    "healthy": "44c1097bbe1c197224a7ca0ba027bd5133b04dc1fda8afc7d141c661746dc3f9",
    "issue": "47ddb9d7939b4b9df1687a5a94fc8c57debffc4e3a00ba98c395f076077e9429",
    "boundary": "21e95cff2466e9ca42fa0455557afc3cfa7df905a8bbff2849e4baab03d1be0e",
    "labels": "85d64c1c9df87f223af0d282582748cd91d185fe211bc2373fb26b9bcc108f95",
    "markdown": "a03046bbde9fb0cd804dc1b71d7302172fc5b74b7b95ceb3b822d2750ee13eae",
    "series": "fc4a8fede1c6afe9c5927bf8ed2c35c6448e6577c109f9e9c06b436e5ce52fba",
    "compare": "a15f55148c1016a1ebe10918efabb58daccce9e3debd905ed3236f469e0282d6",
}


class AcknowledgmentReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root/"audit.sqlite"); self.addCleanup(self.store.close)
        self.store.put_config({"sources": [{"name": "test"}], "prices": BOOK})
        self.selection = Selection(since=START, until=START+3600, activity="observed")
        self.ack = self.root/"ack.json"
        self.labels = self.root/"labels.json"
        self.labels.write_text(json.dumps({"schema": "workflow_auditor.labels.v1", "labels": [
            {"name": "task", "since": "2026-10-20T09:00:00Z", "until": "2026-10-20T12:00:00Z"}]}))
        self.put()

    def put(self, events=None, issues=(ISSUE,)):
        events = [point()] if events is None else events
        raw = "".join(json.dumps(e.data())+"\n" for e in events)+( "{bad\n" if issues else "")
        path = self.root/"fixture.jsonl"; path.write_text(raw)
        self.digest = hashlib.sha256(raw.encode()).hexdigest()
        self.store.replace_source("a", "test", str(path), self.digest, events, {"issues": list(issues)})
        self.store.set_import_state("complete")
        self.entry = dict(registration="test", path=str(path), sha256=self.digest, **ISSUE, reason=MARKER)

    def parsed(self, entries=None):
        raw = json.dumps({"schema": SCHEMA, "acknowledgments": [self.entry] if entries is None else entries}).encode()
        self.ack.write_bytes(raw)
        return parse_acknowledgments(raw)

    def cli(self, command="report", extra=()):
        out, err = StringIO(), StringIO()
        args = [command, "--db", str(self.root/"audit.sqlite"), *WINDOW, "--activity", "observed", *extra]
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = main(args)
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue(), err.getvalue()

    def baseline_outputs(self):
        for name, issues, events, command, extra in (
            ("healthy", (), [point()], "report", []),
            ("issue", (ISSUE,), [point()], "report", []),
            ("boundary", (), [aggregate()], "report", []),
            ("labels", (ISSUE,), [point()], "report", ["--labels", str(self.labels), "--group-by", "label"]),
            ("markdown", (ISSUE,), [point()], "report", ["--format", "markdown"]),
            ("series", (ISSUE,), [point()], "series", ["--every", "1h"]),
            ("compare", (ISSUE,), [point()], "compare", ["--before-start", "2026-10-20T10:00:00Z",
                "--before-end", "2026-10-20T11:00:00Z", "--after-start", "2026-10-20T11:00:00Z",
                "--after-end", "2026-10-20T12:00:00Z"]),
        ):
            self.put(events, issues)
            code, out, err = self.cli(command, extra)
            self.assertEqual((code, err), (0, ""))
            yield name, out

    def test_01_byte_baselines(self):
        """Fault: omission or an empty list changes existing report, series, compare or Markdown bytes."""
        for name, out in self.baseline_outputs():
            self.assertEqual(hashlib.sha256(out.replace(str(self.root), "<TMP>").encode()).hexdigest(), BASELINES[name], name)
        for issues, name in (((), "healthy"), ((ISSUE,), "issue")):
            self.put(issues=issues); self.parsed([])
            for extra in (["--acknowledgments", str(self.ack)],
                          ["--acknowledgments", str(self.ack), "--format", "markdown"]):
                code, out, err = self.cli(extra=extra)
                self.assertEqual((code, err), (0, ""))
                if "markdown" not in extra:
                    self.assertEqual(hashlib.sha256(out.replace(str(self.root), "<TMP>").encode()).hexdigest(), BASELINES[name])
                else:
                    self.assertEqual(out, self.cli(extra=["--format", "markdown"])[1])

    def test_02_cli_errors(self):
        """Fault: invalid or unreadable files emit reports, write output, or leak supplied values."""
        output = self.root/"report.json"
        for raw in (b"", b"\xff", ("{"+MARKER).encode()):
            self.ack.write_bytes(raw)
            code, out, err = self.cli(extra=["--acknowledgments", str(self.ack), "--output", str(output)])
            self.assertEqual((code, out), (2, "")); self.assertFalse(output.exists())
            self.assertNotIn(MARKER, err)
            self.assertEqual(json.loads(err)["message"], "acknowledgments: invalid UTF-8 JSON or duplicate JSON key")
        code, out, err = self.cli(extra=["--acknowledgments", str(self.root/MARKER), "--output", str(output)])
        self.assertEqual((code, out), (2, "")); self.assertFalse(output.exists())
        self.assertEqual(json.loads(err), {"error": "ValueError", "message": "acknowledgments file cannot be read"})
        self.assertNotIn(MARKER, err)
        self.parsed()
        code, out, err = self.cli(extra=["--acknowledgments", str(self.ack)])
        self.assertEqual((code, err), (0, "")); self.assertNotIn(MARKER, out+err)
        self.assertNotIn('"reason"', out)
        kept = self.root/"kept.json"; kept.write_text("previous")
        self.ack.write_bytes(json.dumps({"schema": SCHEMA,
            "acknowledgments": [{**self.entry, "path": MARKER + "\ud800"}]}).encode())
        code, out, err = self.cli(extra=["--acknowledgments", str(self.ack), "--output", str(kept)])
        self.assertEqual((code, out), (2, "")); self.assertEqual(kept.read_text(), "previous")
        self.assertNotIn(MARKER, err); self.assertNotIn("ud800", err)
        self.assertEqual(json.loads(err)["message"], "acknowledgments entry 1: 'path' must be a nonempty string")

    def test_03_markdown_guard(self):
        """Fault: nonempty acknowledgments produce unqualified Markdown or write an output file."""
        self.parsed(); output = self.root/"report.md"
        code, out, err = self.cli(extra=["--acknowledgments", str(self.ack), "--format", "markdown", "--output", str(output)])
        self.assertEqual((code, out), (2, "")); self.assertFalse(output.exists())
        self.assertEqual(json.loads(err)["message"], "acknowledgments need --format json until Markdown support")

    def test_04_unsupported_commands(self):
        """Fault: series or compare accepts the report-only acknowledgment option."""
        self.parsed()
        for command, extra in (("series", ["--every", "1h"]), ("compare", [
            "--before-start", "2026-10-20T10:00:00Z", "--before-end", "2026-10-20T11:00:00Z",
            "--after-start", "2026-10-20T11:00:00Z", "--after-end", "2026-10-20T12:00:00Z"])):
            code, out, err = self.cli(command, [*extra, "--acknowledgments", str(self.ack)])
            self.assertEqual((code, out), (2, "")); self.assertIn("unrecognized arguments: --acknowledgments", err)

    def test_05_remaining_blockers(self):
        """Fault: one matched issue clears another issue, a same-code issue, a second registration,
        conflict, reset, failure or incomplete import."""
        # adapter_version_mismatch is a retention problem, not a lined issue; "unavailable" covers bad_sources.
        for fault in ("issue", "conflict", "counter_reset_unresolved", "final_result_missing",
                      "unavailable", "in_progress", "unverified", "same_code", "registration"):
            with self.subTest(fault=fault):
                self.store.db.execute("DELETE FROM sources WHERE id='b'"); self.store.db.commit()
                issues = [ISSUE]
                if fault == "issue": issues.append({"code": "json_record_not_object", "line": 3})
                elif fault == "same_code": issues.append({"code": "invalid_json", "line": 5})
                elif fault == "counter_reset_unresolved": issues.append({"code": fault, "line": 3})
                elif fault == "final_result_missing": issues.append({"code": fault})
                if fault == "registration":
                    self.store.put_config({"sources": [{"name": "test"}, {"name": "other"}], "prices": BOOK})
                self.put(issues=issues)
                if fault == "registration":
                    self.store.replace_source("b", "other", self.entry["path"], self.digest, [], {"issues": [ISSUE]})
                if fault in ("conflict", "unavailable"):
                    self.store.replace_source("b", "test", str(self.root/"other.jsonl"), "1"*64,
                        [replace(point(), model="other")] if fault == "conflict" else [], {"issues": []})
                    if fault == "unavailable":
                        with self.store.db:
                            self.store.db.execute("UPDATE sources SET status='unavailable' WHERE id='b'")
                if fault in ("in_progress", "unverified"): self.store.set_import_state(fault)
                result = report(self.store, self.selection, acknowledgments=self.parsed())
                self.assertEqual(result["coverage"]["acknowledgments"]["entries"]["matched"], 1)
                self.assertEqual(result["status"], "partial")
                self.assertIsNone(result["summary"]["tokens"]["input_total"])
                if fault in ("issue", "counter_reset_unresolved", "final_result_missing", "same_code", "registration"):
                    self.assertEqual(result["coverage"]["acknowledgments"]["source_issue_counts"],
                                     dict(total=2, acknowledged=1, effective_blockers=1))

    def test_06_selection_guards(self):
        """Fault: acknowledgment bypasses unknown time, descendant hierarchy, overlap or boundary withholding."""
        for fault, events, selection in (
            ("time", [point(), point("unknown", at=None)], self.selection),
            ("hierarchy", [point(parent="root")], replace(self.selection, agents=("root",), descendants=True)),
            ("overlap", [point(), aggregate(start=START, end=START+3600, run="point")], self.selection),
            ("boundary", [aggregate()], self.selection),
        ):
            with self.subTest(fault=fault):
                self.put(events); result = report(self.store, selection, acknowledgments=self.parsed())
                self.assertEqual(result["coverage"]["acknowledgments"]["source_issue_counts"]["acknowledged"], 1)
                self.assertEqual(result["status"], "ready_with_missing_metrics" if fault == "boundary" else "partial")
                self.assertIsNone(result["summary"]["tokens"]["input_total"])
                if fault == "boundary":
                    self.assertEqual(result["coverage"]["omitted"]["aggregate_crosses_boundary"], 1)
                    self.assertIsNone(result["summary"]["activity"]["agent_hours"])
                    self.assertEqual(result["summary"]["events"], 0)

    def test_07_release_and_disclosure(self):
        """Fault: sole acknowledgment fails to release unchanged amounts, hides diagnostics or invents missing metrics."""
        for fault, events in (("complete", [point()]), ("price", [point(model="unpriced")]),
                              ("activity", [point(activity_start=None, activity_end=None, activity_basis=None)])):
            with self.subTest(fault=fault):
                self.put(events); before = report(self.store, self.selection); parsed = self.parsed()
                after = report(self.store, self.selection, acknowledgments=parsed)
                self.assertEqual(before["status"], "partial")
                self.assertEqual(after["status"], "ready" if fault == "complete" else "ready_with_missing_metrics")
                self.assertEqual(after["summary"]["tokens"], before["summary"]["known_token_subtotals"])
                self.assertEqual(after["groups"][0]["tokens"], before["groups"][0]["known_token_subtotals"])
                self.assertEqual(after["coverage"]["source_issues"], before["coverage"]["source_issues"])
                for key in ("known_token_subtotals", "cli_recorded_usd_estimate", "list_estimates"):
                    a, b = after["summary"][key], before["summary"][key]
                    if key == "cli_recorded_usd_estimate": self.assertEqual(a["known_total"], b["known_total"])
                    elif key == "list_estimates":
                        self.assertEqual([(q["known_min"], q["known_max"]) for q in a], [(q["known_min"], q["known_max"]) for q in b])
                    else: self.assertEqual(a, b)
                disclosure = after["coverage"]["acknowledgments"]
                self.assertEqual(disclosure["source_issue_counts"], dict(total=1, acknowledged=1, effective_blockers=0))
                self.assertEqual(disclosure["file_sha256"], parsed.sha256)
                self.assertEqual(disclosure["entries"], dict(matched=1, lapsed=0, unmatched=0))
                self.assertEqual(disclosure["release_basis"], "unchanged_recorded_amounts")
                self.assertTrue(disclosure["unknown_usage_accepted"])
                self.assertEqual(disclosure["dispositions"][0]["source_issue_index"], 0)
                self.assertEqual(report(self.store, self.selection), before)
                self.assertEqual(after["summary"]["unpriced_events"], int(fault == "price"))
                if fault == "activity": self.assertIsNone(after["summary"]["activity"]["agent_hours"])
        entries = [self.entry, {**self.entry, "sha256": "0"*64}, {**self.entry, "code": "json_record_not_object"}]
        result = report(self.store, self.selection, acknowledgments=self.parsed(entries))
        disclosure = result["coverage"]["acknowledgments"]
        self.assertEqual(disclosure["entries"], dict(matched=1, lapsed=1, unmatched=1))
        self.assertEqual(disclosure["source_issue_counts"], dict(total=1, acknowledged=1, effective_blockers=0))
        for supplied, shown, explanation in zip(entries, disclosure["dispositions"], ("matched", "digest_changed", "issue_absent")):
            self.assertEqual(shown["explanation"], explanation)
            for key in ("registration", "path", "sha256", "code", "line"): self.assertEqual(shown[key], supplied[key])
            if explanation != "matched":
                self.assertEqual(shown.get("stored_registration"), "test"); self.assertEqual(shown.get("stored_sha256"), self.digest)
        self.assertNotIn('"reason"', json.dumps(result)); self.assertNotIn(MARKER, json.dumps(result))
        result = report(self.store, self.selection, acknowledgments=self.parsed(entries[1:]))
        self.assertFalse(result["coverage"]["acknowledgments"]["unknown_usage_accepted"])
        self.assertEqual(result["status"], "partial")

    def test_08_snapshot_binding(self):
        """Fault: report loads or matches repeatedly, or matches outside the read snapshot."""
        from workflow_auditor import query
        parsed = self.parsed(); original = query.match_acknowledgments
        def match(acknowledgments, loaded):
            self.assertTrue(self.store.db.in_transaction)
            return original(acknowledgments, loaded)
        with mock.patch.object(query, "match_acknowledgments", side_effect=match) as matcher, \
             mock.patch.object(query, "_load", wraps=query._load) as loader:
            report(self.store, self.selection, acknowledgments=parsed)
        matcher.assert_called_once(); loader.assert_called_once()
