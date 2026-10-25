from dataclasses import replace
from unittest import mock
import hashlib
import json
import unittest

from workflow_auditor import cli, query
from workflow_auditor.adapters import ingest
from workflow_auditor.labels import Labels
from workflow_auditor.query import report, series
import test_acknowledgment_report as fixtures
from test_acknowledgment_report import BASELINES as REPORT_BYTES, ISSUE, MARKER
from test_crossing_activity import START, BOOK, LABELS, point, aggregate

BASELINES = {  # Captured through cli.main before code changes; only the temporary root is replaced.
    "healthy-series-json": "a692a88220c9bb49a5567a57e26ee102014fe4f0f4eb39cfe98d1398a9c3b419",
    "healthy-series-markdown": "8c8d033a5d6199af2abfe6c531b7c196b85b1127d8debaed57c0a3de988eed27",
    "healthy-report-markdown": "d7344e2d9c4e643210ac04b60f2be268c9a1e449e0539739b8f1d245dcf1d2f4",
    "issue-series-json": "fc4a8fede1c6afe9c5927bf8ed2c35c6448e6577c109f9e9c06b436e5ce52fba",
    "issue-series-markdown": "90e8c951a9c3215b71033a1656b3665613950b5871a715e238439068f500c848",
    "labels-series-json": "b81fea8369e012b615127dd34693f127abbf10523d9ec4d7e0babc29f1f4bcee",
    "labels-series-markdown": "c0fa7c9d425f52eb4d6de4dd514a3255500bc33ff4aff73c396a7e5fe183d2c8",
    "labels-report-markdown": "f8c69dbab18aacb4993095a2a908a648657c8be30976b9aa6d45d13dff7fb748",
    "boundary-series-json": "b84437918e997273e13b30b43fe78e98872809f20c547e1a8e81fff4281dff90",
    "boundary-series-markdown": "8252d5e8e422ec515a04c9326b0457fab77ecc79138b55f9640817b5d6fe9804",
    "boundary-report-markdown": "a86c01ccfc7975b6592045e413e3fa16840961a08782d8911b0df01dc39b7bd9",
    "retained-series-json": "0bfa3ca9a731cd9cd882ee15a99b973fb253596314d03f4d790bfcb238ad4f0f",
    "retained-series-markdown": "b886f85ceec0c6061e7b5115e8de0d58dbbf3393286a076e760600f83fc50c3f",
}


class AcknowledgmentReportingTests(unittest.TestCase):
    setUp, put, parsed, cli = (fixtures.AcknowledgmentReportTests.setUp, fixtures.AcknowledgmentReportTests.put,
                              fixtures.AcknowledgmentReportTests.parsed, fixtures.AcknowledgmentReportTests.cli)

    def baseline_outputs(self, extra=()):
        for name, issues, events in (("healthy", (), [point()]), ("issue", (ISSUE,), [point()]),
                                    ("labels", (ISSUE,), [point()]), ("boundary", (), [aggregate()]),
                                    ("retained", (), [point()])):
            with self.store.db:
                self.store.db.execute("DELETE FROM sources")
            self.store.put_config({"sources": [{"name": "test"}], "prices": BOOK})
            self.put(events, issues)
            if name == "retained":
                with self.store.db:
                    self.store.db.execute("DELETE FROM sources")
                config = {"sources": [{"name": "test", "kind": "normalized", "roots": [self.entry["path"]]}], "prices": BOOK}
                ingest(self.store, config)
                (self.root/"fixture.jsonl").unlink(); ingest(self.store, config)
                self.assertEqual(self.store.sources()[0]["status"], "rotated")
            labels = ["--labels", str(self.labels), "--group-by", "label"] if name == "labels" else []
            cases = [("series", fmt) for fmt in ("json", "markdown")]
            if name != "retained": cases.append(("report", "markdown"))
            for command, fmt in cases:
                code, out, err = self.cli(command, [*labels, *(["--every", "1h"] if command == "series" else []),
                                                   "--format", fmt, *extra])
                self.assertEqual((code, err), (0, ""))
                yield f"{name}-{command}-{fmt}", hashlib.sha256(out.replace(str(self.root), "<TMP>").encode()).hexdigest()

    def test_01_byte_baselines(self):
        """Fault: unconditional acknowledgment fields change omission or empty-list bytes."""
        expected = {**BASELINES, "issue-report-markdown": REPORT_BYTES["markdown"]}
        self.assertEqual(dict(self.baseline_outputs()), expected)
        self.parsed([])
        self.assertEqual(dict(self.baseline_outputs(["--acknowledgments", str(self.ack)])), expected)

    def test_02_period_equivalence(self):
        """Fault: per-period gates, groups, label selection or global counts diverge from report."""
        self.put([point(), point("later", at=START+3700, repo="other", agent="child",
                                 activity_start=START+3600, activity_end=START+7200)])
        parsed = self.parsed()
        for grouping in ((), ("repo", "role"), ("label",)):
            for label in (None, "task", "unlabeled"):
                selection = replace(self.selection, until=START+10800, labels=LABELS, label=label, group_by=grouping)
                result = series(self.store, selection, "1h", "UTC", acknowledgments=parsed)
                self.assertEqual(result["coverage"], report(self.store, selection, acknowledgments=parsed)["coverage"])
                for i, period in enumerate(result["periods"]):
                    standalone = report(self.store, replace(selection, since=START+i*3600, until=START+(i+1)*3600), acknowledgments=parsed)
                    for key in ("status", "summary", *(["groups"] if grouping else [])):
                        self.assertEqual(period[key], standalone[key], key)
                    self.assertEqual(period["omitted"], standalone["coverage"]["omitted"])
                    counts = {key: standalone["coverage"]["acknowledgments"][key] for key in ("entries", "source_issue_counts")}
                    self.assertEqual(period["acknowledgments"], counts)
                    self.assertEqual(counts["source_issue_counts"], dict(total=1, acknowledged=1, effective_blockers=0))
                    self.assertNotIn("trust_scope", period)

    def test_03_mixed_dispositions(self):
        """Fault: range receipts disappear, stale entries release trust, or identities repeat in groups."""
        entries = [self.entry, {**self.entry, "sha256": "0"*64}, {**self.entry, "code": "json_record_not_object"}]
        for rows in (entries, entries[1:]):
            parsed = self.parsed(rows)
            result = series(self.store, replace(self.selection, group_by=("repo",)), "1h", "UTC", acknowledgments=parsed)
            standalone = report(self.store, self.selection, acknowledgments=parsed)
            disclosure = result["coverage"]["acknowledgments"]
            self.assertEqual(disclosure, standalone["coverage"]["acknowledgments"])
            self.assertEqual(disclosure["entries"], dict(matched=int(rows is entries), lapsed=1, unmatched=1))
            self.assertEqual([d["explanation"] for d in disclosure["dispositions"]][-2:], ["digest_changed", "issue_absent"])
            for d in disclosure["dispositions"][-2:]:
                self.assertEqual((d["stored_registration"], d["stored_sha256"]), ("test", self.digest))
            self.assertNotIn(self.entry["path"], json.dumps(result["periods"]))
            text = cli.markdown(result)
            self.assertEqual(text.count("| test | " + cli.acknowledgment_text(self.entry["path"])), len(rows))
            self.assertEqual(text.count(parsed.sha256), 1)
            self.assertIn("matched: " + str(int(rows is entries)) + "; lapsed: 1; unmatched: 1", text)
            self.assertEqual("No acknowledgment applied." in text, rows is not entries)
            self.assertEqual(result["periods"][0]["status"], "ready" if rows is entries else "partial")
            if rows is not entries:
                for view in (text, cli.markdown(standalone)):
                    self.assertNotIn("unknown usage may be missing", view)
                    self.assertNotIn("(source issues acknowledged)", view)

    def test_04_three_period_crossing(self):
        """Fault: acknowledging an issue restores an unevidenced crosser's hours or consumption."""
        self.put([aggregate(end=START+10900), point()])
        for method in ("observed", "buckets"):
            result = series(self.store, replace(self.selection, until=START+10800, activity=method), "1h", "UTC", acknowledgments=self.parsed())
            for i, period in enumerate(result["periods"]):
                summary = period["summary"]
                self.assertEqual(period["status"], "ready_with_missing_metrics")
                self.assertEqual(period["omitted"]["aggregate_crosses_boundary"], 1)
                self.assertEqual(summary["events"], int(i == 0))
                self.assertTrue(all(v is None for v in summary["tokens"].values()))
                self.assertEqual(summary["known_token_subtotals"]["input_total"], 100 if i == 0 else 0)
                self.assertEqual([q["known_min"] for q in summary["list_estimates"]], ["0.000131"] if i == 0 else [])
                self.assertAlmostEqual(summary["activity"]["known_agent_hours"],
                                       (1 if method == "observed" else 1/6) if i == 0 else 0)
                self.assertEqual(summary["activity"]["agents_or_aggregates_missing_activity"], ["root"])
                for key in ("agent_hours", "project_active_hours", "parallelism"):
                    self.assertIsNone(summary["activity"][key])

    def test_05_unsafe_text(self):
        """Fault: disclosure echoes reasons, raw HTML, newlines or Markdown identity syntax."""
        unsafe = "ID|\\\n\r<>`*_[]&$#()!~"
        parsed = self.parsed([{**self.entry, "registration": unsafe, "path": unsafe}])
        expected = "ID\\|\\\\&#10;&#13;&lt;&gt;\\`\\*\\_\\[\\]&amp;\\$\\#\\(\\)\\!\\~"
        for command in ("report", "series"):
            code, text, err = self.cli(command, ["--acknowledgments", str(self.ack), "--format", "markdown",
                                                *(["--every", "1h"] if command == "series" else [])])
            self.assertEqual((code, err), (0, "")); self.assertNotIn(MARKER, text)
            self.assertNotIn(unsafe, text); self.assertEqual(text.count(expected), 2)
            self.assertIn("No acknowledgment applied.", text)
            self.assertNotIn("unknown usage may be missing", text)
            self.assertNotIn("(source issues acknowledged)", text)
        result = report(self.store, self.selection, acknowledgments=self.parsed())
        row = result["coverage"]["acknowledgments"]["dispositions"][0]
        row.update(registration=unsafe, path=unsafe, stored_registration=unsafe, reason=MARKER)
        text = cli.markdown(result)
        self.assertEqual(text.count(expected), 3); self.assertNotIn(MARKER, text)
        self.assertNotIn("reason", text)
        bare = next(line for line in text.splitlines() if line.startswith("| " + expected))
        for char in "\\|`*_[]{}()#+-!.~$":
            bare = bare.replace("\\" + char, "")
        for entity in ("&amp;", "&lt;", "&gt;", "&#10;", "&#13;"):
            bare = bare.replace(entity, "")
        self.assertEqual(set(bare.replace(" | ", "")) & set("\\`*_[]{}()#+-!.~$&<>"), set())

    def test_06_release_qualification(self):
        """Fault: released report/period statuses, including empty groups, missing metrics or a
        partial neighbor, lose the accepted-gap warning or claim released totals while partial."""
        self.parsed()
        sentence = "Source issues acknowledged: 1; unknown usage may be missing. Released totals use unchanged recorded amounts."
        for grouping in ((), ("repo",)):
            selection = replace(self.selection, until=START+7200, group_by=grouping)
            parsed = self.parsed()
            for offset in (0, 3600):
                result = report(self.store, replace(selection, since=START+offset), acknowledgments=parsed)
                self.assertEqual(result["status"], "ready")
                status = next(line for line in cli.markdown(result).splitlines() if line.startswith("Status:"))
                self.assertIn(sentence, status)
            result = series(self.store, selection, "1h", "UTC", acknowledgments=parsed)
            text = cli.markdown(result)
            self.assertEqual(text.count(sentence), 1)
            self.assertLess(text.index(sentence), text.index("| Local start" if not grouping else "| Group"))
            statuses = [line for line in text.splitlines() if "| ready" in line]
            self.assertEqual(len(statuses), 2)
            self.assertTrue(all("ready (source issues acknowledged)" in line for line in statuses))
        self.put([aggregate(end=START+10900), point()])
        parsed = self.parsed()
        result = report(self.store, self.selection, acknowledgments=parsed)
        self.assertEqual(result["status"], "ready_with_missing_metrics")
        status = next(line for line in cli.markdown(result).splitlines() if line.startswith("Status:"))
        self.assertIn(sentence, status)
        result = series(self.store, self.selection, "1h", "UTC", acknowledgments=parsed)
        self.assertEqual([p["status"] for p in result["periods"]], ["ready_with_missing_metrics"])
        text = cli.markdown(result)
        self.assertEqual(text.count(sentence), 1)
        self.assertIn("ready_with_missing_metrics (source issues acknowledged)", text)
        self.put(); self.store.set_import_state("in_progress")
        parsed = self.parsed()
        short = "Source issues acknowledged: 1; unknown usage may be missing."
        result = report(self.store, self.selection, acknowledgments=parsed)
        self.assertEqual(result["status"], "partial")
        status = next(line for line in cli.markdown(result).splitlines() if line.startswith("Status:"))
        self.assertIn(short, status); self.assertNotIn("Released totals", status)
        result = series(self.store, self.selection, "1h", "UTC", acknowledgments=parsed)
        self.assertEqual([p["status"] for p in result["periods"]], ["partial"])
        text = cli.markdown(result)
        self.assertEqual(text.count(short), 1); self.assertNotIn("Released totals", text)
        self.assertIn("partial (source issues acknowledged)", text)

    def test_07_omission_and_compare(self):
        """Fault: acknowledgment persists without its file, changes compare, or compare accepts it."""
        self.parsed()
        for command in ("report", "series"):
            extra = ["--every", "1h"] if command == "series" else []
            self.assertEqual(self.cli(command, [*extra, "--acknowledgments", str(self.ack)])[0], 0)
            code, text, err = self.cli(command, extra)
            self.assertEqual((code, err), (0, "")); self.assertNotIn('"acknowledgments"', text)
            result = json.loads(text)
            view = result["periods"][0] if command == "series" else result
            self.assertEqual(view["status"], "partial"); self.assertIsNone(view["summary"]["tokens"]["input_total"])
        extra = ["--before-start", "2026-10-20T10:00:00Z", "--before-end", "2026-10-20T11:00:00Z",
                 "--after-start", "2026-10-20T11:00:00Z", "--after-end", "2026-10-20T12:00:00Z"]
        code, text, err = self.cli("compare", extra)
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(hashlib.sha256(text.replace(str(self.root), "<TMP>").encode()).hexdigest(), REPORT_BYTES["compare"])
        code, text, err = self.cli("compare", [*extra, "--acknowledgments", str(self.ack)])
        self.assertEqual((code, text), (2, "")); self.assertIn("unrecognized arguments: --acknowledgments", err)

    def test_08_work_per_series(self):
        """Fault: series parses/loads/matches per period, outside the snapshot, or after label assignment."""
        self.parsed(); load, match, assign = query._load, query.match_acknowledgments, Labels.assign
        def loading(store):
            self.assertTrue(store.db.in_transaction)
            return load(store)
        def matching(parsed, loaded):
            self.assertTrue(loader.call_args.args[0].db.in_transaction)
            self.assertEqual(assigner.call_count, 0)
            return match(parsed, loaded)
        with mock.patch.object(cli, "parse_acknowledgments", wraps=cli.parse_acknowledgments) as parser, \
             mock.patch.object(query, "_load", side_effect=loading) as loader, \
             mock.patch.object(query, "match_acknowledgments", side_effect=matching) as matcher, \
             mock.patch.object(Labels, "assign", autospec=True,
                        side_effect=lambda labels, events: assign(labels, events)) as assigner:
            code, out, err = self.cli("series", ["--every", "1min", "--labels", str(self.labels), "--acknowledgments", str(self.ack)])
            self.assertEqual((code, err), (0, "")); self.assertEqual(len(json.loads(out)["periods"]), 60)
        parser.assert_called_once(); loader.assert_called_once(); matcher.assert_called_once(); assigner.assert_called_once()
        for raw in (b"", b"\xff"):
            self.ack.write_bytes(raw)
            code, out, err = self.cli("series", ["--every", "1h", "--acknowledgments", str(self.ack)])
            self.assertEqual((code, out), (2, "")); self.assertEqual(json.loads(err)["message"], "acknowledgments: invalid UTF-8 JSON or duplicate JSON key")
