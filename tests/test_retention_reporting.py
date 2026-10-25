from pathlib import Path
from unittest import mock
import json
import tempfile
import unittest

from workflow_auditor.adapters import ingest
from workflow_auditor.cli import markdown
from workflow_auditor.labels import read_labels
from workflow_auditor.query import Selection, report, series
from workflow_auditor.retention import ADAPTER_VERSIONS, root_states, suppress_empty
from workflow_auditor.store import Store
from test_retention_receipts import normalized, serializations


class ReportingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.logs = self.root/'logs'; self.logs.mkdir()
        self.path = self.logs/'a.jsonl'; self.write()
        self.source = {'name': 'test', 'kind': 'normalized', 'roots': [str(self.logs)]}
        self.config = {'sources': [self.source]}
        self.store = Store(self.root/'test.sqlite'); self.addCleanup(self.store.close)

    def write(self, text=None):
        self.path.write_text(json.dumps(normalized())+'\n' if text is None else text)

    def ingest(self):
        return ingest(self.store, self.config)

    def view(self):
        return report(self.store, Selection())

    def row(self):
        return next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.path))

    def save(self, **changes):
        row = {**self.row(), **changes}
        detail = {k: v for k, v in row.items() if k not in ('id', 'registration', 'path', 'sha256', 'status')}
        with self.store.db:
            self.store.db.execute('UPDATE sources SET sha256=?,status=?,detail=? WHERE id=?',
                                  (row['sha256'], row['status'], json.dumps(detail), row['id']))

    def empty_failure(self, expected=True):
        codes = [i['code'] for i in self.view()['coverage']['source_issues']]
        self.assertEqual('no_matching_files' in codes, expected)

    def test_01_all_children(self):
        """Fault: deleting all captured children adds a spurious empty sentinel or loses metrics."""
        other = self.logs/'b.jsonl'; other.write_bytes(self.path.read_bytes()); self.ingest()
        before = self.view()['summary']; self.path.unlink(); other.unlink()
        for _ in range(2):
            self.assertEqual(self.ingest()['unavailable_sources'], 0); self.empty_failure(False)
            view = self.view(); self.assertEqual(view['summary'], before)
            self.assertEqual((view['coverage']['source_files'], view['coverage']['rotated_sources']['observations']), (2, 2))

    def test_02_zero_observations(self):
        """Fault: a captured zero-observation file is rejected as qualifying history."""
        self.write(''); self.ingest(); self.path.unlink(); self.ingest(); self.empty_failure(False)
        self.assertEqual(self.view()['coverage']['source_files'], 1)
        self.assertEqual(self.view()['coverage']['rotated_sources']['observations'], 0)
        self.assertNotEqual(self.view()['status'], 'partial')

    def test_03_ineligible_history(self):
        """Fault: first-empty, inactive, retired, excluded, corrupt or null-digest history suppresses failure."""
        self.path.unlink(); self.ingest(); self.empty_failure()
        for fault in ('retired', 'excluded', 'receipt', 'count', 'null', 'inactive'):
            with self.subTest(fault=fault):
                self.source.pop('exclude', None); self.source['name'] = 'test'
                self.write(); self.ingest(); self.path.unlink()
                if fault == 'retired': self.save(status='retired')
                if fault == 'excluded': self.source['exclude'] = ['a.*']
                if fault == 'receipt': self.save(receipt={})
                if fault == 'count': self.save(observations=2, receipt={**self.row()['receipt'], 'observations': 2})
                if fault == 'null': self.save(sha256=None)
                if fault == 'inactive': self.source['name'] = 'new'
                self.ingest(); self.empty_failure()
                self.assertEqual(self.view()['status'], 'partial')
        self.source['name'] = 'test'; self.write(); self.ingest()
        row = {**self.row(), 'status': 'rotated', 'sha256': None}
        self.assertFalse(suppress_empty(self.source, root_states(self.source), [row]))
        row.update(sha256=self.row()['sha256'], registration='other')
        self.assertFalse(suppress_empty(self.source, root_states(self.source), [row]))
        other = self.logs/'b.jsonl'; other.write_bytes(self.path.read_bytes()); self.ingest()
        self.path.unlink(); other.unlink(); self.source['exclude'] = ['b.*']; self.ingest()
        original = self.path; self.path = other; self.save(receipt={}); self.path = original
        self.source['exclude'] = []; self.ingest(); self.empty_failure()
        self.assertEqual(self.view()['status'], 'partial')

    def test_04_policy_and_root(self):
        """Fault: require_present, a missing directory or a missing explicit-file parent hides collection failure."""
        self.ingest(); self.path.unlink(); self.ingest(); row = self.row()
        self.source['rotation_policy'] = 'require_present'
        self.assertFalse(suppress_empty(self.source, root_states(self.source), [row]))
        self.ingest(); self.empty_failure(); self.assertEqual(self.row()['status'], 'unavailable')
        self.source.pop('rotation_policy'); self.logs.rmdir(); self.ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.source['roots'] = [str(self.path)]; self.ingest()
        self.assertEqual(self.view()['status'], 'partial')
        self.assertEqual(self.row()['status'], 'unavailable')
        self.logs.mkdir(); self.write(); self.ingest(); self.path.unlink()
        self.store.failed_source('empty', 'test', '', 'no_matching_files')
        self.ingest(); self.empty_failure(False)

    def test_05_mixed_roots(self):
        """Fault: another healthy root suppresses unresolved empty or root sentinels and failed history."""
        other = self.root/'other'; other.mkdir(); self.source['roots'].append(str(other))
        self.path.unlink(); self.ingest(); other.rmdir(); self.ingest(); self.empty_failure()
        self.assertIn({'source': str(other), 'code': 'registered_root_missing'}, self.view()['coverage']['source_issues'])
        self.write(); self.ingest(); self.path.unlink(); self.ingest()
        self.assertEqual(self.row()['status'], 'rotated')
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 1)
        other.mkdir(); bad = other/'bad.jsonl'; bad.write_text('')
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('first import')): self.ingest()
        self.assertIn({'source': str(bad), 'code': 'source_import_failed:PermissionError'}, self.view()['coverage']['source_issues'])
        bad.unlink(); self.ingest(); self.empty_failure()
        self.assertEqual(self.view()['status'], 'partial')
        self.assertIn({'source': str(bad), 'code': 'source_import_failed:PermissionError'}, self.view()['coverage']['source_issues'])
        self.source['accounting_role'] = 'corroborating'; self.ingest()
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 0)
        self.source['roots'].remove(str(other)); self.ingest()
        self.assertNotIn({'source': str(bad), 'code': 'source_import_failed:PermissionError'}, self.view()['coverage']['source_issues'])

    def test_06_sentinel_recovery(self):
        """Fault: fail/recover/fail retires an empty sentinel early or resolves the wrong missing root."""
        self.path.unlink(); self.ingest(); self.empty_failure()
        moved = self.root/'moved'
        for _ in range(2):
            self.logs.rename(moved); self.ingest(); self.empty_failure()
            moved.rename(self.logs); self.ingest(); self.empty_failure()
        self.write(); self.ingest(); self.empty_failure(False)
        self.path.unlink(); self.ingest(); self.ingest(); self.empty_failure(False)
        for _ in range(2):
            self.logs.rename(moved); self.ingest(); self.empty_failure(False)
            self.assertIn({'source': str(self.logs), 'code': 'registered_root_missing'}, self.view()['coverage']['source_issues'])
            self.source['roots'] = [str(self.logs/'..'/'logs')]; self.ingest()
            placeholder = next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.logs))
            self.assertEqual(placeholder['status'], 'unavailable')
            self.source['roots'] = [str(self.logs)]
            moved.rename(self.logs); self.ingest()
            self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 0)
        missing = self.logs/'missing'; self.source['roots'].append(str(missing)); self.ingest()
        self.source['roots'].remove(str(missing)); self.ingest()
        self.assertNotIn({'source': str(missing), 'code': 'registered_root_missing'}, self.view()['coverage']['source_issues'])
        self.source['roots'].append(str(missing)); missing.mkdir(); self.ingest()
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 0)

    def test_07_trust_problems(self):
        """Fault: suppression launders parser, attribution, version or last-import failures in either field."""
        for fault in ('parser', 'attribution', 'version', 'issues', 'collection_issues'):
            with self.subTest(fault=fault):
                self.source['kind'] = 'normalized'; self.write(); self.ingest(); self.path.unlink()
                if fault == 'parser': self.save(issues=[{'code': 'invalid_json'}])
                if fault == 'attribution': self.source['kind'] = 'codex'
                if fault in ('issues', 'collection_issues'): self.save(**{fault: [{'code': 'source_import_failed:PermissionError'}]})
                with mock.patch.dict(ADAPTER_VERSIONS, {'normalized': 2} if fault == 'version' else {}):
                    self.ingest(); view = self.view(); self.assertEqual(view['status'], 'partial')
                    self.empty_failure(fault in ('issues', 'collection_issues'))
                    self.assertEqual(view['summary']['events'], 1)
                    if fault in ('issues', 'collection_issues'): self.assertEqual(self.row()['status'], 'unavailable')
                    else: self.assertIn('rotated_sources', view['coverage'])

    def test_08_inactive_history(self):
        """Fault: removed/renamed registrations migrate observations, omit retired history or count placeholders."""
        self.write(json.dumps(normalized())+'\n'+json.dumps({**normalized(), 'key': 'two'})+'\n')
        other = self.logs/'b.jsonl'; other.write_text(json.dumps(normalized())+'\n')
        self.ingest()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0], 3)
        self.path.unlink(); other.unlink(); self.ingest(); self.source['exclude'] = ['a.*']; self.ingest()
        self.store.failed_source('placeholder', 'aaa', '', 'no_matching_files')
        self.store.replace_source('zero', 'aaa', 'synthetic', '0'*64, [], {})
        self.source.update(name='new', exclude=[]); self.ingest()
        before = self.view(); self.assertEqual(before['summary']['events'], 0)
        self.assertIn('inactive_registrations', before['coverage'])
        self.assertEqual(before['coverage']['inactive_registrations'], [
            {'registration': 'aaa', 'captured_files': 1, 'observations': 0},
            {'registration': 'test', 'captured_files': 2, 'observations': 3}])
        self.source['name'] = 'test'; self.ingest()
        self.assertEqual(self.view()['summary']['events'], 2)
        self.assertEqual(len(self.view()['coverage']['inactive_registrations']), 1)
        self.assertEqual(self.store.db.execute("SELECT status FROM sources WHERE id='placeholder'").fetchone()[0], 'unavailable')

    def test_09_markdown_series(self):
        """Fault: conditional history/provenance summaries disappear, repeat per period or damage labels."""
        labels = self.root/'labels.json'; labels.write_bytes(Path('examples/history/labels.json').read_bytes())
        self.ingest(); self.path.unlink(); self.ingest()
        self.save(receipt={**self.row()['receipt'], 'adapter_version': 0})
        self.store.replace_source('excluded', 'test', 'synthetic', '0'*64, [], {})
        with self.store.db: self.store.db.execute("UPDATE sources SET status='retired' WHERE id='excluded'")
        self.store.replace_source('inactive', 'old', 'synthetic', '0'*64, [], {})
        selection = Selection(since=1792454400, until=1792627200, labels=read_labels(labels), group_by=('label',))
        result = series(self.store, selection, every='day', tz='UTC'); text = markdown(result)
        for label in ('Retained rotation:', 'Retained provenance problems:', 'Membership exclusions:', 'Inactive registrations:'):
            self.assertEqual(text.count(label), 1)
            self.assertEqual(markdown(report(self.store, selection)).count(label), 1)
        self.assertIn('- Labels:', markdown(report(self.store, selection)))
        self.assertEqual(len(result['periods']), 2)
        self.assertEqual(result['coverage']['rotated_sources']['observations'], 1)
        self.save(status='unavailable')
        self.assertNotIn('Retained rotation:', markdown(self.view()))
        self.assertIn('Retained provenance problems:', markdown(self.view()))

    def test_10_unchanged_bytes_and_figures(self):
        """Fault: unaffected CLI bytes drift from main or disclosure alters active accounting figures."""
        self.assertEqual(serializations(self.root), [
            'd35a6b95c1bb0693e4c2f31526572cc4adad51cfdc5acd17d1522b2fe62045be',
            'ba03d8fb8f226bac0b96e4a1beb76fab3924fe1943937a9f64c3b704b7ced65a',
            'aaa61eaa28545485be52fca5ef81a6d5c4fe30ef36cd86df914789b0a1470582',
            'b5d7a5033d2e10ac601ad59599980642cf858732819000f515d0c0758fe87581',
            'e4682cec058f750e2cdb606cb7929c23e17c63663f66d61c93f8bce64619e031',
            '400350a083aae8e6198e27d93e07e2c9205360f808222afb8b41b5b64d55e47d',
            '21722c49e94384e5a6ac121c705d4e21dba6903bc928c7e2cdbb79dce941d78a',
            'f13cb1495090b43eaf6ab1ed2a8c2cd48a53bbae012541bcc95d14829d0e84c7'])
        self.ingest(); before = self.view()['summary']
        self.store.replace_source('old', 'old', 'synthetic', '0'*64, [normalized_event()], {})
        self.assertEqual(self.view()['summary'], before)

    def test_11_removed_root_failures(self):
        """Fault: removed roots keep failures, re-added missing roots stay hidden, or overlaps retire placeholders."""
        missing = self.root/'missing'; self.source['roots'].append(str(missing))
        issue = {'source': str(missing), 'code': 'registered_root_missing'}
        for _ in range(2):
            self.ingest(); self.assertIn(issue, self.view()['coverage']['source_issues'])
            self.assertEqual(self.view()['status'], 'partial')
            self.source['roots'].remove(str(missing)); self.ingest()
            self.assertNotEqual(self.view()['status'], 'partial')
            sentinel = next(s for s in self.store.sources(True) if s['path'] == str(missing))
            self.assertEqual(sentinel['status'], 'retired')
            self.source['roots'].append(str(missing)); self.ingest(); self.ingest()
            self.assertEqual(self.view()['coverage']['source_issues'].count(issue), 1)
            self.assertEqual(len([s for s in self.store.sources(True) if s['path'] == str(missing)]), 1)
        self.source['roots'][-1] = str(missing/'..'/'missing'); self.ingest()
        self.assertEqual(next(s for s in self.store.sources(True) if s['path'] == str(missing))['status'], 'unavailable')
        self.source['roots'][-1] = str(missing)
        self.source['roots'].remove(str(missing))
        other = self.root/'other'; other.mkdir(); bad = other/'bad.jsonl'; bad.write_text('')
        self.source['roots'].append(str(other))
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError): self.ingest()
        bad.unlink(); other.rmdir(); self.source['roots'].remove(str(other))
        self.source['roots'].append(str(self.root)); self.ingest()
        placeholder = lambda: next(s for s in self.store.sources(True) if s['path'] == str(bad))
        self.assertEqual(placeholder()['status'], 'unavailable')
        settings = placeholder().get('membership_settings'); original = self.path; self.path = bad
        self.save(membership_settings=None)
        self.source['roots'].remove(str(self.root)); self.ingest()
        self.assertEqual(placeholder()['status'], 'unavailable')  # Older placeholders need exclusion proof.
        self.save(membership_settings=settings); self.path = original; self.ingest()
        self.assertEqual(placeholder()['status'], 'retired')

    def test_12_final_unavailable_count(self):
        """Fault: ingest counts only current failures instead of final registered unavailable rows (N1 S1-S3)."""
        missing = self.root/'missing'; self.source['roots'].append(str(missing)); self.ingest()
        self.source['roots'].remove(str(missing))
        self.assertEqual(self.ingest()['unavailable_sources'], 0)
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 0)
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError): self.ingest()
        self.path.unlink()
        self.assertEqual(self.ingest()['unavailable_sources'], 2)
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 2)
        self.store = Store(self.root/'empty.sqlite'); self.addCleanup(self.store.close); self.ingest()
        self.logs.rmdir()
        self.assertEqual(self.ingest()['unavailable_sources'], 2)
        self.assertEqual(self.view()['coverage']['unavailable_primary_sources'], 2)


def normalized_event():
    from workflow_auditor.model import Event
    return Event.from_data({**normalized(), 'key': 'inactive'})
