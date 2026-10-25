from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock
import io
import hashlib
import json
import tempfile
import unittest

from workflow_auditor.adapters import ingest, validate_config
from workflow_auditor.cli import main
from workflow_auditor.labels import read_labels
from workflow_auditor.query import Selection, report, series
from workflow_auditor.retention import captured_file, exclusion_proven, root_states
from workflow_auditor.store import Store
from test_retention_receipts import normalized, serializations


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.logs = self.root/'logs'; self.logs.mkdir()
        self.path = self.logs/'a.jsonl'; self.write()
        self.source = {'name': 'test', 'kind': 'normalized', 'roots': [str(self.path)]}
        self.config = {'sources': [self.source]}
        self.store = Store(self.root/'test.sqlite'); self.addCleanup(self.store.close)
        self.ingest()

    def write(self, rows=None):
        self.path.write_text(''.join(json.dumps(r)+'\n' for r in (rows if rows is not None else [normalized()])))

    def ingest(self):
        return ingest(self.store, self.config)

    def row(self):
        return next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.path))

    def save(self, row):
        detail = {k: v for k, v in row.items() if k not in ('id', 'registration', 'path', 'sha256', 'status')}
        with self.store.db:
            self.store.db.execute('UPDATE sources SET status=?,detail=? WHERE id=?', (row['status'], json.dumps(detail), row['id']))

    def result(self):
        return report(self.store, Selection())

    def directory(self):
        (self.logs/'keep.jsonl').write_text('')
        self.source['roots'] = [str(self.logs)]; self.ingest()

    def test_01_explicit_roots(self):
        """Fault: deleting every explicit root, including an empty file, loses captured history."""
        empty = self.logs/'empty'; empty.write_text('')
        alias = self.root/'alias'; alias.symlink_to(self.path)
        copy = self.logs/'copy'; copy.write_bytes(self.path.read_bytes())
        self.source['roots'] = [str(alias), str(empty), str(copy)]; self.ingest()
        original = self.row()['receipt']; self.path.unlink(); empty.unlink(); copy.unlink()
        for _ in range(2):
            self.assertEqual(self.ingest()['unavailable_sources'], 0)
            result = self.result()
            self.assertEqual((result['status'], result['summary']['events']), ('ready_with_missing_metrics', 1))
            self.assertEqual(result['coverage']['rotated_sources'], dict(files=3, primary_files=3, observations=2,
                             reason='source_disappeared_after_snapshot', attribution_mismatches=0,
                             adapter_version_mismatches=0, unverified_receipts=0))
            self.assertEqual(self.row()['receipt'], original)
        self.source['exclude'] = ['a.*']; self.ingest(); self.source['exclude'] = []; self.ingest()
        self.assertEqual(self.result()['coverage']['unavailable_primary_sources'], 0)
        unseen = self.logs/'never'; self.source['roots'].append(str(unseen)); self.ingest()
        self.assertIn({'source': str(unseen), 'code': 'registered_root_missing'}, self.result()['coverage']['source_issues'])
        self.config['sources'].append({**self.source, 'name': 'never', 'roots': [str(alias)]}); self.ingest()
        self.assertIn({'source': str(alias), 'code': 'registered_root_missing'}, self.result()['coverage']['source_issues'])

    def test_02_missing_directory(self):
        """Fault: a missing directory grants retention or blocks a different healthy root."""
        self.directory(); other = self.root/'other'; other.mkdir()
        child = other/'b.jsonl'; child.write_text('')
        self.source['roots'].append(str(other)); self.ingest()
        child.unlink(); other.rmdir(); self.path.unlink(); self.ingest()
        self.assertEqual(self.row()['status'], 'rotated')
        rows = self.store.sources()
        self.assertEqual(next(s for s in rows if s['path'] == str(child))['collection_issues'], [{'code': 'registered_root_unavailable'}])
        self.assertEqual(self.result()['coverage']['unavailable_primary_sources'], 2)

    def test_03_overlap(self):
        """Fault: an unhealthy overlapping directory blocks qualification through a healthy root."""
        self.directory(); self.source['roots'].append(str(self.root)); self.ingest(); self.path.unlink()
        from workflow_auditor.adapters import discover
        paths, roots, provenance = discover(self.source); roots[0]['health'] = 'error'
        with mock.patch('workflow_auditor.adapters.discover', return_value=(paths, roots, provenance)):
            self.ingest()
        self.assertEqual(self.row()['status'], 'rotated')
        self.assertEqual(self.row()['collection_issues'], [])

    def test_04_recovery(self):
        """Fault: fail/recover/fail root collection erases retention or accumulates sticky failures."""
        self.directory(); self.path.unlink(); self.ingest(); original = self.row()['retention']
        moved = self.root/'moved'
        for _ in range(2):
            self.logs.rename(moved); self.ingest()
            self.assertEqual(self.row()['retention'], original)
            self.assertEqual(self.result()['status'], 'partial')
            self.assertEqual(self.row()['collection_issues'], [{'code': 'registered_root_unavailable'}])
            moved.rename(self.logs); self.ingest(); self.ingest()
            self.assertEqual(self.row()['status'], 'rotated')
            self.assertEqual(self.result()['coverage']['unavailable_primary_sources'], 0)
        self.write(); self.source['roots'] = [str(self.path)]; self.ingest(); self.path.unlink(); self.ingest()
        stat = Path.stat
        def fail(path, *args, **kwargs):
            if path == self.path: raise PermissionError('file root fault')
            return stat(path, *args, **kwargs)
        with mock.patch.object(Path, 'stat', fail): self.ingest()
        self.ingest(); self.assertEqual(self.row()['status'], 'rotated')
        with mock.patch('workflow_auditor.adapters.discover', side_effect=OSError('collection fault')):
            self.ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual(self.row()['collection_issues'], [{'code': 'registered_root_unavailable'}])
        self.assertEqual(self.result()['status'], 'partial')
        self.assertEqual(self.result()['coverage']['import_state']['unavailable_sources'], 1)
        self.ingest(); self.assertEqual(self.row()['status'], 'rotated')

    def test_05_replacement(self):
        """Fault: changed restored content leaves stale or duplicate observations and retention metadata."""
        self.path.unlink(); self.ingest(); event = normalized(); event.update(key='new'); event['tokens']['input_total'] = 99
        self.write([event])
        self.ingest(); self.ingest()
        self.assertEqual((self.row()['status'], self.result()['summary']['events']), ('imported', 1))
        self.assertEqual(self.store.events()[0][0].tokens.input_total, 99)
        self.assertNotIn('retention', self.row())
        self.assertNotIn('rotated_sources', self.result()['coverage'])

    def test_06_failed_replacement(self):
        """Fault: failed replacement trusts stale observations or rewrites their digest/receipt."""
        original = self.row(); self.path.unlink(); self.ingest(); self.write([])
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('replacement fault')): self.ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual((self.row()['sha256'], self.row()['receipt']), (original['sha256'], original['receipt']))
        self.assertEqual((self.result()['status'], self.result()['summary']['events']), ('partial', 1))
        self.assertEqual(self.result()['coverage']['unavailable_primary_sources'], 1)
        self.assertIn({'source': str(self.path), 'code': 'source_import_failed:PermissionError'}, self.result()['coverage']['source_issues'])
        self.ingest(); self.source['exclude'] = ['a.*']; self.ingest(); self.source['exclude'] = []
        def vanished(*args):
            self.path.unlink()
            raise PermissionError('vanished replacement')
        with mock.patch('workflow_auditor.adapters.parse', side_effect=vanished): self.ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual(self.row()['collection_issues'], [{'code': 'source_import_failed:PermissionError'}])
        for retired in (False, True):
            with self.subTest(retired=retired):
                self.write(); self.ingest()
                if retired:
                    self.source['exclude'] = ['a.*']; self.ingest()
                    self.assertEqual(self.row()['status'], 'retired')
                    self.source['exclude'] = []
                extra = normalized(); extra['key'] = 'two'; self.write([normalized(), extra])
                with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('import fault')):
                    self.ingest()
                self.assertEqual((self.row()['status'], self.result()['status']), ('unavailable', 'partial'))
                self.path.unlink()
                for _ in range(3):
                    self.ingest()
                    self.assertEqual(self.row()['status'], 'unavailable')
                    result = self.result()
                    self.assertEqual((result['status'], result['summary']['events']), ('partial', 1))
                    self.assertEqual(result['coverage']['unavailable_primary_sources'], 1)
                    self.assertIn({'source': str(self.path), 'code': 'source_import_failed:PermissionError'},
                                  result['coverage']['source_issues'])
                self.write([normalized(), extra]); self.ingest()
                self.assertEqual((self.row()['status'], self.result()['summary']['events']), ('imported', 2))
                self.assertEqual(self.result()['status'], 'ready_with_missing_metrics')
                self.assertEqual(self.result()['coverage']['source_issues'], [])
                self.path.unlink(); self.ingest()
                self.assertEqual(self.row()['status'], 'rotated')

    def test_07_policy_roles(self):
        """Fault: invalid policy opens a database; policies or current Router participation are ignored."""
        config = self.root/'config.json'; db = self.root/'never.sqlite'
        self.source['rotation_policy'] = 'bad'; config.write_text(json.dumps(self.config))
        with redirect_stderr(io.StringIO()): self.assertEqual(main(['ingest', '--config', str(config), '--db', str(db)]), 2)
        self.assertFalse(db.exists())
        self.source.update(kind='router', rotation_policy='require_present', accounting_role='primary')
        self.write([{'requestId': 'q', 'model': 'm', 'inputTokens': 1, 'outputTokens': 1}]); self.ingest(); self.path.unlink(); self.ingest()
        self.assertEqual(self.store.config()['sources'][0]['rotation_policy'], 'require_present')
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual(self.result()['coverage']['import_state']['unavailable_sources'], 1)
        self.source['rotation_policy'] = 'retain'; self.source.pop('accounting_role'); self.ingest()
        self.assertEqual((self.row()['status'], self.row()['primary'], self.result()['summary']['events']), ('rotated', False, 0))
        self.assertEqual(self.result()['coverage']['rotated_sources']['primary_files'], 0)
        self.source['accounting_role'] = 'primary'; self.ingest()
        self.assertEqual((self.row()['primary'], self.result()['summary']['events']), (True, 1))

    def test_08_integrity(self):
        """Fault: corrupted counts/provenance/version or parser and collection issues gain retained trust."""
        self.path.unlink(); self.ingest(); original = self.row()
        from copy import deepcopy
        for fault in ('count', 'receipt', 'missing', 'version', 'provenance', 'parser', 'collection'):
            row = deepcopy(original)
            if fault == 'count': row['observations'] = row['receipt']['observations'] = 2
            if fault == 'receipt': row['receipt']['sha256'] = '0'*64
            if fault == 'missing': row.pop('receipt')
            if fault == 'version': row['receipt']['adapter_version'] = 0
            if fault == 'provenance': row['provenance_valid'] = False
            if fault == 'parser': row['issues'] = [{'code': 'invalid_json', 'line': 2}]
            if fault == 'collection': row['collection_issues'] = [{'code': 'known_path_stat_failed'}]
            self.save(row)
            if fault == 'parser': self.ingest()
            result = self.result()
            self.assertEqual(result['status'], 'partial', fault)
            if fault != 'parser': self.assertEqual(result['coverage']['unavailable_primary_sources'], 1)
            self.assertEqual(result['summary']['events'], 1)
            if fault in ('count', 'receipt', 'missing', 'version', 'provenance'):
                key = 'adapter_version_mismatches' if fault == 'version' else 'unverified_receipts'
                self.assertEqual(result['coverage']['rotated_sources'][key], 1)
            self.assertEqual(self.row().get('issues'), row.get('issues'))
            if fault in ('count', 'receipt', 'missing'):
                self.ingest()
                self.assertEqual(self.row()['status'], 'unavailable')
                self.assertEqual(self.result()['coverage']['rotated_sources']['unverified_receipts'], 1)

    def test_09_applicable_maps(self):
        """Fault N1: unrelated maps invalidate rotated history, or relevant maps silently reattribute it."""
        (self.root/'.git').mkdir(); self.source.update(kind='router', accounting_role='primary')
        self.write([{'requestId': 'q', 'model': 'm', 'cwd': str(self.root), 'agentId': 'native', 'inputTokens': 1, 'outputTokens': 1}])
        self.ingest(); self.path.unlink(); self.ingest(); original = self.store.events()[0][0].data()
        self.config.update(repo_aliases={'/unrelated': 'other'}, agent_roles={'other': 'scout'}, type_roles={'other': 'lead'})
        self.ingest(); self.assertTrue(self.row()['provenance_valid'])
        for field, value in [('repo_aliases', {str(self.root): 'changed'}), ('agent_roles', {'native': 'reviewer'})]:
            previous = self.config[field]; self.config[field] = value; self.ingest()
            self.assertFalse(self.row()['provenance_valid'])
            self.assertEqual(self.result()['coverage']['rotated_sources']['attribution_mismatches'], 1)
            self.assertEqual(self.result()['status'], 'partial')
            self.assertEqual(self.store.events()[0][0].data(), original)
            self.config[field] = previous; self.ingest(); self.assertTrue(self.row()['provenance_valid'])

    def test_10_legacy_and_placeholders(self):
        """Fault: legacy unavailable or imported rows and recurring null-digest failures become trusted/hidden."""
        self.assertFalse(captured_file(root_states(self.source)[0], {**self.row(), 'sha256': None}))
        for change in ({'status': 'retired'}, {'receipt': None}, {'receipt': {'roots': None}},
                       {'receipt': {'roots': [None]}}, {'receipt': {'roots': [{'type': 'file'}]}},
                       {'receipt': {'roots': [{'type': 'file', 'identity': str(self.path)}]}},
                       {'receipt': {'roots': [{'type': 'file', 'resolved': str(self.path)}]}},
                       {'receipt': {'roots': [{'type': 'directory', 'identity': str(self.path), 'resolved': str(self.path)}]}},
                       {'receipt': {'roots': [{'type': 'file', 'identity': str(self.path), 'resolved': str(self.root/'other')}]}}):
            self.assertFalse(captured_file(root_states(self.source)[0], {**self.row(), **change}))
        self.directory(); row = self.row(); row.pop('receipt'); row.pop('provenance_valid'); self.path.unlink()
        for status in ('imported', 'unavailable'):
            row['status'] = status; self.save(row); self.ingest()
            self.assertEqual(self.row()['status'], 'unavailable')
            self.assertEqual(self.result()['status'], 'partial')
        missing = self.root/'missing'; self.source['roots'] = [str(missing)]
        for _ in range(2):
            self.ingest(); self.ingest()
            self.assertIn({'source': str(missing), 'code': 'registered_root_missing'}, self.result()['coverage']['source_issues'])
            missing.mkdir(); keeper = missing/'keep.jsonl'; keeper.write_text(''); self.ingest(); keeper.unlink(); missing.rmdir()

    def test_11_labels_series(self):
        """Fault: retained label-filtered series omit history or invent activity in absence periods."""
        labels = self.root/'labels.json'; labels.write_text(json.dumps({'schema': 'workflow_auditor.labels.v1', 'labels': [
            {'name': 'task', 'since': '2026-10-20T00:00:00Z', 'until': '2026-10-21T00:00:00Z', 'agents': ['agent']}]}))
        selection = Selection(since=1792454400, until=1792627200, labels=read_labels(labels), label='task')
        before = report(self.store, selection)['summary']; self.path.unlink(); self.ingest()
        result = series(self.store, selection, every='day', tz='UTC')
        self.assertEqual(report(self.store, selection)['summary'], before)
        self.assertEqual([p['summary']['events'] for p in result['periods']], [1, 0])
        self.assertEqual([p['summary']['activity']['known_agent_hours'] for p in result['periods']], [1/6, 0])
        self.assertEqual(result['coverage']['rotated_sources']['observations'], 1)

    def test_12_unchanged_bytes(self):
        """Fault: healthy JSON/Markdown report/series bytes drift from the frozen post-label baseline."""
        self.assertEqual(serializations(self.root), [
            'd35a6b95c1bb0693e4c2f31526572cc4adad51cfdc5acd17d1522b2fe62045be',
            'ba03d8fb8f226bac0b96e4a1beb76fab3924fe1943937a9f64c3b704b7ced65a',
            'aaa61eaa28545485be52fca5ef81a6d5c4fe30ef36cd86df914789b0a1470582',
            'b5d7a5033d2e10ac601ad59599980642cf858732819000f515d0c0758fe87581',
            'e4682cec058f750e2cdb606cb7929c23e17c63663f66d61c93f8bce64619e031',
            '400350a083aae8e6198e27d93e07e2c9205360f808222afb8b41b5b64d55e47d',
            '21722c49e94384e5a6ac121c705d4e21dba6903bc928c7e2cdbb79dce941d78a',
            'f13cb1495090b43eaf6ab1ed2a8c2cd48a53bbae012541bcc95d14829d0e84c7'])

    def test_13_pattern_validation(self):
        """Fault: an unusable discovery pattern opens a database and leaves import running."""
        config = self.root/'config.json'; db = self.root/'never.sqlite'
        for pattern in ('/absolute/*.jsonl', 'bad**part/*.jsonl', 'bad\x00glob', 7, 0, False, [], {}, ''):
            self.source['pattern'] = pattern; config.write_text(json.dumps(self.config))
            with self.assertRaises(ValueError): validate_config(self.config)
            with redirect_stderr(io.StringIO()): self.assertEqual(main(['ingest', '--config', str(config), '--db', str(db)]), 2)
            self.assertFalse(db.exists())
        for pattern in (None, '*.jsonl', '**/*.jsonl'):
            self.source['pattern'] = pattern; validate_config(self.config)

    def test_14_normalized_identities(self):
        """Fault: raw 1a-1 receipt roots with dot-dot/trailing slash prove a nonexistent settings edit."""
        row = self.row(); row.pop('membership_settings'); self.source['pattern'] = 'never'
        row['receipt']['roots'][0]['identity'] = str(self.logs/'..'/'logs'/'a.jsonl')+'/'
        roots = root_states(self.source)
        for root in roots: root.update(type='unknown', health='error')
        self.assertFalse(exclusion_proven(str(self.path), self.source, roots, [], row))

    def test_15_imported_receipt_bytes(self):
        """Fault S1: receipt checks change present-file reports; hashes frozen from 83ba300 CLI bytes."""
        from contextlib import redirect_stdout
        from workflow_auditor.retention import ADAPTER_VERSIONS
        for kind, expected in (
                ('normalized', 'd35a6b95c1bb0693e4c2f31526572cc4adad51cfdc5acd17d1522b2fe62045be'),
                ('router', '5b4af0f79b78c476bcdafaef98327346846653aaa0418d18224ad2b36eb54250')):
            if kind == 'router':
                self.source.update(kind=kind, accounting_role='primary')
                self.write([{'requestId': 'q', 'model': 'm', 'provider': 3, 'inputTokens': 1, 'outputTokens': 1}])
                self.ingest(); self.assertFalse(self.row()['receipt']['replayable'])
            self.assertEqual(self.row()['status'], 'imported')
            with mock.patch.dict(ADAPTER_VERSIONS, {kind: 2}):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(main(['report', '--db', str(self.root/'test.sqlite')]), 0)
                encoded = output.getvalue().replace(str(self.root), '{root}').encode()
                self.assertEqual(hashlib.sha256(encoded).hexdigest(), expected)

    def test_16_unavailable_receipt(self):
        """Fault R2: limiting checks to rotated/tagged rows hides an unavailable receipt problem."""
        self.write([])
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('import fault')):
            self.ingest()
        row = self.row(); row['receipt']['adapter_version'] = 0; self.save(row)
        self.assertNotIn('retention', row)
        self.assertEqual(self.result()['coverage']['rotated_sources']['adapter_version_mismatches'], 1)
        self.assertIn({'source': str(self.path), 'code': 'adapter_version_mismatch'}, self.result()['coverage']['source_issues'])

    def test_17_explicit_file_parent(self):
        """Fault: a missing, unstatable or non-directory parent grants captured-file rotation."""
        moved = self.root/'moved'
        stat = Path.stat
        def check(status, issues, report_status, unavailable):
            for _ in range(2):
                self.ingest()
                self.assertEqual(self.row()['status'], status)
                self.assertEqual(self.row().get('collection_issues', []), issues)
                self.assertEqual(self.result()['status'], report_status)
                self.assertEqual(self.result()['coverage']['import_state']['unavailable_sources'], unavailable)
        for fault in ('missing', 'unstatable', 'file'):
            with self.subTest(fault=fault):
                try:
                    self.write(); self.ingest(); self.logs.rename(moved)
                    if fault == 'file': self.logs.write_text('not a directory')
                    def fail(path, *args, **kwargs):
                        if fault == 'unstatable' and path == self.logs: raise PermissionError('parent stat fault')
                        return stat(path, *args, **kwargs)
                    with mock.patch.object(Path, 'stat', fail):
                        check('unavailable', [{'code': 'registered_root_unavailable'}], 'partial', 1)
                    if fault == 'file': self.logs.unlink()
                    moved.rename(self.logs)
                    check('imported', [], 'ready_with_missing_metrics', 0)
                    self.path.unlink(); self.logs.rmdir()
                    check('unavailable', [{'code': 'registered_root_unavailable'}], 'partial', 1)
                    self.logs.mkdir()
                    check('rotated', [], 'ready_with_missing_metrics', 0)
                finally:
                    if self.logs.is_file(): self.logs.unlink()
                    if moved.exists(): moved.rename(self.logs)
                    self.logs.mkdir(exist_ok=True)
