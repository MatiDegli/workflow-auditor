from pathlib import Path
from unittest import mock
import json
import os
import tempfile
import unittest

from workflow_auditor.adapters import discover, ingest
from workflow_auditor.model import identifier
from workflow_auditor.query import Selection, report
from workflow_auditor.retention import glob_file, lexical, membership, root_states
from workflow_auditor.store import Store
from test_retention_receipts import normalized, serializations


class MembershipTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.logs = self.root/'logs.jsonl'; self.logs.mkdir()
        self.path = self.logs/'a.jsonl'
        self.path.write_text(json.dumps(normalized())+'\n')
        self.source = {'name': 'test', 'kind': 'normalized', 'roots': [str(self.logs)]}
        self.config = {'sources': [self.source]}
        self.store = Store(self.root/'test.sqlite')
        self.addCleanup(self.store.close)
        self.run_ingest()
        self.sid = self.row()['id']

    def run_ingest(self):
        ingest(self.store, self.config)

    def row(self):
        return next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.path))

    def edit(self, **changes):
        self.source.update(changes)
        self.run_ingest()
        return self.row()

    def save(self, row):
        detail = {k: v for k, v in row.items() if k not in ('id', 'path', 'sha256', 'registration', 'status')}
        with self.store.db:
            self.store.db.execute('UPDATE sources SET status=?,detail=? WHERE id=?',
                                  (row['status'], json.dumps(detail), row['id']))

    def excluded(self):
        return report(self.store, Selection())['coverage'].get('retained_history_excluded', [])

    def test_01_spellings(self):
        """Fault: relative, Windows and WSL root spellings lose lexical/source identity."""
        self.assertEqual(lexical(os.path.relpath(self.logs)), lexical(str(self.logs)))
        self.assertEqual(lexical(str(self.logs/'..'/'logs.jsonl')), lexical(str(self.logs)))
        roots = [{'identity': '/mnt/c/logs', 'resolved': '/mnt/c/logs', 'type': 'directory', 'health': 'missing'}]
        for path in ('C:\\Logs\\a.jsonl', '/mnt/c/Logs/a.jsonl'):
            self.assertEqual(lexical(path), '/mnt/c/logs/a.jsonl')
            self.assertEqual(len(membership(path, self.source, roots)[0]), 1)
        self.edit(roots=[str(self.logs/'..'/'logs.jsonl')])
        self.assertEqual((self.row()['id'], len(self.store.events()[0])), (self.sid, 1))
        self.assertEqual(self.row()['matching_roots'][0]['type'], 'directory')
        link = self.root/'link'; link.symlink_to(self.logs, target_is_directory=True)
        self.edit(roots=[str(link)])
        self.assertEqual(self.row()['matching_roots'][0]['identity'], str(link))
        self.path.unlink(); self.logs.rmdir()
        self.edit(roots=[str(self.logs)])
        self.assertEqual(self.row()['matching_roots'][0]['type'], 'directory')

    def test_02_globs(self):
        """Fault: lexical glob matching disagrees with discovery at root or nested depth."""
        nested = self.logs/'sub'; nested.mkdir()
        (nested/'b.jsonl').write_text('{}\n')
        for pattern, count in [('**/*.jsonl', 2), ('*.jsonl', 1), ('sub/*.jsonl', 1), ('sub/../*.jsonl', 1),
                               ('**/a.jsonl', 1), ('**/*.jsonl/', 0), ('**/*.jsonl/**', 0), ('**', 0), ('**/', 0), ('sub/**/b.jsonl', 1)]:
            self.source['pattern'] = pattern
            expected = sorted(str(p.resolve()) for p in self.logs.glob(pattern) if p.is_file())
            self.assertEqual(len(expected), count)
            self.assertEqual([str(p) for p in discover(self.source)[0]], expected)
            for path in (self.path, nested/'b.jsonl'):
                self.assertEqual(bool(membership(str(path), self.source, root_states(self.source))[0]), str(path) in expected)
        self.assertFalse(glob_file('deep/sub/a.jsonl', 'sub/*.jsonl'))
        self.assertFalse(glob_file('sub/a.jsonl/extra', 'sub/*.jsonl'))
        self.edit(pattern='sub/**/../*.jsonl')
        self.path.unlink(); self.run_ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.path.write_text(json.dumps(normalized())+'\n')
        foreign = self.root/'foreign.jsonl'; foreign.write_text(json.dumps(normalized())+'\n')
        (self.logs/'alias.jsonl').symlink_to(foreign)
        self.edit(pattern='**/*.jsonl')
        foreign.unlink(); self.run_ingest()
        row = next(s for s in self.store.sources() if s['path'] == str(foreign))
        self.assertEqual(row['matching_roots'][0]['relative'], 'alias.jsonl')
        self.source.update(kind='glm_report', pattern=None)
        (self.logs/'report.json').write_text('{}')
        self.assertEqual(discover(self.source)[0], [self.logs/'report.json'])
        self.assertTrue(membership(str(self.logs/'report.json'), self.source, root_states(self.source))[0])

    def test_03_explicit(self):
        """Fault: explicit files obey a directory pattern or bypass full-path/basename exclusions."""
        self.edit(roots=[str(self.path)], pattern='never')
        self.assertEqual(self.row()['status'], 'imported')
        self.assertEqual(self.row()['receipt']['roots'][0]['type'], 'file')
        self.assertTrue(membership(str(self.path), self.source, root_states(self.source))[0])
        plain = self.root/'plain'; plain.write_text('')
        self.assertEqual(root_states({**self.source, 'roots': [str(plain)]})[0]['type'], 'file')
        for excluded in ([self.path.name], [str(self.path)]):
            self.assertEqual(self.edit(exclude=excluded)['status'], 'retired')
            self.assertEqual(self.excluded()[0]['reason'], 'excluded_by_exclusion')
            self.assertEqual(self.edit(exclude=[])['status'], 'imported')
        self.path.unlink()
        self.run_ingest()
        self.assertEqual(self.row()['issues'], [])
        self.assertEqual(len(self.row()['collection_issues']), 1)
        self.edit(exclude=['a.*'])
        row = self.row(); row['receipt'] = None; self.save(row)
        self.edit(exclude=[])
        self.assertEqual(self.row()['status'], 'retired')

    def test_04_pattern_undo(self):
        """Fault: narrowing then undoing a pattern permanently drops absent history or parser issues."""
        original = self.row()['receipt']
        row = self.row(); row['issues'] = [{'code': 'invalid_json', 'line': 2}]; self.save(row)
        self.path.unlink()
        self.assertEqual(self.edit(pattern='other*')['status'], 'retired')
        self.assertEqual(self.row()['membership_exclusion']['prior_status'], 'imported')
        self.assertEqual(report(self.store, Selection())['summary']['events'], 0)
        self.assertEqual(self.excluded(), [{'registration': 'test', 'source_id': self.sid,
                                           'reason': 'excluded_by_pattern', 'observations': 1}])
        row = self.edit(pattern='**/*.jsonl')
        self.assertEqual((row['status'], row['receipt']), ('unavailable', original))
        self.assertEqual(row['issues'], [{'code': 'invalid_json', 'line': 2}])
        self.assertEqual(len(self.store.events()[0]), 1)
        self.assertEqual(self.excluded(), [])

    def test_05_roots_undo(self):
        """Fault: narrowing roots and restoring an absent root irreversibly retires its snapshots."""
        other = self.root/'other'; other.mkdir()
        self.path.unlink(); self.logs.rmdir()
        self.assertEqual(root_states(self.source)[0]['health'], 'missing')
        self.assertEqual(self.edit(roots=[str(other)])['status'], 'retired')
        self.assertEqual(self.excluded()[0]['reason'], 'excluded_by_roots')
        row = self.edit(roots=[str(self.logs)])
        self.assertEqual(row['status'], 'unavailable')
        self.assertEqual(row['matching_roots'][0]['type'], 'directory')
        self.assertEqual(row['collection_issues'], [{'code': 'registered_root_unavailable'}])

    def test_06_exclusion_undo(self):
        """Fault: removing an exclusion cannot restore absent history with an intact receipt."""
        self.path.unlink()
        self.assertEqual(self.edit(exclude=['a.*'])['status'], 'retired')
        self.assertEqual(self.edit(exclude=[])['status'], 'unavailable')
        self.assertEqual(len(self.store.events()[0]), 1)
        self.assertNotIn('membership_exclusion', self.row())

    def test_07_repeated_toggles(self):
        """Fault: repeated toggles duplicate observations, disclosures or transient failures."""
        second = self.logs/'b.jsonl'
        second.write_text(json.dumps({**normalized(), 'key': 'two'})+'\n'); self.run_ingest()
        self.path.unlink(); second.unlink()
        for _ in range(3):
            self.edit(exclude=['*.jsonl']); first = self.excluded()
            self.assertEqual(first, sorted(first, key=lambda s: (s['registration'], s['source_id'])))
            self.run_ingest(); self.assertEqual(self.excluded(), first)
            self.assertEqual(self.row()['membership_exclusion']['prior_status'], 'imported' if _ == 0 else 'unavailable')
            self.edit(exclude=[]); self.run_ingest()
            self.assertEqual(self.row()['collection_issues'], [{'code': 'source_disappeared_after_snapshot'}])
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0], 2)
            self.assertEqual(self.row()['issues'], [])

    def test_08_invalid_and_legacy(self):
        """Fault: corrupt receipts/counts or unmarked legacy retirement revive without evidence."""
        self.path.unlink()
        self.edit(exclude=['a.*']); original = self.row()
        for fault in ('receipt', 'count', 'null', 'roots_list', 'roots_item', 'roots_shape', 'legacy', 'old_legacy'):
            row = json.loads(json.dumps(original))
            if fault == 'receipt': row['receipt']['sha256'] = None
            if fault == 'count': row['observations'] = row['receipt']['observations'] = 2
            if fault in ('null', 'roots_list', 'roots_item', 'roots_shape'): row.pop('matching_roots')
            if fault == 'null': row['receipt'] = None
            if fault.startswith('roots_'): row['receipt']['roots'] = {'roots_list': None, 'roots_item': [1], 'roots_shape': [{}]}[fault]
            if fault in ('legacy', 'old_legacy'): row.pop('membership_exclusion')
            if fault == 'old_legacy': row.pop('receipt')
            self.save(row)
            if fault == 'legacy': self.edit(exclude=['a.*'])
            self.edit(exclude=[])
            self.assertEqual(self.row()['status'], 'retired', fault)
            self.assertEqual(self.excluded()[0]['observations'], 1)
            if fault in ('legacy', 'old_legacy'): self.assertEqual(self.excluded()[0]['reason'], 'unknown_legacy_retirement')
        self.store.failed_source('sentinel', 'test', '', 'no_matching_files')
        with self.store.db: self.store.db.execute("UPDATE sources SET status='retired' WHERE id='sentinel'")
        self.assertEqual(len(self.excluded()), 1)

    def test_09_root_failures(self):
        """Fault: root stat/enumeration failure propagates to another root or escapes ingest."""
        other = self.root/'other'; other.mkdir()
        self.source['roots'].append(str(other))
        bad = other/'b.jsonl'; bad.write_text(json.dumps({**normalized(), 'key': 'two'})+'\n')
        self.run_ingest(); bad.unlink()
        stat, iterdir = Path.stat, Path.iterdir
        for method, original in [('stat', stat), ('iterdir', iterdir)]:
            def fail(path, *args, **kwargs):
                if path == other: raise PermissionError('injected root fault')
                return original(path, *args, **kwargs)
            with mock.patch.object(Path, method, fail):
                if method == 'stat': self.assertEqual(root_states(self.source)[1]['health'], 'error')
                self.run_ingest()
            self.assertEqual(self.row()['status'], 'imported')
            failed = next(s for s in self.store.sources() if s['path'] == str(bad))
            self.assertEqual(failed['matching_roots'][0]['type'], 'unknown')
            self.assertEqual(failed['collection_issues'], [{'code': 'registered_root_unavailable'}])
        with mock.patch('workflow_auditor.adapters.discover', side_effect=OSError('escaped')):
            self.run_ingest()
        self.assertEqual(self.row()['collection_issues'], [{'code': 'registered_root_unavailable'}])
        self.assertEqual(self.row()['matching_roots'][0]['type'], 'unknown')
        self.config['sources'].append({**self.source, 'name': 'second', 'roots': [str(other)]})
        self.run_ingest()
        self.assertEqual(self.row()['status'], 'imported')
        glob = Path.glob
        def partial(base, pattern):
            if base == self.logs:
                yield self.path
                raise OSError('enumeration failed after a match')
            yield from glob(base, pattern)
        with mock.patch.object(Path, 'glob', partial): self.run_ingest()
        self.assertEqual(self.row()['matching_roots'][0]['health'], 'error')
        self.assertEqual(self.row()['matching_roots'][0]['type'], 'unknown')

    def test_10_overlap(self):
        """Fault: an unhealthy overlapping root blocks membership through a healthy root."""
        self.edit(roots=[str(self.logs), str(self.root)])
        self.path.unlink()
        roots = root_states(self.source); roots[0]['health'] = 'error'
        with mock.patch('workflow_auditor.adapters.discover', return_value=([], roots, {})): self.run_ingest()
        self.assertEqual(self.row()['collection_issues'], [{'code': 'source_disappeared_after_snapshot'}])
        self.assertEqual({r['health'] for r in self.row()['matching_roots']}, {'error', 'healthy'})

    def test_11_known_paths(self):
        """Fault: discovery omission or known-path stat error is misclassified as confirmed absence."""
        original = Path.stat
        roots = root_states(self.source)
        with mock.patch('workflow_auditor.adapters.discover', return_value=([], roots, {})):
            self.run_ingest()
            self.assertEqual(self.row()['collection_issues'], [{'code': 'source_omitted_by_discovery'}])
            def fail(path, *args, **kwargs):
                if path == self.path: raise PermissionError('known path fault')
                return original(path, *args, **kwargs)
            with mock.patch.object(Path, 'stat', fail): self.run_ingest()
        self.assertEqual(self.row()['collection_issues'], [{'code': 'known_path_stat_failed'}])
        view = report(self.store, Selection())
        self.assertIn({'source': str(self.path), 'code': 'known_path_stat_failed'}, view['coverage']['source_issues'])
        self.run_ingest()
        self.assertNotIn('collection_issues', self.row())

    def test_12_unchanged_bytes(self):
        """Fault: unaffected report/series JSON/Markdown bytes or label grouping drift."""
        expected = ['d35a6b95c1bb0693e4c2f31526572cc4adad51cfdc5acd17d1522b2fe62045be',
                    'ba03d8fb8f226bac0b96e4a1beb76fab3924fe1943937a9f64c3b704b7ced65a',
                    'aaa61eaa28545485be52fca5ef81a6d5c4fe30ef36cd86df914789b0a1470582',
                    'b5d7a5033d2e10ac601ad59599980642cf858732819000f515d0c0758fe87581',
                    'e4682cec058f750e2cdb606cb7929c23e17c63663f66d61c93f8bce64619e031',
                    '400350a083aae8e6198e27d93e07e2c9205360f808222afb8b41b5b64d55e47d',
                    '21722c49e94384e5a6ac121c705d4e21dba6903bc928c7e2cdbb79dce941d78a',
                    'f13cb1495090b43eaf6ab1ed2a8c2cd48a53bbae012541bcc95d14829d0e84c7']
        self.assertEqual(serializations(self.root), expected)

    def test_13_failed_retired_import(self):
        self.edit(exclude=['a.*']); original = self.row()
        for fault in ('receipt', 'count', 'legacy', 'valid'):
            with self.subTest(fault=fault):
                row = json.loads(json.dumps(original))
                if fault == 'receipt': row['receipt']['sha256'] = None
                if fault == 'count': row['observations'] = row['receipt']['observations'] = 2
                if fault == 'legacy': row.pop('membership_exclusion')
                self.save(row)
                with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('injected')):
                    self.edit(exclude=[])
                row = self.row()
                self.assertEqual(row['status'], 'unavailable' if fault == 'valid' else 'retired')
                self.assertEqual(row['issues'], [])
                self.assertIn({'code': 'source_import_failed:PermissionError'}, row['collection_issues'])
                self.assertNotIn({'source': str(self.path), 'code': 'source_omitted_by_discovery'},
                                 report(self.store, Selection())['coverage']['source_issues'])
                self.assertEqual(report(self.store, Selection())['summary']['events'], int(fault == 'valid'))
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)
                if fault in ('receipt', 'count'):
                    self.assertEqual(self.excluded()[0].get('restore_refused'), 'invalid_receipt' if fault == 'receipt' else 'count_mismatch')

    def test_14_old_collection_failures(self):
        self.edit(roots=[str(self.path)])
        row = self.row()
        for key in ('receipt', 'matching_roots', 'membership_settings'): row.pop(key)
        self.save(row)
        stat = Path.stat
        def fail(path, *args, **kwargs):
            if path == self.path: raise PermissionError('explicit root stat failed')
            return stat(path, *args, **kwargs)
        with mock.patch.object(Path, 'stat', fail):
            self.assertEqual(root_states(self.source)[0]['type'], 'unknown')
            self.run_ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual(self.row()['collection_issues'], [{'code': 'registered_root_unavailable'}])
        # A pre-1a-2 receipt knows the root, but not the escaping symlink's relative path.
        foreign = self.root/'foreign.jsonl'; self.path.rename(foreign)
        self.path.symlink_to(foreign)
        self.edit(roots=[str(self.logs)])
        row = next(s for s in self.store.sources() if s['path'] == str(foreign))
        row.pop('matching_roots'); row.pop('membership_settings'); self.save(row)
        with mock.patch('workflow_auditor.adapters.discover', return_value=([], root_states(self.source), {})):
            for _ in range(2): self.run_ingest()
        row = next(s for s in self.store.sources() if s['path'] == str(foreign))
        self.assertEqual(row['status'], 'unavailable')
        self.assertEqual(row['collection_issues'], [{'code': 'membership_unverifiable'}])

    def test_15_reason_and_refusal(self):
        self.path.unlink()
        self.edit(pattern='other*')
        self.edit(pattern='**/*.jsonl', exclude=['a.*'])
        self.assertEqual(self.excluded()[0]['reason'], 'excluded_by_exclusion')
        self.assertEqual(self.row()['membership_exclusion']['prior_status'], 'imported')
        row = self.row(); row['receipt']['sha256'] = None; self.save(row)
        self.edit(exclude=[])
        self.assertEqual(self.excluded()[0].get('restore_refused'), 'invalid_receipt')
        self.assertEqual(self.excluded()[0]['reason'], 'selected')
        self.edit(exclude=['a.*'])
        self.assertNotIn('restore_refused', self.excluded()[0])

    def test_16_boundaries_and_disclosure(self):
        sibling = self.root/'logs.jsonl2'; sibling.mkdir()
        second = next(sibling/f'b{i}.jsonl' for i in range(1000)
                      if identifier('alpha', str(sibling/f'b{i}.jsonl')) > identifier('test', str(sibling/f'b{i}.jsonl')))
        second.write_text(json.dumps({**normalized(), 'key': 'two'})+'\n')
        self.source['roots'].append(str(sibling))
        self.config['sources'].append({**self.source, 'name': 'alpha', 'roots': [str(sibling)]})
        self.run_ingest()
        row = next(s for s in self.store.sources() if s['registration'] == 'test' and s['path'] == str(second))
        row.pop('membership_settings'); self.save(row)  # Root-set evidence must also work from receipts alone.
        second.unlink(); self.path.unlink()
        self.source['roots'] = [str(self.logs)]
        self.config['sources'][1]['exclude'] = ['*.jsonl']
        self.run_ingest()
        entries = self.excluded()
        self.assertEqual(len(entries), 2)
        self.assertEqual([s['registration'] for s in entries], ['alpha', 'test'])
        self.assertEqual(entries[1]['reason'], 'excluded_by_roots')
        self.config['sources'][1]['accounting_role'] = 'corroborating'
        # Accounting role is refreshed only for participating rows; seed a captured corroborator.
        row = next(s for s in self.store.sources(include_retired=True) if s['registration'] == 'alpha' and s['sha256'])
        row['primary'] = False; self.save(row)
        self.assertEqual(len(self.excluded()), 1)

    def test_17_old_symlink_root(self):
        link = self.root/'link'; link.symlink_to(self.logs, target_is_directory=True)
        self.edit(roots=[str(link)])
        row = self.row(); row.pop('matching_roots'); row.pop('membership_settings'); self.save(row)
        self.path.unlink(); self.logs.rmdir()
        self.run_ingest()
        self.assertEqual(self.row()['status'], 'unavailable')
        self.assertEqual(len(self.row()['matching_roots']), 1)
        self.assertEqual(self.row()['matching_roots'][0]['relative'], 'a.jsonl')
        self.assertEqual(self.row()['collection_issues'], [{'code': 'registered_root_unavailable'}])

    def test_18_placeholder_issues_and_other_receipt(self):
        from workflow_auditor.retention import receipt_matches
        for kind in ('other', 'unknown'):
            row = self.row(); row['receipt']['roots'][0]['type'] = kind
            self.assertEqual(receipt_matches(row, self.source, self.config), (True, None))
        self.path.unlink(); self.logs.rmdir()
        stat = Path.stat
        def fail(path, *args, **kwargs):
            if path == self.logs: raise PermissionError('root unreadable')
            return stat(path, *args, **kwargs)
        for context, code in ((None, 'registered_root_missing'), (mock.patch.object(Path, 'stat', fail), 'registered_root_unreadable')):
            from contextlib import nullcontext
            with context or nullcontext():
                for _ in range(2):
                    self.run_ingest()
                    placeholder = next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.logs))
                    placeholder['issues'][0]['fixture'] = 'kept'; self.save(placeholder)
            placeholder = next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.logs))
            self.assertEqual(placeholder['status'], 'unavailable')
            self.assertEqual(sum(i['code'] == code for i in placeholder['issues']), 1)
            self.assertNotIn('no_matching_files', [i['code'] for s in self.store.sources() for i in s.get('issues', [])])

    def test_19_exclusion_evidence(self):
        # Saved settings prove an edit even when the root cannot establish lexical exclusion.
        self.path.unlink(); self.logs.rmdir()
        self.assertEqual(self.edit(pattern='other*')['status'], 'retired')
        self.assertEqual(self.edit(pattern='**/*.jsonl')['status'], 'unavailable')
        self.assertEqual(self.edit(exclude=['a.*'])['status'], 'retired')
        self.edit(exclude=[])
        # Older receipts can still prove an exclusion under a healthy root of known type.
        self.logs.mkdir(); self.path.write_text(json.dumps(normalized())+'\n'); self.run_ingest()
        row = self.row(); row.pop('membership_settings'); self.save(row)
        previous = row['matching_roots']
        self.path.unlink(); self.logs.rmdir()
        self.assertEqual(self.edit(pattern='other*')['status'], 'unavailable')
        self.assertEqual(self.row()['matching_roots'], previous)
        self.source['pattern'] = '**/*.jsonl'
        self.logs.mkdir(); self.path.write_text(json.dumps(normalized())+'\n'); self.run_ingest()
        row = self.row(); row.pop('matching_roots'); row.pop('membership_settings'); self.save(row)
        self.assertEqual(self.edit(pattern='other*')['status'], 'retired')

    def test_20_nonretired_import_failure(self):
        original = self.row()
        with mock.patch('workflow_auditor.adapters.parse', side_effect=PermissionError('injected')):
            for _ in range(2): self.run_ingest()
        row = self.row()
        self.assertEqual(row['status'], 'unavailable')
        self.assertEqual(row['receipt'], original['receipt'])
        self.assertEqual(row['issues'], [{'code': 'source_import_failed:PermissionError'}] * 2)
        self.assertNotIn('collection_issues', row)
        self.assertEqual(len(self.store.events()[0]), 1)

    def test_21_recurring_missing_root(self):
        for _ in range(2):
            self.path.unlink(); self.logs.rmdir(); self.run_ingest()
            view = report(self.store, Selection())
            self.assertEqual(view['summary']['events'], 1)
            self.assertEqual(view['coverage']['unavailable_primary_sources'], 2)
            self.assertIn({'source': str(self.logs), 'code': 'registered_root_missing'}, view['coverage']['source_issues'])
            self.logs.mkdir(); self.path.write_text(json.dumps(normalized())+'\n'); self.run_ingest()
            placeholder = next(s for s in self.store.sources(include_retired=True) if s['path'] == str(self.logs))
            self.assertEqual((placeholder['status'], placeholder['sha256']), ('retired', None))

    def test_22_recurring_no_matches(self):
        for _ in range(2):
            self.edit(pattern='never')
            view = report(self.store, Selection())
            self.assertEqual(view['summary']['events'], 0)
            self.assertEqual(view['coverage']['unavailable_primary_sources'], 1)
            self.assertIn({'source': '', 'code': 'no_matching_files'}, view['coverage']['source_issues'])
            self.edit(pattern='**/*.jsonl')
            self.assertEqual(report(self.store, Selection())['summary']['events'], 1)
            placeholder = next(s for s in self.store.sources(include_retired=True) if s['path'] == '')
            self.assertEqual((placeholder['status'], placeholder['sha256']), ('retired', None))

    def test_23_recurring_first_import_failure(self):
        from workflow_auditor.adapters import parse
        fresh = self.logs/'b.jsonl'; fresh.write_text(json.dumps({**normalized(), 'key': 'two'})+'\n')
        def fail(path, *args):
            if path == fresh: raise PermissionError('injected first import failure')
            return parse(path, *args)
        with mock.patch('workflow_auditor.adapters.parse', side_effect=fail):
            self.run_ingest()
            self.edit(exclude=['b.*'])
            row = next(s for s in self.store.sources(include_retired=True) if s['path'] == str(fresh))
            self.assertEqual((row['status'], row['sha256']), ('retired', None))
            self.edit(exclude=[])
        view = report(self.store, Selection())
        self.assertEqual(view['summary']['events'], 1)
        self.assertEqual(view['coverage']['unavailable_primary_sources'], 1)
        self.assertIn({'source': str(fresh), 'code': 'source_import_failed:PermissionError'}, view['coverage']['source_issues'])
        row = next(s for s in self.store.sources() if s['path'] == str(fresh))
        self.assertEqual((row['status'], row['sha256']), ('unavailable', None))
