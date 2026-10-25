from contextlib import redirect_stdout
from copy import deepcopy
from pathlib import Path
from unittest import mock
import hashlib
import io
import json
import tempfile
import unittest

from workflow_auditor.adapters import ingest, parse
from workflow_auditor.cli import main
from workflow_auditor.model import Event, Tokens
from workflow_auditor.store import Store


# Frozen from 6e88c7f: run its adapters.parse once on each synthetic compare_main
# case; save Event.data(), issues and the compared attribution fields. Only the
# generated temporary root is replaced by {root}; no parser computes expectations.
MAIN_EVENT = {
    'key': 'f590a3cb09dfbf27bcc9384efb4976d135433601b27b5ad22c00082996bebda5',
    'client': 'claude_code', 'provider': 'anthropic', 'provider_basis': 'model_family_inference',
    'model': 'claude-test', 'repo': '{root}/repo', 'run': 'run', 'agent': 'claude:run:child',
    'parent': 'claude:run:main', 'role': 'unknown', 'role_basis': 'unclassified',
    'tokens': {'input_total': 10, 'cache_read': 0, 'cache_write': 0, 'output_total': 2,
               'reasoning': None, 'write_5m': None, 'write_1h': None},
    'time_kind': 'point', 'at': 1792490400.0, 'start': None, 'end': None,
    'activity_start': None, 'activity_end': None, 'activity_basis': None,
    'cli_usd': None, 'speed': 'unknown', 'geo': 'unknown', 'unit': 'response',
}
MAIN_ATTRIBUTION = {
    'repo': '{root}/repo', 'provider': 'anthropic', 'provider_basis': 'model_family_inference',
    'agent': 'claude:run:child', 'role': 'unknown', 'role_basis': 'unclassified',
}
# Each tuple contains events, issues and attribution, including empty/error cases.
MAIN_EXPECTED = {
    'test_10_corrupt_receipts': (
        [{**MAIN_EVENT, 'agent': "claude:run:{'body': 'SECRET'}"}], [],
        {**MAIN_ATTRIBUTION, 'agent': "claude:run:{'body': 'SECRET'}"}),
    'test_13_nonstring_agent_type': (
        [MAIN_EVENT, {**MAIN_EVENT,
                      'key': 'f600420fd3c05ce5c18f4e13edd1ce83d315ec11196b193a7555878c22efe0fa'}],
        [], MAIN_ATTRIBUTION),
    'test_14_numeric_agent_id': (
        [{**MAIN_EVENT, 'agent': 'claude:run:42'}], [],
        {**MAIN_ATTRIBUTION, 'agent': 'claude:run:42'}),
    'test_15_excluded_bad_cwd': ([], [], None),
    'test_16_override_ignores_bad_cwd': (
        [{**MAIN_EVENT, 'repo': 'fixed'}], [], {**MAIN_ATTRIBUTION, 'repo': 'fixed'}),
    'test_17_override_ignores_bad_provider': (
        [{**MAIN_EVENT, 'key': '4388c05a62a09a2c3281ba0f7d3cd361f0eb7c0886d1e31d4a0baaeabe4f8121',
          'client': 'codex', 'provider': 'fixed', 'provider_basis': 'registration',
          'model': 'unknown', 'repo': 'unattributed', 'agent': 'codex:run', 'parent': None,
          'role': 'lead', 'role_basis': 'structural_root', 'at': None, 'unit': 'counter_delta',
          'tokens': {'input_total': 10, 'cache_read': None, 'cache_write': None, 'output_total': 2,
                     'reasoning': None, 'write_5m': None, 'write_1h': None}}], [],
        {'repo': 'unattributed', 'provider': 'fixed', 'provider_basis': 'registration',
         'agent': 'codex:run', 'role': 'lead', 'role_basis': 'structural_root'}),
    # All three faults (empty_alias, null_role, numeric_provider) had this result.
    'test_18_import_checks_receipt': (
        [], [{'code': 'invalid_accounting_record', 'line': 1, 'exception': 'ValueError'}], None),
    'test_19_raw_root_cwd': (
        [{**MAIN_EVENT, 'repo': 'root-alias'}], [], {**MAIN_ATTRIBUTION, 'repo': 'root-alias'}),
}


def normalized():
    return Event('one', 'fixture', 'test', 'fixture', 'test', 'repo', 'run', 'agent', None,
                 'reviewer', 'fixture', Tokens(10, 0, 0, 2), 'point', at=1792490400).data()


def serializations(root):
    path = root/'normalized.jsonl'
    path.write_text(json.dumps(normalized())+'\n')
    db = root/'baseline.sqlite'
    store = Store(db)
    ingest(store, {'sources': [{'name': 'test', 'kind': 'normalized', 'roots': [str(path)]}]})
    store.close()
    labels = Path('examples/history/labels.json')
    outputs = []
    for command in ('report', 'series'):
        for enabled in (False, True):
            for fmt in ('json', 'markdown'):
                args = [command, '--db', str(db), '--format', fmt]
                if enabled:
                    args += ['--labels', str(labels), '--group-by', 'label,provider']
                if command == 'series':
                    args += ['--since', '2026-10-20T00:00:00Z', '--until', '2026-10-21T00:00:00Z', '--every', 'day']
                out = io.StringIO()
                with redirect_stdout(out):
                    assert main(args) == 0
                outputs.append(hashlib.sha256(out.getvalue().encode()).hexdigest())
    return outputs


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root/'test.sqlite')
        self.cwd = self.root/'repo'
        self.cwd.mkdir()
        (self.cwd/'.git').mkdir()
        self.source = {'name': 'test', 'kind': 'claude', 'roots': [str(self.root/'agent-child.jsonl')]}
        self.config = {'sources': [self.source]}
        self.row = {'type': 'assistant', 'sessionId': 'run', 'agentId': 'child', 'cwd': str(self.cwd),
                    'timestamp': '2026-10-20T10:00:00Z', 'message': {'id': 'response', 'model': 'claude-test',
                    'usage': {'input_tokens': 10, 'output_tokens': 2}, 'content': 'SECRET_BODY'}}

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def load(self, rows=None):
        self.path = Path(self.source['roots'][0])
        rows = [self.row] if rows is None else rows
        self.path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        ingest(self.store, self.config)
        self.snapshot = self.store.sources()[0]
        return self.snapshot['receipt']

    def matches(self, snapshot=None):
        from workflow_auditor.retention import receipt_matches
        return receipt_matches(self.snapshot if snapshot is None else snapshot, self.source, self.config)[0]

    def test_01_unrelated_alias(self):
        """Fault: hashing the whole alias map invalidates unrelated snapshots."""
        receipt = self.load()
        self.config['repo_aliases'] = {str(self.root/'other'): 'other'}
        self.assertTrue(self.matches())
        self.assertNotIn('SECRET', json.dumps(receipt))
        self.assertEqual(receipt['roots'], [{'identity': str(self.path), 'resolved': str(self.path), 'type': 'file'}])
        self.assertEqual(self.store.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0], '1')

    def test_02_unrelated_roles(self):
        """Fault: unrelated agent/type overrides invalidate applicable role evidence."""
        self.load()
        self.config.update(agent_roles={'elsewhere': 'reviewer'}, type_roles={'other-type': 'scout'},
                           prices={'unused': 2}, labels='ignored')
        self.source.update(accounting_role='corroborating', rotation_policy='require_present')
        self.assertTrue(self.matches())

    def test_03_applicable_results(self):
        """Fault: applicable alias, agent-role or native-type changes leave stale attribution valid."""
        self.path = Path(self.source['roots'][0])
        self.path.with_suffix('.meta.json').write_text('{"agentType":"reviewer"}')
        self.load()
        for key, value in [('repo_aliases', {str(self.cwd): 'changed'}),
                           ('agent_roles', {'claude:run:child': 'scout'}), ('type_roles', {'reviewer': 'scout'})]:
            with self.subTest(key=key):
                self.config[key] = value
                self.assertFalse(self.matches())
                del self.config[key]
                self.assertTrue(self.matches())

    def test_04_precedence_and_basis(self):
        """Fault: replay disagrees with longest-prefix, tied-alias order or attribution bases."""
        self.config['repo_aliases'] = {str(self.root): 'broad', str(self.cwd): 'specific', str(self.cwd)+'/': 'tie'}
        receipt = self.load()
        self.assertEqual(receipt['attribution'][0]['result']['repo'], 'specific')
        self.config['repo_aliases'] = {str(self.cwd)+'/': 'tie', str(self.cwd): 'specific'}
        self.assertFalse(self.matches())
        self.config['repo_aliases'] = {str(self.cwd): 'specific'}
        self.source['provider'] = 'anthropic'
        self.assertFalse(self.matches())
        del self.source['provider']
        self.source['role'] = 'unknown'
        self.assertFalse(self.matches())

    def test_05_native_exclusions(self):
        """Fault: excluding a native agent discards the evidence needed to detect later inclusion."""
        self.source['exclude_agents'] = ['child']
        receipt = self.load()
        self.assertEqual(receipt['observations'], 0)
        self.assertEqual(len(receipt['attribution']), 1)
        self.assertFalse(receipt['attribution'][0]['result']['included'])
        self.assertTrue(self.matches())
        self.source['exclude_agents'] = []
        self.assertFalse(self.matches())
        self.path.unlink()
        ingest(self.store, self.config)
        self.assertFalse(self.store.sources()[0]['provenance_valid'])
        self.source['exclude_agents'] = ['child']
        ingest(self.store, self.config)
        self.assertTrue(self.store.sources()[0]['provenance_valid'])

    def test_06_router_inputs(self):
        """Fault: Router replay uses a full/overridden agent instead of its native fallback, or loses provider."""
        self.source.update(kind='router', agent='override')
        self.config['agent_roles'] = {'override': 'reviewer', 'native': 'scout'}
        receipt = self.load([{'requestId': 'request', 'model': 'glm-test', 'provider': 'zai-coding',
                              'agentId': 'native', 'threadId': 'thread', 'cwd': str(self.cwd),
                              'inputTokens': 10, 'outputTokens': 2}])
        self.assertEqual(receipt['attribution'][0]['result']['provider'], 'z.ai')
        self.assertEqual(receipt['attribution'][0]['inputs']['native_agent'], 'native')
        result = receipt['attribution'][0]['result']
        self.assertEqual((result['agent'], result['role'], result['provider_basis']), ('override', 'reviewer', 'client_recorded'))
        del self.source['agent']
        self.assertFalse(self.matches())
        self.source['agent'] = 'override'
        self.source['provider'] = 'z.ai'
        self.assertFalse(self.matches())

    def test_07_stream_controls_and_kind(self):
        """Fault: stream bounds or adapter-kind edits reuse incompatible parsing evidence."""
        self.source.update(kind='claude_stream', since='2026-10-20T10:00:00Z', until='2026-10-20T11:00:00Z')
        self.load([{'type': 'result', 'session_id': 'run', 'modelUsage': {'claude-test': {'outputTokens': 2}}}])
        self.assertTrue(self.matches())
        for field, changed in [('since', '2026-10-20T09:00:00Z'), ('until', None)]:
            old = self.source[field]
            self.source[field] = changed
            self.assertFalse(self.matches())
            self.source[field] = old
        self.source.update(kind='codex')
        self.load([])
        self.source['kind'] = 'claude'  # Equal controls: only the kind guard can reject this.
        self.assertFalse(self.matches())

    def test_08_normalized_explicit(self):
        """Fault: registration overrides invent new attribution for normalized events."""
        self.source['kind'] = 'normalized'
        self.load([normalized()])
        self.source.update(repo='changed', provider='other', role='scout', agent='other', exclude_agents=['agent'])
        self.config.update(agent_roles={'agent': 'lead'}, repo_aliases={str(self.cwd): 'new'})
        self.assertTrue(self.matches())
        self.assertEqual(parse(self.path, self.source, self.config).events[0].data(), normalized())

    def test_09_final_counts_and_siblings(self):
        """Fault: pre-reconciliation counts reject zero files or locally merged copies; sibling imports lack receipts."""
        self.assertEqual(self.load([])['observations'], 0)
        self.assertTrue(self.matches())
        receipt = self.load([self.row, self.row])
        self.assertEqual(len(receipt['attribution']), 1)
        self.assertEqual(receipt['observations'], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)
        for kind, path in [('codex', 'codex/rollout-2026-10-20T11-00-00-example.jsonl'),
                           ('glm_report', 'glm/attempt-1/report.json')]:
            self.source.update(kind=kind, roots=[str(Path('examples/history')/path)])
            ingest(self.store, self.config)
            snapshot = next(s for s in self.store.sources() if s['status'] == 'imported')
            self.assertTrue(self.matches(snapshot))
            self.assertGreater(snapshot['receipt']['observations'], 0)

    def test_10_corrupt_receipts(self):
        """Fault: malformed, legacy, mismatched digest/count/schema or stale adapter receipts prove eligibility."""
        self.load()
        self.assertTrue(self.matches())
        changes = {'version': True, 'sha256': '0'*64, 'kind': 'router', 'adapter_version': 0, 'observations': 2,
                   'roots': [{}], 'controls': {'unexpected': 1}, 'attribution': [{}]}
        for key, value in changes.items():
            bad = deepcopy(self.snapshot)
            bad['receipt'][key] = value
            with self.subTest(key=key):
                self.assertFalse(self.matches(bad))
        for target, value in [('sha256', None), ('receipt', None),
                              ('roots', [['identity', 'resolved', 'type']])]:
            bad = deepcopy(self.snapshot)
            if target == 'receipt':
                bad['receipt'] = value
            else:
                bad['receipt'][target] = value
            self.assertFalse(self.matches(bad))
            self.store.db.execute('UPDATE sources SET detail=?', (json.dumps(bad),))
            self.store.replay_receipts(self.source, self.config)
            self.assertFalse(self.store.sources()[0]['provenance_valid'])
            self.assertEqual(self.store.sources()[0]['provenance_reason'], 'invalid_receipt')
        for target in ('receipt', 'inputs', 'result', 'pair'):
            bad = deepcopy(self.snapshot)
            obj = bad['receipt'] if target == 'receipt' else (bad['receipt']['attribution'][0] if target == 'pair' else bad['receipt']['attribution'][0][target])
            obj['unexpected'] = 'not allowed'
            self.assertFalse(self.matches(bad))
        from workflow_auditor.retention import ADAPTER_VERSIONS
        for kind, valid in [('codex', True), ('claude', False)]:
            with mock.patch.dict(ADAPTER_VERSIONS, {kind: 2}):
                self.assertEqual(self.matches(), valid)
        for field, values in {'root': [None, 1], 'cwd': [{}], 'model': [None], 'fallback': [None],
                              'explicit': [{**self.snapshot['receipt']['attribution'][0]['result']} ]}.items():
            for value in values:
                bad = deepcopy(self.snapshot)
                bad['receipt']['attribution'][0]['inputs'][field] = value
                self.assertFalse(self.matches(bad))
        for field, value in [('included', 1), ('role', None), ('agent', {})]:
            bad = deepcopy(self.snapshot)
            bad['receipt']['attribution'][0]['result'][field] = value
            self.assertFalse(self.matches(bad))
        for value in ([], [self.snapshot['receipt']['attribution'][0]]*2, None):
            bad = deepcopy(self.snapshot)
            bad['receipt']['attribution'] = value
            self.assertFalse(self.matches(bad))
        bad = deepcopy(self.snapshot)
        del bad['receipt']
        self.assertFalse(self.matches(bad))
        malformed = deepcopy(self.row)
        malformed['agentId'] = {'body': 'SECRET'}
        self.compare_main([malformed])
        self.assertNotIn('SECRET', json.dumps(self.store.sources()))
        self.assertEqual(self.snapshot['provenance_reason'], 'unreplayable_input')

    def test_11_vanished_metadata(self):
        """Fault: replay scans a vanished cwd/sidecar instead of saved Git fallback and native type."""
        path = Path(self.source['roots'][0]).with_suffix('.meta.json')
        path.write_text('{"agentType":"reviewer","description":"SECRET"}')
        self.config['repo_aliases'] = {str(self.cwd): str(self.cwd)}
        receipt = self.load()
        path.unlink()
        (self.cwd/'.git').rmdir()
        self.cwd.rmdir()
        del self.config['repo_aliases']
        with mock.patch('pathlib.Path.is_dir', side_effect=AssertionError('live cwd read')):
            self.assertTrue(self.matches())
        self.assertEqual(receipt['attribution'][0]['result']['role'], 'reviewer')
        self.assertNotIn('SECRET', json.dumps(receipt))

    def compare_main(self, rows=None):
        receipt = self.load(rows)
        events, issues, attribution = deepcopy(MAIN_EXPECTED[self._testMethodName])
        for value in [*events, *([attribution] if attribution is not None else [])]:
            value['repo'] = value['repo'].replace('{root}', str(self.root))
        actual = parse(self.path, self.source, self.config)
        self.assertEqual([e.data() for e in actual.events], events)
        self.assertEqual(actual.issues, issues)
        self.assertEqual(self.snapshot['issues'], issues)
        self.assertEqual([e.data() for e in self.store.events()[0]], events)
        if attribution is not None and receipt['attribution']:
            result = receipt['attribution'][0]['result']
            self.assertEqual({k: result[k] for k in result if k != 'included'}, attribution)
        return receipt

    def test_13_nonstring_agent_type(self):
        """Fault: receipt validation drops main's events when a numeric sidecar type matches no role key."""
        Path(self.source['roots'][0]).with_suffix('.meta.json').write_text('{"agentType":3}')
        second = deepcopy(self.row)
        second['message']['id'] = 'second'
        receipt = self.compare_main([self.row, second])
        self.assertIsNone(receipt['attribution'][0]['inputs']['agent_type'])
        self.assertTrue(self.snapshot['provenance_valid'])

    def test_14_numeric_agent_id(self):
        """Fault: capture rejects a native number that main coerces into a full Claude agent identifier."""
        self.row['agentId'] = 42
        receipt = self.compare_main()
        self.assertEqual(receipt['attribution'][0]['inputs']['agent'], 'claude:run:42')
        self.assertEqual(receipt['attribution'][0]['inputs']['native_agent'], 42)
        self.assertTrue(self.matches())
        self.source['exclude_agents'] = [42]
        self.assertFalse(self.matches())  # Native exclusion retains its original scalar equality.

    def test_15_excluded_bad_cwd(self):
        """Fault: capturing malformed cwd adds an issue to an excluded row main skips silently."""
        self.source['exclude_agents'] = ['child']
        self.row['cwd'] = {'private': 'SECRET'}
        receipt = self.compare_main()
        self.assertIsNone(receipt['attribution'][0]['inputs']['cwd'])
        self.assertFalse(receipt['attribution'][0]['result']['included'])
        self.assertEqual(self.snapshot['provenance_reason'], 'unreplayable_input')
        self.assertNotIn('SECRET', json.dumps(receipt))

    def test_16_override_ignores_bad_cwd(self):
        """Fault: capture rejects malformed cwd that main ignores behind a registration repo."""
        self.source['repo'] = 'fixed'
        self.row['cwd'] = 3
        receipt = self.compare_main()
        self.assertIsNone(receipt['attribution'][0]['inputs']['cwd'])
        self.assertEqual(self.snapshot['provenance_reason'], 'unreplayable_input')

    def test_17_override_ignores_bad_provider(self):
        """Fault: capture rejects a non-string model_provider that main ignores behind a provider override."""
        self.source.update(kind='codex', provider='fixed')
        receipt = self.compare_main([{'type': 'session_meta', 'payload': {'id': 'run', 'model_provider': 3}},
                                    {'type': 'event_msg', 'payload': {'type': 'token_count', 'info': {
                                        'total_token_usage': {'input_tokens': 10, 'output_tokens': 2}}}}])
        self.assertIsNone(receipt['attribution'][0]['inputs']['recorded_provider'])
        self.assertEqual(self.snapshot['provenance_reason'], 'unreplayable_input')

    def test_18_import_checks_receipt(self):
        """Fault: import claims valid provenance without invoking receipt_matches on the finalized count."""
        from workflow_auditor.retention import receipt_matches
        for fault in ('empty_alias', 'null_role', 'numeric_provider'):
            self.config.pop('repo_aliases', None)
            self.config.pop('type_roles', None)
            self.source.pop('provider', None)
            if fault == 'empty_alias':
                self.config['repo_aliases'] = {str(self.cwd): ''}
            elif fault == 'null_role':
                Path(self.source['roots'][0]).with_suffix('.meta.json').write_text('{"agentType":"reviewer"}')
                self.config['type_roles'] = {'reviewer': None}
            else:
                self.source['provider'] = 3
            with mock.patch('workflow_auditor.retention.receipt_matches', wraps=receipt_matches) as check:
                self.compare_main()
                self.assertTrue(check.called)
                self.assertFalse(self.snapshot['provenance_valid'])
                self.assertIsNotNone(self.snapshot['provenance_reason'])
        self.source.pop('provider')
        self.load()
        self.assertIsNone(self.snapshot['provenance_reason'])

    def test_19_raw_root_cwd(self):
        """Fault: path_key collapses cwd '/' to empty, losing main's root alias attribution on replay."""
        self.row['cwd'] = '/'
        self.config['repo_aliases'] = {'/': 'root-alias'}
        # Capture still needs a fallback; do not inspect any filesystem outside the worktree.
        with mock.patch('workflow_auditor.adapters.repo_fallback', return_value='unattributed'):
            receipt = self.compare_main()
        self.assertEqual(receipt['attribution'][0]['inputs']['cwd'], '/')
        self.assertTrue(self.matches())

    def test_20_shadowed_guards(self):
        """Fault: equal or replay-matching corruption bypasses digest/count/result/input/detail guards."""
        self.load()
        for field, value in [('sha256', 'not-a-digest'), ('observations', True), ('kind', 'codex')]:
            bad = deepcopy(self.snapshot)
            bad[field] = value
            if field != 'kind':
                bad['receipt'][field] = value
            self.assertFalse(self.matches(bad))
        for field, value in [('role', ''), ('agent', 3)]:
            bad = deepcopy(self.snapshot)
            result = bad['receipt']['attribution'][0]['result']
            result[field] = value
            with mock.patch('workflow_auditor.adapters.replay_attribution', return_value=result):
                self.assertFalse(self.matches(bad))
        bad = deepcopy(self.snapshot)
        bad['receipt']['attribution'][0]['inputs']['model'] = None
        bad['receipt']['attribution'][0]['result']['provider_basis'] = 'registration'
        self.source['provider'] = 'anthropic'  # Provider short-circuits the malformed model.
        self.assertFalse(self.matches(bad))
        self.source.pop('provider')
        bad = deepcopy(self.snapshot)
        self.source['kind'] = bad['kind'] = bad['receipt']['kind'] = 'normalized'
        pair = bad['receipt']['attribution'][0]
        pair['result']['included'] = False
        pair['inputs']['explicit'] = deepcopy(pair['result'])
        self.assertFalse(self.matches(bad))

    def test_21_replay_reasons(self):
        """Fault: replay collapses distinct provenance failures or fails to persist their reason."""
        self.load()
        for field, value, reason in [('adapter_version', 0, 'adapter_version_mismatch'),
                                     ('sha256', None, 'invalid_receipt'),
                                     ('replayable', 1, 'invalid_receipt'),
                                     ('replayable', False, 'unreplayable_input')]:
            bad = deepcopy(self.snapshot)
            bad['receipt'][field] = value
            self.store.db.execute('UPDATE sources SET detail=?', (json.dumps(bad),))
            self.store.replay_receipts(self.source, self.config)
            self.assertEqual((self.store.sources()[0]['provenance_valid'],
                              self.store.sources()[0]['provenance_reason']), (False, reason))
        self.load()
        self.config['agent_roles'] = {'claude:run:child': 'reviewer'}
        self.store.replay_receipts(self.source, self.config)
        self.assertEqual(self.store.sources()[0]['provenance_reason'], 'attribution_mismatch')
        self.config.pop('agent_roles')
        self.store.replay_receipts(self.source, self.config)
        self.assertIsNone(self.store.sources()[0]['provenance_reason'])
        self.assertTrue(self.store.sources()[0]['provenance_valid'])

    def test_12_main_serialization(self):
        """Fault: provenance changes healthy main report/series JSON/Markdown bytes with or without labels."""
        # Frozen from main 6e88c7f, before implementation, in loop order of serializations().
        expected = ['d35a6b95c1bb0693e4c2f31526572cc4adad51cfdc5acd17d1522b2fe62045be',
                    'ba03d8fb8f226bac0b96e4a1beb76fab3924fe1943937a9f64c3b704b7ced65a',
                    'aaa61eaa28545485be52fca5ef81a6d5c4fe30ef36cd86df914789b0a1470582',
                    'b5d7a5033d2e10ac601ad59599980642cf858732819000f515d0c0758fe87581',
                    'e4682cec058f750e2cdb606cb7929c23e17c63663f66d61c93f8bce64619e031',
                    '400350a083aae8e6198e27d93e07e2c9205360f808222afb8b41b5b64d55e47d',
                    '21722c49e94384e5a6ac121c705d4e21dba6903bc928c7e2cdbb79dce941d78a',
                    'f13cb1495090b43eaf6ab1ed2a8c2cd48a53bbae012541bcc95d14829d0e84c7']
        self.assertEqual(serializations(self.root), expected)
