from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock
import io
import json
import os
import tempfile
import unittest

from workflow_auditor.cli import main

CLAUDE_ROOT, CODEX_ROOT = '~/.claude/projects', '~/.codex/sessions'


class InitConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def env(self, claude='', codex=''):
        # Empty variables count as unset; the real home is never touched.
        return mock.patch.dict(os.environ, {'HOME': str(self.home), 'USERPROFILE': str(self.home),
                                            'CLAUDE_CONFIG_DIR': claude, 'CODEX_HOME': codex})

    def run_main(self, *argv):
        out = io.StringIO()
        with redirect_stdout(out):
            code = main(list(argv))
        return code, out.getvalue()

    def init(self):
        path = self.root / 'sources.json'
        code, text = self.run_main('init-config', '--output', str(path))
        return path, code, json.loads(text)

    def claude_row(self, mid, at, sid, output=5):
        return {'type': 'assistant', 'timestamp': at, 'sessionId': sid, 'requestId': 'req-' + mid,
                'message': {'id': mid, 'model': 'claude-test', 'usage': {'input_tokens': 10,
                            'cache_creation_input_tokens': 0, 'cache_read_input_tokens': 0, 'output_tokens': output}}}

    def test_found_folders_are_registered(self):
        """Fault: a found folder is left out of the configuration."""
        for path in (self.home / '.claude/projects', self.home / '.codex/sessions'):
            path.mkdir(parents=True)
        with self.env():
            path, code, printed = self.init()
        self.assertEqual((code, printed), (0, {'config': str(path), 'registered': [CLAUDE_ROOT, CODEX_ROOT],
                                               'not_found': []}))
        config = json.loads(path.read_text())
        self.assertEqual(config['sources'], [{'name': 'claude', 'kind': 'claude', 'roots': [CLAUDE_ROOT]},
                                             {'name': 'codex', 'kind': 'codex', 'roots': [CODEX_ROOT]}])

    def test_missing_folder_is_not_registered(self):
        """Fault: a missing folder is registered, keeping every report partial."""
        (self.home / '.claude/projects').mkdir(parents=True)
        with self.env():
            path, code, printed = self.init()
        self.assertEqual(code, 0)
        config = json.loads(path.read_text())
        self.assertEqual(config['sources'], [{'name': 'claude', 'kind': 'claude', 'roots': [CLAUDE_ROOT]}])
        self.assertEqual((printed['registered'], printed['not_found']), ([CLAUDE_ROOT], [CODEX_ROOT]))

    def test_first_run_needs_no_hand_editing(self):
        """Fault: a first run fails, or reports partial, until the configuration is edited by hand."""
        session = self.home / '.claude/projects/project-one'
        session.mkdir(parents=True)
        (session / 's1.jsonl').write_text(json.dumps(self.claude_row('m1', '2026-10-20T12:00:00Z', 's1')) + '\n')
        with self.env():
            path, code, _ = self.init()
            self.assertEqual(code, 0)
            db = self.root / 'workflow.sqlite'
            self.assertEqual(self.run_main('ingest', '--config', str(path), '--db', str(db))[0], 0)
            result = json.loads(self.run_main('report', '--db', str(db))[1])
        self.assertNotEqual(result['status'], 'partial')
        self.assertEqual(result['summary']['events'], 1)
        self.assertNotIn('registered_root_missing', [i['code'] for i in result['coverage']['source_issues']])

    def test_environment_variables_win_over_existing_defaults(self):
        """Fault: the default path is registered despite the variable pointing elsewhere."""
        for path in (self.home / '.claude/projects', self.home / '.codex/sessions'):
            path.mkdir(parents=True)
        claude, codex = self.root / 'env-claude', self.root / 'env-codex'
        (claude / 'projects').mkdir(parents=True)
        (codex / 'sessions').mkdir(parents=True)
        with self.env(str(claude), str(codex)):
            path, code, printed = self.init()
        self.assertEqual(code, 0)
        roots = [str(claude / 'projects'), str(codex / 'sessions')]
        self.assertEqual((printed['registered'], printed['not_found']), (roots, []))
        self.assertEqual([s['roots'] for s in json.loads(path.read_text())['sources']], [[r] for r in roots])

    def test_no_folders_writes_both_defaults(self):
        """Fault: an empty configuration is written when nothing is found."""
        with self.env():
            path, code, printed = self.init()
        self.assertEqual(code, 0)
        config = json.loads(path.read_text())
        self.assertEqual(config['sources'], [{'name': 'claude', 'kind': 'claude', 'roots': [CLAUDE_ROOT]},
                                             {'name': 'codex', 'kind': 'codex', 'roots': [CODEX_ROOT]}])
        self.assertEqual((printed['registered'], printed['not_found']), ([], [CLAUDE_ROOT, CODEX_ROOT]))

    def test_existing_output_is_refused(self):
        """Fault: an existing configuration is overwritten."""
        path = self.root / 'sources.json'
        path.write_text('{"sentinel": true}')
        with self.env():
            err = io.StringIO()
            with redirect_stderr(err):
                code, _ = self.run_main('init-config', '--output', str(path))
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(path.read_text()), {'sentinel': True})

    def test_file_named_like_the_folder_is_not_registered(self):
        """Fault: any existing path counts, not only a directory."""
        (self.home / '.claude').mkdir()
        (self.home / '.claude/projects').write_text('a file, not a folder')
        (self.home / '.codex/sessions').mkdir(parents=True)
        with self.env():
            path, code, printed = self.init()
        self.assertEqual(code, 0)
        self.assertEqual(printed['registered'], [CODEX_ROOT])
        self.assertEqual(printed['not_found'], [CLAUDE_ROOT])
        self.assertEqual([s['roots'] for s in json.loads(path.read_text())['sources']], [[CODEX_ROOT]])

    @unittest.skipIf(not hasattr(os, 'geteuid') or os.geteuid() == 0, 'root can read every folder')
    def test_unreadable_folder_is_reported_not_found(self):
        """Fault: an unreadable folder aborts init-config."""
        codex = self.home / '.codex'
        codex.mkdir()
        codex.chmod(0o000)
        (self.home / '.claude/projects').mkdir(parents=True)
        try:
            with self.env():
                path, code, printed = self.init()
        finally:
            codex.chmod(0o755)
        self.assertEqual(code, 0)
        self.assertEqual(printed['registered'], [CLAUDE_ROOT])
        self.assertEqual(printed['not_found'], [CODEX_ROOT])

    def test_notes_name_what_was_found(self):
        """Fault: the notes or the unchanged keys drift."""
        (self.home / '.claude/projects').mkdir(parents=True)
        with self.env():
            path, code, _ = self.init()
        self.assertEqual(code, 0)
        config = json.loads(path.read_text())
        self.assertIn('Found: ~/.claude/projects.', config['notes'])
        self.assertIn('Not found: ~/.codex/sessions.', config['notes'])
        self.assertEqual((config['repo_aliases'], config['agent_roles'], config['type_roles']), ({}, {}, {}))
        self.assertTrue(config['price_book'].endswith('prices-20261022.json'))

    def test_relative_environment_variable_is_written_absolute(self):
        """Fault: a relative root is written that breaks from another directory."""
        (self.root / 'rel-claude/projects').mkdir(parents=True)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.root)
        with self.env('rel-claude'):
            path, code, printed = self.init()
        self.assertEqual(code, 0)
        root = json.loads(path.read_text())['sources'][0]['roots'][0]
        self.assertTrue(os.path.isabs(root))
        self.assertEqual(root, str(self.root / 'rel-claude/projects'))
        self.assertEqual(printed['registered'], [root])
