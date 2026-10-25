"""Import-time metadata for replay; receipt versions are independent of SQLite's schema."""

from fnmatch import fnmatch, fnmatchcase
from pathlib import Path
import json
import os
import re
import stat

# Bump only the kind whose parsing semantics changed.
ADAPTER_VERSIONS = dict.fromkeys(('claude', 'codex', 'glm_report', 'claude_stream', 'router', 'normalized'), 1)
RESULT_FIELDS = {'repo', 'provider', 'provider_basis', 'agent', 'role', 'role_basis', 'included'}
INPUT_FIELDS = {'cwd', 'fallback', 'model', 'recorded_provider', 'native_agent', 'agent', 'agent_type', 'root', 'explicit'}


def controls(source: dict) -> dict:
    from .model import timestamp
    return {k: timestamp(source.get(k)) for k in ('since', 'until')} if source['kind'] == 'claude_stream' else {}


def capture(source: dict, digest: str, attribution: list[dict], replayable: bool = True) -> dict:
    roots = [{k: r[k] for k in ('identity', 'resolved', 'type')} for r in root_states(source)]
    return {'version': 1, 'sha256': digest, 'kind': source['kind'], 'adapter_version': ADAPTER_VERSIONS[source['kind']],
            'roots': roots, 'controls': controls(source), 'attribution': attribution, 'replayable': replayable}


def result_shape(value: object) -> bool:
    return (isinstance(value, dict) and set(value) == RESULT_FIELDS and type(value['included']) is bool
            and all(isinstance(value[k], str) and value[k] for k in RESULT_FIELDS - {'agent', 'included'})
            and (value['agent'] is None or isinstance(value['agent'], str)))


def input_shape(value: object) -> bool:
    return (isinstance(value, dict) and set(value) == INPUT_FIELDS and type(value['root']) is bool
            and all(v is None or isinstance(v, str) for k, v in value.items() if k not in ('root', 'explicit', 'native_agent'))
            and (value['native_agent'] is None or type(value['native_agent']) in (str, int, float, bool))
            and isinstance(value['model'], str) and isinstance(value['fallback'], str)
            and (value['explicit'] is None or result_shape(value['explicit']) and value['explicit']['included']))


def receipt_matches(snapshot: dict, source: dict, config: dict) -> tuple[bool, str | None]:
    """No source/cwd/sidecar reads: only the results of applicable inputs can invalidate attribution."""
    from .adapters import replay_attribution
    try:
        r = snapshot['receipt']
        if set(r) != {'version', 'sha256', 'kind', 'adapter_version', 'roots', 'controls', 'attribution', 'observations', 'replayable'}:
            return False, 'invalid_receipt'
        if type(r['version']) is not int or r['version'] != 1:
            return False, 'invalid_receipt'
        if r['kind'] != source['kind']:
            return False, 'attribution_mismatch'
        if r['kind'] != snapshot['kind']:
            return False, 'invalid_receipt'
        if type(r['adapter_version']) is not int or r['adapter_version'] != ADAPTER_VERSIONS[r['kind']]:
            return False, 'adapter_version_mismatch'
        if not re.fullmatch('[0-9a-f]{64}', r['sha256']) or r['sha256'] != snapshot['sha256']:
            return False, 'invalid_receipt'
        if type(r['observations']) is not int or r['observations'] < 0 or r['observations'] != snapshot['observations']:
            return False, 'invalid_receipt'
        if r['controls'] != controls(source):
            return False, 'attribution_mismatch'
        if type(r['replayable']) is not bool:
            return False, 'invalid_receipt'
        if not r['replayable']:
            return False, 'unreplayable_input'
        if not isinstance(r['roots'], list) or not all(set(root) == {'identity', 'resolved', 'type'} and
                all(isinstance(v, str) and v for v in root.values()) and root['type'] in ('file', 'directory', 'missing', 'other', 'unknown') for root in r['roots']):
            return False, 'invalid_receipt'
        if not isinstance(r['attribution'], list) or (r['observations'] and not r['attribution']):
            return False, 'invalid_receipt'
        distinct = set()
        for pair in r['attribution']:
            if set(pair) != {'inputs', 'result'} or not result_shape(pair['result']):
                return False, 'invalid_receipt'
            inputs = pair['inputs']
            if not input_shape(inputs) or (r['kind'] == 'normalized') != (inputs['explicit'] is not None):
                return False, 'invalid_receipt'
            key = json.dumps(inputs, sort_keys=True)
            if key in distinct:
                return False, 'invalid_receipt'
            if replay_attribution(inputs, source, config) != pair['result']:
                return False, 'attribution_mismatch'
            distinct.add(key)
        return True, None
    except (KeyError, TypeError, ValueError, AttributeError):
        return False, 'invalid_receipt'


def lexical(value: str) -> str:
    from .adapters import local_path, path_key
    return path_key(os.path.normpath(str(local_path(value).absolute()))) or '/'


def snapshot_problem(snapshot: dict, source: dict, config: dict, count: int) -> str | None:
    valid, reason = receipt_matches(snapshot, source, config)
    if not valid:
        return reason
    if count != snapshot.get('observations'):
        return 'count_mismatch'
    return None if snapshot.get('provenance_valid') is True else snapshot.get('provenance_reason') or 'invalid_receipt'


def parent_directory_present(root: dict) -> bool:
    try:
        return stat.S_ISDIR(Path(root['identity']).parent.stat().st_mode)
    except OSError:
        return False


def captured_file(root: dict, snapshot: dict) -> bool:
    receipt = snapshot.get('receipt')
    return (snapshot.get('sha256') is not None and (snapshot.get('status') != 'retired' or 'membership_exclusion' in snapshot)
            and isinstance(receipt, dict) and isinstance(receipt.get('roots'), list) and any(
        isinstance(r, dict) and r.get('type') == 'file' and isinstance(r.get('identity'), str)
        and isinstance(r.get('resolved'), str) and lexical(r['resolved']) == lexical(snapshot['path'])
        and lexical(r['identity']) == root['identity'] for r in receipt['roots']))


def root_states(source: dict) -> list[dict]:
    from .adapters import local_path
    roots = []
    for value in source.get('roots', []):
        root = local_path(value)
        item = dict(identity=lexical(value), resolved=lexical(value), type='missing', health='missing')
        try:
            mode = root.stat().st_mode
            item.update(resolved=lexical(str(root.resolve())),
                        type='file' if stat.S_ISREG(mode) else 'directory' if stat.S_ISDIR(mode) else 'other', health='healthy')
        except FileNotFoundError:
            pass
        except OSError:
            item.update(health='error', type='unknown')
        roots.append(item)
    return roots


def glob_file(relative: str, pattern: str) -> bool:
    # Path.glob's ** consumes directories, including zero; a trailing ** or slash selects no files.
    parts, wanted = relative.split('/'), Path(pattern).parts
    def matches(i, j):
        if j == len(wanted):
            return i == len(parts)
        if wanted[j] == '**':
            return j + 1 < len(wanted) and (matches(i, j + 1) or i < len(parts) - 1 and matches(i + 1, j))
        return i < len(parts) and fnmatchcase(parts[i], wanted[j]) and matches(i + 1, j + 1)
    raw = matches(0, 0)
    parts, wanted = os.path.normpath(relative).split('/'), Path(os.path.normpath(pattern)).parts
    return not pattern.endswith('/') and (raw or matches(0, 0))


def membership(path: str, source: dict, roots: list[dict], previous: list[dict] = (), discovered: bool = False) -> tuple[list[dict], str]:
    from .adapters import local_path
    pattern = source.get('pattern') or ('**/report.json' if source['kind'] == 'glm_report' else '**/*.jsonl')
    candidates, inside = [], False
    for current in roots:
        root = dict(current)
        saved = next((r for r in previous if r['identity'] == root['identity'] or r['resolved'] == root['resolved']), {})
        if root['type'] == 'missing':
            root['type'] = saved.get('type', 'missing')
        relative = saved.get('relative')
        for prefix in (root['identity'], root['resolved'], saved.get('resolved', root['resolved'])):
            key = lexical(path)
            if key == prefix or key.startswith(prefix.rstrip('/') + '/'):
                if relative is None:
                    relative = os.path.normpath(str(local_path(path).absolute()))[len(prefix):].lstrip('/')
                break
        if relative is None:
            continue
        inside = True
        if discovered or root['type'] in ('file', 'unknown', 'missing') and relative == '' or root['type'] != 'file' and glob_file(relative, pattern):
            candidates.append({**root, 'relative': relative})
    if any(fnmatch(path, p) or fnmatch(Path(path).name, p) for p in source.get('exclude', [])):
        return [], 'excluded_by_exclusion'
    return candidates, 'excluded_by_pattern' if inside else 'excluded_by_roots'


def membership_settings(source: dict) -> dict:
    return dict(roots=sorted({lexical(r) for r in source.get('roots', [])}),
                pattern=source.get('pattern') or ('**/report.json' if source['kind'] == 'glm_report' else '**/*.jsonl'),
                exclude=sorted(source.get('exclude', [])))


def exclusion_proven(path: str, source: dict, roots: list[dict], previous: list[dict], detail: dict) -> bool:
    settings = membership_settings(source)
    saved = detail.get('membership_settings')
    receipt = detail.get('receipt')
    prior_roots = receipt.get('roots', []) if isinstance(receipt, dict) else previous
    identities = sorted({lexical(r['identity']) for r in prior_roots if isinstance(r, dict) and isinstance(r.get('identity'), str)}) if isinstance(prior_roots, list) else []
    if saved is not None and saved != settings or identities and identities != settings['roots']:
        return True
    # Without saved settings, only a healthy, known root can prove lexical exclusion.
    healthy = [r for r in roots if r['health'] == 'healthy' and r['type'] in ('file', 'directory')]
    return bool(membership(path, {**source, 'pattern': '**/*', 'exclude': []}, healthy, previous)[0])
