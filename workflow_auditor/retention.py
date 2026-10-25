"""Import-time metadata for replay; receipt versions are independent of SQLite's schema."""

import json
import re

# Bump only the kind whose parsing semantics changed.
ADAPTER_VERSIONS = dict.fromkeys(('claude', 'codex', 'glm_report', 'claude_stream', 'router', 'normalized'), 1)
RESULT_FIELDS = {'repo', 'provider', 'provider_basis', 'agent', 'role', 'role_basis', 'included'}
INPUT_FIELDS = {'cwd', 'fallback', 'model', 'recorded_provider', 'native_agent', 'agent', 'agent_type', 'root', 'explicit'}


def controls(source: dict) -> dict:
    from .model import timestamp
    return {k: timestamp(source.get(k)) for k in ('since', 'until')} if source['kind'] == 'claude_stream' else {}


def capture(source: dict, digest: str, attribution: list[dict], replayable: bool = True) -> dict:
    from .adapters import local_path, path_key
    roots = []
    for value in source.get('roots', []):
        root = local_path(value)
        roots.append({'identity': path_key(str(root.absolute())), 'resolved': path_key(str(root.resolve())),
                      'type': 'file' if root.is_file() else 'directory' if root.is_dir() else 'missing'})
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
                all(isinstance(v, str) and v for v in root.values()) and root['type'] in ('file', 'directory', 'missing') for root in r['roots']):
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
