"""Compare an accounting snapshot with frozen regression references (history and cost fixtures)."""
from pathlib import Path
from copy import deepcopy
from decimal import Decimal
import argparse
import json
import sys

from workflow_auditor.model import INFORMATIONAL, timestamp
from workflow_auditor.store import Store
from workflow_auditor.query import report, Selection


def default_fixtures_folder(script_folder: Path) -> Path:
    """The script's folder when it ships history-fixtures.json, otherwise examples/history beside it."""
    return script_folder if (script_folder / 'history-fixtures.json').is_file() else script_folder / 'examples' / 'history'


def where(f, index, name):
    """Fixture file and case, for one-line fault messages; the index stands in when there is no id."""
    case = f.get('id') if isinstance(f, dict) and isinstance(f.get('id'), str) else f'#{index}'
    return f'{name}: case {case}'


def need(f, index, name, key, *types):
    """A missing key or a wrong type is 'could not check': one line naming file, case and key."""
    if not isinstance(f, dict) or key not in f:
        raise ValueError(f"{where(f, index, name)}: missing key '{key}'")
    if types and not isinstance(f[key], types):
        expected = ' or '.join(t.__name__ for t in types)
        raise ValueError(f"{where(f, index, name)}: key '{key}' is {type(f[key]).__name__}, not {expected}")
    return f[key]


def need_window(f, index, name):
    window = need(f, index, name, 'window', dict)
    for part in ('start', 'end'):
        if part not in window:
            raise ValueError(f"{where(f, index, name)}: missing key 'window.{part}'")
        value = window[part]
        if not isinstance(value, str):
            raise ValueError(f"{where(f, index, name)}: key 'window.{part}' is not an ISO 8601 timestamp")
        try:
            timestamp(value)
        except ValueError:
            raise ValueError(f"{where(f, index, name)}: key 'window.{part}' is not an ISO 8601 timestamp") from None
    return window


def check_history_fixture(f, index, name):
    """Every key the comparison reads, checked before it reads one."""
    kind = need(f, index, name, 'kind', str)
    if kind == 'codex':
        need(f, index, name, 'agents', list)
    elif kind == 'glm':
        need(f, index, name, 'run', str)
        try:
            timestamp(need(f, index, name, 'start', str))
        except ValueError:
            raise ValueError(f"{where(f, index, name)}: key 'start' is not an ISO 8601 timestamp") from None
    elif kind == 'claude':
        need(f, index, name, 'repo', str)
        need_window(f, index, name)
    else:
        raise ValueError(f"{where(f, index, name)}: unknown kind '{kind}'")
    need(f, index, name, 'expected', dict)


def check_cost_fixture(f, index, name, models):
    """Every key the cost comparison reads, including that overrides name priced models."""
    need(f, index, name, 'repo', str)
    need(f, index, name, 'unit', str)
    need_window(f, index, name)
    for key in ('expected', 'tolerance_absolute'):
        try:
            Decimal(str(need(f, index, name, key, str, int, float)))
        except ArithmeticError:
            raise ValueError(f"{where(f, index, name)}: key '{key}' is not a decimal number") from None
    for model, rates in need(f, index, name, 'price_overrides', dict).items():
        if model not in models:
            raise ValueError(f"{where(f, index, name)}: price_overrides names model '{model}', which the price book lacks")
        if not isinstance(rates, dict):
            raise ValueError(f"{where(f, index, name)}: key 'price_overrides[{model}]' is {type(rates).__name__}, not a tier mapping")


def validate(root: Path, db: Path | None = None) -> dict:
    """root holds history-fixtures.json and cost-fixtures.json; the database is checked first."""
    db = db or root / 'historical.sqlite'
    if not db.is_file():
        raise ValueError(f'No snapshot at {db}: databases are not tracked in Git. Pass --db with its path.')
    store = Store(db, readonly=True)
    try:
        if not (root / 'history-fixtures.json').is_file():
            raise ValueError(f'No history-fixtures.json in {root}. Pass --fixtures with its folder.')
        name = root / 'history-fixtures.json'
        fixtures = json.loads(name.read_text())
        if 'prices' not in store.config():
            raise ValueError(f'{db} has no stored price book; it is not the historical snapshot.')
        events, issues = store.events()
        conflicts = {}
        for issue in issues:
            if issue['code'] not in INFORMATIONAL:
                conflicts[issue['code']] = conflicts.get(issue['code'], 0) + 1
        checks = []
        for index, f in enumerate(fixtures):
            check_history_fixture(f, index, name)
            if f['kind'] == 'codex':
                items = [e for e in events if e.agent in f['agents']]
            elif f['kind'] == 'glm':
                items = [e for e in events if e.run == f['run'] and e.unit == 'attempt_total' and e.start == timestamp(f['start'])]
            else:
                start, end = timestamp(f['window']['start']), timestamp(f['window']['end'])
                items = [e for e in events if e.repo == f['repo'] and e.client == 'claude_code' and
                         e.time_kind == 'point' and e.at is not None and start <= e.at < end]
            for key, expected in f['expected'].items():
                values = [getattr(e.tokens, key) for e in items] if key != 'events' else []
                actual = len(items) if key == 'events' else sum(values) if all(x is not None for x in values) else None
                checks.append({'case': f['id'], 'metric': key, 'expected': expected,
                               'observed': actual, 'pass': actual == expected})
        cost_name = root / 'cost-fixtures.json'
        cost_checks=[]
        for index, f in enumerate(json.loads(cost_name.read_text())):
            book=deepcopy(store.config()['prices'])
            check_cost_fixture(f, index, cost_name, book['models'])
            for model, rates in f['price_overrides'].items():
                for tier in book['models'][model]['tiers']:
                    tier.update(rates)
            r=report(store,Selection(since=timestamp(f['window']['start']),until=timestamp(f['window']['end']),
                                   repos=(f['repo'],),providers=('anthropic',)),book)
            quote=next((q for q in r['summary']['list_estimates'] if q['unit']==f['unit']), None)
            tolerance=Decimal(f['tolerance_absolute'])
            if quote is None:  # No list estimate for this unit: a failed comparison, not an exception.
                cost_checks.append({'id':f['id'],'expected':f['expected'],'unit':f['unit'],
                                    'observed_min':None,'observed_max':None,
                                    'tolerance_absolute':str(tolerance),'complete':False,'pass':False})
                continue
            matches=all(abs(Decimal(quote[key])-Decimal(f['expected']))<=tolerance for key in ('known_min','known_max'))
            cost_checks.append({'id':f['id'],'expected':f['expected'],'unit':f['unit'],
                                'observed_min':quote['known_min'],'observed_max':quote['known_max'],
                                'tolerance_absolute':str(tolerance),
                                'complete':quote['complete_for_selection'],
                                'pass':matches and quote['complete_for_selection']})
        return {'basis': 'Frozen prior accounting references, not invoices or independent acceptance.',
                'conflicts': conflicts,
                'cases': len(fixtures), 'checks': len(checks), 'passed': sum(x['pass'] for x in checks),
                'comparisons': checks, 'cost_checks':len(cost_checks),
                'cost_checks_passed':sum(x['pass'] for x in cost_checks), 'cost_comparisons':cost_checks}
    finally:
        store.close()


if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, default=default_fixtures_folder(here),
                        help='folder with history-fixtures.json and cost-fixtures.json '
                             '(default: history-fixtures.json beside this script, else examples/history beside it)')
    parser.add_argument('--db', type=Path,
                        help='snapshot to validate, opened read-only (default: historical.sqlite in the fixtures folder)')
    args = parser.parse_args()
    try:
        result = validate(args.fixtures, args.db)
    except (OSError, ValueError) as exc:
        print(f'validate_history: {exc}', file=sys.stderr)
        raise SystemExit(2)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if not result['conflicts'] and result['checks'] == result['passed']
                     and result['cost_checks'] == result['cost_checks_passed'] else 1)
