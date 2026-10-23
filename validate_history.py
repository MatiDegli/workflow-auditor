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


def validate(root: Path, db: Path | None = None) -> dict:
    """root holds history-fixtures.json and cost-fixtures.json; the database is checked first."""
    db = db or root / 'historical.sqlite'
    if not db.is_file():
        raise ValueError(f'No snapshot at {db}: databases are not tracked in Git. Pass --db with its path.')
    store = Store(db, readonly=True)
    try:
        if not (root / 'history-fixtures.json').is_file():
            raise ValueError(f'No history-fixtures.json in {root}. Pass --fixtures with its folder.')
        fixtures = json.loads((root / 'history-fixtures.json').read_text())
        if 'prices' not in store.config():
            raise ValueError(f'{db} has no stored price book; it is not the historical snapshot.')
        events, issues = store.events()
        conflicts = [i for i in issues if i['code'] not in INFORMATIONAL]
        assert not conflicts, conflicts
        checks = []
        for f in fixtures:
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
        cost_checks=[]
        for f in json.loads((root/'cost-fixtures.json').read_text()):
            book=deepcopy(store.config()['prices'])
            for model, rates in f['price_overrides'].items():
                for tier in book['models'][model]['tiers']:
                    tier.update(rates)
            r=report(store,Selection(since=timestamp(f['window']['start']),until=timestamp(f['window']['end']),
                                   repos=(f['repo'],),providers=('anthropic',)),book)
            quote=next(q for q in r['summary']['list_estimates'] if q['unit']==f['unit'])
            tolerance=Decimal(f['tolerance_absolute'])
            matches=all(abs(Decimal(quote[key])-Decimal(f['expected']))<=tolerance for key in ('known_min','known_max'))
            cost_checks.append({'id':f['id'],'expected':f['expected'],'unit':f['unit'],
                                'observed_min':quote['known_min'],'observed_max':quote['known_max'],
                                'tolerance_absolute':str(tolerance),
                                'complete':quote['complete_for_selection'],
                                'pass':matches and quote['complete_for_selection']})
        return {'basis': 'Frozen prior accounting references, not invoices or independent acceptance.',
                'cases': len(fixtures), 'checks': len(checks), 'passed': sum(x['pass'] for x in checks),
                'comparisons': checks, 'cost_checks':len(cost_checks),
                'cost_checks_passed':sum(x['pass'] for x in cost_checks), 'cost_comparisons':cost_checks}
    finally:
        store.close()


if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, default=here,
                        help='folder with history-fixtures.json and cost-fixtures.json (default: beside this script)')
    parser.add_argument('--db', type=Path,
                        help='snapshot to validate, opened read-only (default: historical.sqlite in the fixtures folder)')
    args = parser.parse_args()
    try:
        result = validate(args.fixtures, args.db)
    except (OSError, ValueError) as exc:
        print(f'validate_history: {exc}', file=sys.stderr)
        raise SystemExit(2)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result['checks'] == result['passed'] and result['cost_checks']==result['cost_checks_passed'] else 1)
