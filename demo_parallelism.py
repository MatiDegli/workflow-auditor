"""Reproduce the README's agent-hour example with explicitly synthetic own-activity intervals."""
from pathlib import Path
import json
import tempfile

from workflow_auditor.adapters import ingest
from workflow_auditor.model import Event, Tokens, timestamp
from workflow_auditor.query import Selection, compare_periods
from workflow_auditor.store import Store


def demo() -> dict:
    start = timestamp('2026-10-22T10:00:00-03:00')
    rows = []
    for i in range(4):
        beginning = start if i < 3 else start + 7200
        rows.append(Event(str(i), 'synthetic', 'synthetic', 'fixture', 'example', 'example', str(i),
                          str(i), None, 'example', 'fixture', Tokens(100 if i < 3 else 150, 0, 0, 0),
                          'point', at=beginning, activity_start=beginning, activity_end=beginning + 3600,
                          activity_basis='own').data())
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = root / 'synthetic.jsonl'
        path.write_text(''.join(json.dumps(x) + '\n' for x in rows))
        config = {'sources': [{'name': 'synthetic', 'kind': 'normalized', 'roots': [str(path)]}],
                  'prices': {'as_of': 'synthetic', 'source': 'Illustrative units only, not provider pricing',
                             'models': {'example': {'unit': 'example_units', 'tiers': [{'input': 1, 'read': 0, 'output': 0}]}}}}
        store = Store(root / 'demo.sqlite')
        try:
            ingest(store, config)
            result = compare_periods(store, Selection(since=start, until=start + 3600, activity='observed'),
                                     Selection(since=start + 7200, until=start + 10800, activity='observed'))
            result['dataset'] = 'synthetic user parallelism example; no actual provider use'
            return result
        finally:
            store.close()


if __name__ == '__main__':
    print(json.dumps(demo(), indent=2))
