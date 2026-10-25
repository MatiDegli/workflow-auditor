from dataclasses import replace
from pathlib import Path
import hashlib
import json
import tempfile
import unittest

from workflow_auditor.cli import markdown
from workflow_auditor.labels import Label, Labels, Rule
from workflow_auditor.model import Event, Tokens
from workflow_auditor.query import Selection, compare_periods, report, select, series
from workflow_auditor.store import Store

START = 1792490400.0  # Synthetic UTC hour boundary.
BOOK = {'models': {'test': {'unit': 'test_units', 'tiers': [{'input': 1, 'read': .1, 'output': 2}]}}}
LABELS = Labels('synthetic', (Label('task', (Rule(START-7200, START+14400, repos=('repo',)),)),
                              Label('narrow', (Rule(START, START+3600),))))


def point(key='point', **kw):
    fields = dict(key=key, client='test', provider='test', provider_basis='fixture', model='test',
                  repo='repo', run=key, agent='root', parent=None, role='builder', role_basis='fixture',
                  tokens=Tokens(100, 10, 0, 20, 0), time_kind='point', at=START+100,
                  activity_start=START, activity_end=START+3600, activity_basis='own', cli_usd='1')
    fields.update(kw)
    return Event(**fields)


def aggregate(key='cross', **kw):
    fields = dict(time_kind='aggregate', start=START-100, end=START+3700, at=None,
                  activity_start=None, activity_end=None, activity_basis=None, unit='attempt_total')
    fields.update(kw)
    return point(key, **fields)


class CrossingActivityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root/'audit.sqlite')
        self.addCleanup(self.store.close)
        self.store.put_config({'sources': [{'name': 'test'}], 'prices': BOOK})
        self.selection = Selection(since=START, until=START+3600, activity='observed')

    def put(self, events, issues=()):
        self.store.replace_source('test', 'test', str(self.root/'fixture'), 'synthetic', events, {'issues': list(issues)})
        self.store.set_import_state('complete')

    def view(self, **kw):
        return report(self.store, replace(self.selection, **kw))

    def assert_missing(self, result, missing=('root',)):
        a = result['summary']['activity']
        for field in ('agent_hours', 'project_active_hours', 'parallelism'):
            self.assertIsNone(a[field], field)
        self.assertEqual(a['agents_or_aggregates_missing_activity'], list(missing))
        self.assertFalse(result['coverage']['rate_denominators_complete'])

    def test_01_contained_and_crossing_units(self):
        """Fault: crossing attempt/run totals lose missing activity under either method."""
        for unit in ('attempt_total', 'run_model_total'):
            for method in ('observed', 'buckets'):
                for start, end in ((START, START+3600), (START-100, START+3700)):
                    with self.subTest(unit=unit, method=method, start=start):
                        self.put([aggregate(unit=unit, start=start, end=end)])
                        r = self.view(activity=method)
                        self.assert_missing(r)
                        self.assertEqual(r['status'], 'ready_with_missing_metrics')
                        self.assertEqual(r['summary']['activity']['known_agent_hours'], 0)

    def test_02_crossing_only_empty_groups(self):
        """Fault: crossing-only selection claims complete zero hours or vacuous additivity."""
        self.put([aggregate(agent=None)])
        r = self.view()
        self.assert_missing(r, ('aggregate:cross',))
        self.assertEqual(r['groups'], [])
        self.assertEqual(r['summary']['events'], 0)
        self.assertFalse(r['group_activity_totals_are_additive'])
        self.assertEqual(r['coverage']['omitted'], dict(outside_window=0, unknown_time=0, aggregate_crosses_boundary=1, filters=0))

    def test_03_three_agents_and_missing_crosser(self):
        """Fault: other activity for the crosser's agent hides its own missing evidence."""
        self.put([point(str(i), agent=str(i)) for i in range(3)] + [aggregate(agent='0')])
        r = self.view()
        self.assert_missing(r, ('0',))
        self.assertEqual(r['summary']['activity']['known_agent_hours'], 3)
        self.assertEqual(r['summary']['activity']['known_project_active_hours'], 1)
        self.assertFalse(r['group_activity_totals_are_additive'])
        self.assertIsNone(r['groups'][0]['activity']['agent_hours'])

    def test_04_observed_clip_union_and_addition(self):
        """Fault: crossing own intervals are dropped, unclipped, or summed without per-agent union."""
        self.put([point(activity_start=START+900, activity_end=START+1800),
                  aggregate('a', activity_start=START-100, activity_end=START+2700, activity_basis='own'),
                  aggregate('b', activity_start=START+1800, activity_end=START+3700, activity_basis='own'),
                  aggregate('c', agent='child', activity_start=START-10, activity_end=START+3610, activity_basis='own')])
        r = self.view()
        a = r['summary']['activity']
        self.assertEqual((a['agent_hours'], a['project_active_hours'], a['parallelism']), (2, 1, 2))
        self.assertEqual(r['groups'][0]['activity'], a)
        self.assertTrue(r['coverage']['rate_denominators_complete'])
        self.assertTrue(r['group_activity_totals_are_additive'])
        self.assertEqual(r['status'], 'ready_with_missing_metrics')
        self.assertEqual(r['summary']['events'], 1)

    def test_05_bucket_duration_is_not_activity(self):
        """Fault: aggregate span or observed evidence is fabricated into request buckets."""
        for basis in (None, 'own', 'parent_inclusive'):
            self.put([point(), aggregate(activity_start=START-100, activity_end=START+3700, activity_basis=basis)])
            r = self.view(activity='buckets')
            self.assert_missing(r)
            self.assertAlmostEqual(r['summary']['activity']['known_agent_hours'], 1/6)
            if basis != 'own':
                self.assert_missing(self.view())

    def test_06_integrity_failures_remain_partial(self):
        """Fault: boundary status promotes source/conflict/time/hierarchy/import/overlap failures."""
        for fault in ('source', 'conflict', 'unknown', 'hierarchy', 'import', 'overlap'):
            with self.subTest(fault=fault):
                events = [point(), aggregate()]
                if fault == 'unknown': events.append(point('unknown', at=None))
                if fault == 'hierarchy': events.append(point('cycle', parent='root'))
                if fault == 'overlap': events.append(aggregate('contained', run='point', start=START, end=START+3600))
                self.put(events, issues=[{'code': 'invalid_json'}] if fault == 'source' else [])
                if fault == 'conflict':
                    self.store.replace_source('copy', 'test', 'synthetic-copy', 'hash', [point(model='other')], {'issues': []})
                if fault == 'import': self.store.set_import_state('running')
                r = self.view(agents=('root',), descendants=True)
                self.assertEqual(r['status'], 'partial')
                self.assertIsNone(r['summary']['activity']['agent_hours'])
                self.assertFalse(r['coverage']['rate_denominators_complete'])
                if fault == 'conflict':
                    self.store.replace_source('copy', 'test', 'synthetic-copy', 'hash', [], {'issues': []})

    def test_07_unknown_endpoints_stay_unknown(self):
        """Fault: unknown aggregate endpoints become crossings or invented activity."""
        for start, end in ((None, START+100), (START-100, None), (None, None)):
            self.put([point(), aggregate(start=start, end=end)])
            r = self.view()
            self.assertEqual(r['status'], 'partial')
            self.assertEqual(r['coverage']['omitted']['unknown_time'], 1)
            self.assertEqual(r['coverage']['omitted']['aggregate_crosses_boundary'], 0)
            self.assertEqual(r['summary']['activity']['known_agent_hours'], 1)
            self.assertIsNone(r['summary']['tokens']['input_total'])

    def test_08_consumption_withheld_and_known_values_preserved(self):
        """Fault: crossers enter consumption or overlap detection, or boundary numerators/rates claim completeness."""
        self.put([point()])
        before = self.view(labels=LABELS)
        self.put([point(), aggregate(agent='child', repo='other', role='unknown', provider='other',
                                    activity_start=START, activity_end=START+3600, activity_basis='own')])
        r = self.view(labels=LABELS)
        a, b = r['summary'], before['summary']
        for key in ('events', 'event_units', 'agents', 'runs', 'known_token_subtotals', 'events_missing_token_fields', 'unpriced_events', 'zero_usage_events'):
            self.assertEqual(a[key], b[key], key)
        for key in ('labels', 'copied_responses_reattributed', 'speed_geo_mismatches', 'aggregate_detail_overlaps', 'withheld_overlap_events', 'unclassified_role_events', 'unattributed_repo_events'):
            self.assertEqual(r['coverage'][key], before['coverage'][key], key)
        self.assertEqual(r['provider_attribution'], before['provider_attribution'])
        for row in [a, *r['groups']]:
            self.assertTrue(all(v is None for v in row['tokens'].values()))
            for key in ('input_tokens_per_agent_hour', 'output_tokens_per_agent_hour', 'cache_read_fraction'):
                self.assertIsNone(row[key])
            q = row['list_estimates'][0]
            self.assertFalse(q['complete_for_selection'])
            self.assertIsNone(q['per_agent_hour_min'])
            self.assertIsNone(q['per_agent_hour_max'])
            self.assertEqual((q['known_min'], q['known_max'], q['priced_events']), ('0.000131', '0.000131', 1))
            self.assertEqual(row['cli_recorded_usd_estimate'], dict(known_total='1', events=1, complete_for_selection=False))
        self.assertEqual(markdown(r).count('Crossing aggregates are withheld'), 1)
        self.assertIn('never prorated', markdown(r))
        self.assertIn('unknown', markdown(r))
        comparison = compare_periods(self.store, self.selection, replace(self.selection, since=START-200, until=START+4000))
        self.assertTrue(all(v.get('change_percent', v.get('change_percent_of_minima')) is None for v in comparison['changes'].values()))
        self.put([point(), aggregate(run='point', unit='run_model_total')])
        r = self.view()
        self.assertEqual(r['status'], 'ready_with_missing_metrics')
        self.assertEqual(r['coverage']['aggregate_detail_overlaps'], [])
        self.assertEqual(r['coverage']['withheld_overlap_events'], 0)
        self.assertEqual(r['summary']['events'], 1)

    def test_09_series_matches_standalone(self):
        """Fault: indexed series loses crossers, group activity, status or range coverage."""
        self.put([point(), point('later', at=START+3700), aggregate(end=START+4000)])
        for method in ('observed', 'buckets'):
            for grouping in ((), ('repo', 'provider', 'role'), ('label',)):
                s = replace(self.selection, until=START+7200, activity=method, group_by=grouping, labels=LABELS)
                changes = self.store.db.total_changes
                result = series(self.store, s, '1h', 'UTC')
                self.assertEqual(result['coverage'], report(self.store, s)['coverage'])
                for i, period in enumerate(result['periods']):
                    r = report(self.store, replace(s, since=START+i*3600, until=START+(i+1)*3600))
                    for key in ('status', 'summary', *(['groups'] if grouping else [])):
                        self.assertEqual(period[key], r[key])
                    self.assertEqual(period['omitted'], r['coverage']['omitted'])
                self.assertEqual(markdown(result).count('Crossing aggregates are withheld'), 1)
                self.assertEqual(self.store.db.total_changes, changes)
        self.put([aggregate(start=START+100, end=START+4000)])
        self.assertEqual(markdown(series(self.store, s, '1h', 'UTC')).count('Crossing aggregates are withheld'), 1)

    def test_10_filters_before_crossers(self):
        """Fault: ordinary/descendant/whole-event label filters leak excluded crossers."""
        for attr, option in (('repo', 'repos'), ('provider', 'providers'), ('role', 'roles'), ('model', 'models'), ('agent', 'agents')):
            self.put([point(), aggregate(**{attr: 'other'})])
            s = replace(self.selection, **{option: (getattr(point(), attr),)})
            r = report(self.store, s)
            self.assertEqual(r['status'], 'ready')
            self.assertEqual(r['summary']['activity']['agent_hours'], 1)
            self.assertEqual(r['coverage']['omitted']['filters'], 1)
            self.assertEqual(series(self.store, s, '1h', 'UTC')['periods'][0]['summary'], r['summary'])
        self.put([point(), aggregate(agent='child', parent='root'), aggregate('excluded', agent='other')])
        s = replace(self.selection, agents=('root',), descendants=True)
        r = report(self.store, s)
        self.assert_missing(r, ('child',))
        self.assertEqual(series(self.store, s, '1h', 'UTC')['periods'][0]['summary'], r['summary'])
        self.assertEqual(self.view(agents=('root',))['status'], 'ready')
        labels = Labels('synthetic', (Label('task', (Rule(START, START+7200),)),))
        self.put([point(), aggregate()])  # The clipped fragment fits task; the whole aggregate does not.
        for name, events in (('task', 1), ('unlabeled', 0)):
            s = replace(self.selection, labels=labels, label=name, group_by=('label',))
            r = report(self.store, s)
            self.assertEqual(r['summary']['events'], events)
            self.assertEqual(r['coverage']['omitted']['aggregate_crosses_boundary'], 1-events)
            self.assertEqual(r['summary']['activity']['agent_hours'], 1 if events else None)
            self.assertEqual(series(self.store, s, '1h', 'UTC')['periods'][0]['summary'], r['summary'])

    def test_11_group_matching_and_additivity(self):
        """Fault: crossers create token rows, join wrong groups, or falsely prove additivity."""
        for grouping in (('repo',), ('provider', 'role'), ('label',)):
            self.put([point(), aggregate(repo='other', provider='other', role='other')])
            r = self.view(group_by=grouping, labels=LABELS)
            self.assertEqual(len(r['groups']), 1)
            self.assertEqual(r['groups'][0]['activity']['agent_hours'], 1)
            self.assertEqual(r['summary']['activity']['known_agent_hours'], 1)
            self.assertFalse(r['group_activity_totals_are_additive'])
        self.put([point(), aggregate()])
        matched = self.view(group_by=('label',), labels=LABELS)
        self.assertEqual(matched['groups'][0]['group'], {'label': 'task'})
        self.assertIsNone(matched['groups'][0]['activity']['agent_hours'])
        self.assertEqual(matched['coverage']['labels']['per_label'], {'task': 1, 'narrow': 0})
        self.put([point(), aggregate(agent='child', repo='other', activity_start=START, activity_end=START+3600, activity_basis='own')])
        r = self.view(group_by=('repo',))
        self.assertEqual(r['summary']['activity']['agent_hours'], 2)
        self.assertFalse(r['group_activity_totals_are_additive'])
        # Maintainer decision 2026-10-25: keep the plan's formula; a same-agent crosser outside every group does not break additivity.
        self.put([point(), aggregate(repo='other', activity_start=START, activity_end=START+3600, activity_basis='own')])
        r = self.view(group_by=('repo',))
        self.assertEqual(r['summary']['activity']['agent_hours'], 1)
        self.assertEqual(len(r['groups']), 1)
        self.assertEqual(r['groups'][0]['group'], {'repo': 'repo'})
        self.assertEqual(r['groups'][0]['activity']['agent_hours'], 1)
        self.assertTrue(r['group_activity_totals_are_additive'])

    def test_12_selection_edges_and_contract(self):
        """Fault: touching, contained, half-open, one-sided or unbounded selection changes."""
        events = [aggregate('left', start=START-100, end=START), aggregate('right', start=START+3600, end=START+3700),
                  aggregate('exact', start=START, end=START+3600), point('end', at=START+3600)]
        chosen, omitted, issues = select(events, self.selection)
        self.assertEqual([e.key for e in chosen], ['exact'])
        self.assertEqual((omitted['outside_window'], omitted['aggregate_crosses_boundary'], issues), (3, 0, []))
        self.assertEqual(select(events, Selection())[0], events)
        for since, until in ((None, START+3600), (START, None), (START, START+3600)):
            crossers = []
            result = select([aggregate()], replace(self.selection, since=since, until=until), crossers=crossers)
            self.assertEqual(len(result), 3)
            self.assertEqual(crossers, [aggregate()])

    def test_14_three_period_middle_missing(self):
        """Fault: an unevidenced aggregate enclosing an entire middle period disappears there."""
        self.put([aggregate(end=START+10900)])
        for method in ('observed', 'buckets'):
            result = series(self.store, replace(self.selection, until=START+10800, activity=method), '1h', 'UTC')
            self.assertEqual(len(result['periods']), 3)
            for period in result['periods']:
                self.assertIsNone(period['summary']['activity']['agent_hours'])
                self.assertEqual(period['summary']['activity']['agents_or_aggregates_missing_activity'], ['root'])
                self.assertEqual(period['omitted']['aggregate_crosses_boundary'], 1)

    def baseline_outputs(self):
        self.put([point(), point('later', at=START+3700, activity_start=START+3600, activity_end=START+7200)])
        for method in ('observed', 'buckets'):
            for labeled in (False, True):
                s = replace(self.selection, until=START+7200, activity=method,
                            labels=LABELS if labeled else None, group_by=('label',) if labeled else ('repo', 'provider', 'role'))
                results = [report(self.store, s), series(self.store, s, '1h', 'UTC'),
                           series(self.store, replace(s, group_by=()), '1h', 'UTC'),
                           compare_periods(self.store, replace(s, until=START+3600), replace(s, since=START+3600))]
                self.put([])
                results.append(report(self.store, s))
                self.put([aggregate(start=START, end=START+3600)])
                results.append(report(self.store, s))  # Contained, missing activity; no crossing.
                self.put([point()], issues=[{'code': 'invalid_json'}])
                results.append(report(self.store, s))
                encoded = [json.dumps(results, indent=2), '\n'.join(markdown(r) for r in results)]
                yield tuple(hashlib.sha256(v.replace(str(self.root), '<TMP>').encode()).hexdigest() for v in encoded)
                self.put([point(), point('later', at=START+3700, activity_start=START+3600, activity_end=START+7200)])

    def test_13_no_crossing_baselines(self):
        """Fault: unrelated JSON/Markdown changes, including labels, grouping, series and compare."""
        self.assertEqual(list(self.baseline_outputs()), BASELINES)


BASELINES = [  # Captured from unchanged main before implementation.
    ('fb86d61b69bf5802ee0229031b6959a22739dfca3d301311cf10e11601174d4d', 'e5a0911e2e484c971f1142f798388c338e4ba29db445aa2b7b44453e1a75457c'),
    ('e99146e2ea1f290cc208b2c8e827340eda6f891cfc73cde5d80f47f0c884d358', '2832044caf73cdddf4a53d4868086e494c8019c03f40ee2c61c0e48e054b4383'),
    ('87928eb76665a1d799d78a0aa0e54a693f2c633e4b22766d7be551c1ec8bdbfa', 'be1a9e72f5379e177f1957c095738a8b738b144c3f82c06994262d8359b69cbb'),
    ('30a3c361ce96e9ba5cb0cf95eaa7088a3c6a41a7333e7c82a0ddf3669bbf5edd', 'c50aac662de8bcb39857596d664584b1a600bb001350921bf87c0be8cc227864'),
]
