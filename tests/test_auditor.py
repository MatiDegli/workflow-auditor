from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from decimal import Decimal
from unittest import mock
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from workflow_auditor.adapters import ingest, parse, path_key
from workflow_auditor.cli import main
from workflow_auditor.model import Event, Tokens, reconcile, timestamp
from workflow_auditor.query import Selection, activity, compare_periods, report, value
from workflow_auditor.store import Store

START = timestamp('2026-10-20T10:00:00Z')
BOOK = {'as_of': '2026-10-22', 'source': 'synthetic test rates, not a provider price',
        'models': {'test': {'unit': 'test_units', 'tiers': [{'input': 1, 'read': .1, 'output': 2}]}}}


def event(key='one', agent='root', amount=100, **kw):
    fields = dict(key=key, client='test', provider='test', provider_basis='fixture', model='test',
                  repo='repo', run='run', agent=agent, parent=None, role='builder', role_basis='fixture',
                  tokens=Tokens(amount, 0, 0, 0), time_kind='point', at=START,
                  activity_start=START, activity_end=START+3600, activity_basis='own')
    fields.update(kw)
    return Event(**fields)


def main_markdown(db):
    return main_markdown_grouped(db,'repo,provider,role')


def main_markdown_grouped(db,group_by):
    out=io.StringIO()
    with redirect_stdout(out):main(['report','--db',str(db),'--format','markdown','--group-by',group_by])
    return out.getvalue()


class AuditorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root/'accounting.sqlite')
        self.store.put_config({'sources': [{'name':'test'}], 'prices':BOOK})

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def put(self, events):
        self.store.replace_source('test','test','fixture','fixture-hash',events,{'issues':[]})
        self.store.set_import_state('complete')

    def file(self, name, rows):
        p=self.root/name
        p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        return p

    def test_user_parallel_work_example(self):
        self.put([event(str(i),agent=str(i)) for i in range(3)])
        first=report(self.store,Selection(activity='observed'))['summary']
        self.put([event(amount=150)])
        second=report(self.store,Selection(activity='observed'))['summary']
        self.assertEqual(first['activity']['agent_hours'],3)
        self.assertEqual(first['activity']['project_active_hours'],1)
        self.assertEqual(second['activity']['agent_hours'],1)
        self.assertAlmostEqual(float(first['list_estimates'][0]['per_agent_hour_min']) /
                               float(second['list_estimates'][0]['per_agent_hour_min']),2/3)

    def test_overlapping_own_activity_merged_parallel_agents_added(self):
        result=activity([event('a'),event('b'),event('c',agent='child')],Selection(activity='observed'))
        self.assertEqual(result['agent_hours'],2)

    def test_buckets_parallelism_and_boundary_clip(self):
        result=activity([event('a'),event('b',agent='child')],Selection(since=START+100,until=START+400))
        self.assertAlmostEqual(result['agent_hours'],600/3600)
        self.assertAlmostEqual(result['project_active_hours'],300/3600)

    def test_agent_role_and_recursive_descendant_filters(self):
        self.put([event('a',role='lead'),event('b',agent='child',parent='root'),
                  event('c',agent='leaf',parent='child',role='reviewer')])
        own=report(self.store,Selection(agents=('root',)))['summary']
        tree=report(self.store,Selection(agents=('root',),descendants=True))['summary']
        reviews=report(self.store,Selection(agents=('root',),descendants=True,roles=('reviewer',)))['summary']
        self.assertEqual((own['events'],tree['events'],reviews['events']),(1,3,1))

    def test_date_intervals_are_half_open(self):
        self.put([event('a',at=START),event('b',at=START+600)])
        a=report(self.store,Selection(since=START,until=START+600))['summary']
        b=report(self.store,Selection(since=START+600,until=START+1200))['summary']
        self.assertEqual((a['events'],b['events']),(1,1))

    def test_aggregate_crossing_boundary_is_withheld(self):
        self.put([event(time_kind='aggregate',start=START-100,end=START+100,agent=None)])
        result=report(self.store,Selection(since=START,until=START+600))
        self.assertEqual(result['coverage']['omitted']['aggregate_crosses_boundary'],1)
        self.assertEqual(result['status'],'partial')
        self.assertIsNone(result['summary']['input_tokens_per_agent_hour'])

    def test_aggregate_duration_not_fabricated_as_agent_hours(self):
        self.put([event(time_kind='aggregate',agent=None,start=START,end=START+3600)])
        result=report(self.store,Selection())['summary']
        self.assertIsNone(result['activity']['agent_hours'])
        self.assertIsNone(result['input_tokens_per_agent_hour'])

    def test_progressive_claude_output_and_cross_source_copies(self):
        self.put([event(tokens=Tokens(100,0,0,16)),event(tokens=Tokens(100,0,0,100))])
        self.store.put_config({'sources':[{'name':'test'},{'name':'copy'}],'prices':BOOK})
        self.store.replace_source('copy','copy','copy','hash',[event(tokens=Tokens(100,0,0,100))],{'issues':[]})
        result=report(self.store,Selection())['summary']
        self.assertEqual(result['events'],1)
        self.assertEqual(result['tokens']['output_total'],100)

    def test_conflicting_observations_hold_rates(self):
        self.put([event(tokens=Tokens(100,0,0,20)),event(tokens=Tokens(80,0,0,30))])
        result=report(self.store,Selection())
        self.assertEqual(result['status'],'partial')
        self.assertIsNone(result['summary']['input_tokens_per_agent_hour'])

    def test_unknown_tokens_and_model_are_not_zero_cost(self):
        self.put([event(model='unknown',tokens=Tokens(None,None,None,None))])
        result=report(self.store,Selection())['summary']
        self.assertIsNone(result['tokens']['input_total'])
        self.assertEqual(result['unpriced_events'],1)
        self.assertIsNone(result['billed_cash'])

    def test_invalid_counts_and_prices_rejected(self):
        for v in (-1,True,1.2,'5'):
            with self.subTest(v=v),self.assertRaises(ValueError):Tokens(v,0,0,1)
        bad={'models':{'test':{'unit':'test','tiers':[{'input':-1,'output':1,'read':1}]}}}
        with self.assertRaises(ValueError):value(event(),bad)

    def test_nested_iterations_and_metadata_role(self):
        p=self.file('agent-child.jsonl',[{'type':'assistant','sessionId':'run','agentId':'child',
            'timestamp':'2026-10-20T10:00:00Z','message':{'id':'r','model':'claude-test',
            'usage':{'input_tokens':0,'output_tokens':0,'iterations':[{'input_tokens':10,'output_tokens':4}]},
            'content':[{'type':'text','text':'SECRET_NOT_ACCOUNTING'}]}}])
        p.with_suffix('.meta.json').write_text(json.dumps({'agentType':'general-purpose','description':'reviewer secret text'}))
        result=parse(p,{'kind':'claude','repo':'repo'},{})
        self.assertEqual(result.events[0].tokens.input_total,10)
        self.assertEqual(result.events[0].role,'unknown')
        self.assertNotIn('SECRET',json.dumps(result.events[0].data()))

    def codex(self, vectors):
        return [{'type':'session_meta','payload':{'id':'child','model_provider':'openai',
            'source':{'subagent':{'thread_spawn':{'parent_thread_id':'root'}}}}},
            {'type':'session_meta','payload':{'id':'root','source':'exec'}},
            {'type':'turn_context','payload':{'model':'gpt-test'}}]+[
            {'type':'event_msg','timestamp':f'2026-10-20T10:0{i}:00Z','payload':{'type':'token_count','info':
            {'total_token_usage':{'input_tokens':n,'cached_input_tokens':0,'cache_write_input_tokens':0,
                                 'output_tokens':n//10,'reasoning_output_tokens':n//20}}}} for i,n in enumerate(vectors)]

    def test_codex_counter_deltas_and_first_identity(self):
        p=self.file('codex.jsonl',self.codex([100,100,200]))
        result=parse(p,{'kind':'codex','repo':'repo'},{})
        self.assertEqual(sum(e.tokens.input_total for e in result.events),200)
        self.assertEqual(len(result.events),2)
        self.assertTrue(all(e.agent=='codex:child' and e.parent=='codex:root' for e in result.events))

    def test_unresolved_counter_reset_visible(self):
        p=self.file('codex.jsonl',self.codex([100,50]))
        result=parse(p,{'kind':'codex','repo':'repo'},{})
        self.assertIn('counter_reset_unresolved',[x['code'] for x in result.issues])

    def test_persistent_reingest_is_idempotent_and_metadata_only(self):
        p=self.file('input.jsonl',[event().data()])
        config={'sources':[{'name':'input','kind':'normalized','roots':[str(p)],'api_key':'SECRET'}],
                'prices':BOOK,'password':'SECRET'}
        ingest(self.store,config)
        first=report(self.store,Selection())
        ingest(self.store,config)
        self.assertEqual(first,report(self.store,Selection()))
        self.assertNotIn('SECRET',json.dumps(self.store.config()))
        self.store.close();self.store=Store(self.root/'accounting.sqlite')
        self.assertEqual(first,report(self.store,Selection()))

    def test_missing_registered_source_and_recovery(self):
        p=self.root/'missing.jsonl'
        config={'sources':[{'name':'new','kind':'normalized','roots':[str(p)]}],'prices':BOOK}
        ingest(self.store,config)
        self.assertEqual(report(self.store,Selection())['status'],'partial')
        p.write_text(json.dumps(event().data())+'\n')
        ingest(self.store,config)
        self.assertEqual(report(self.store,Selection())['status'],'ready')

    def test_bad_json_is_reported_without_discarding_valid_event(self):
        p=self.file('broken.jsonl',[event().data()]);p.write_text(p.read_text()+'{bad\n')
        ingest(self.store,{'sources':[{'name':'broken','kind':'normalized','roots':[str(p)]}]})
        result=report(self.store,Selection())
        self.assertEqual(result['summary']['events'],1)
        self.assertEqual(result['status'],'partial')

    def test_router_is_corroboration_by_default(self):
        p=self.file('usage.jsonl',[{'requestId':'rid','model':'glm-5.3','provider':'zai-coding',
            'at':'2026-10-20T10:00:00Z','inputTokens':100,'cachedInputTokens':0,'outputTokens':10}])
        ingest(self.store,{'sources':[{'name':'router','kind':'router','roots':[str(p)]}]})
        result=report(self.store,Selection())
        self.assertEqual(result['summary']['events'],0)
        self.assertEqual(result['coverage']['corroborating_sources_excluded_from_sums'],1)

    def test_timezone_and_windows_alias_normalization(self):
        self.assertEqual(timestamp('2026-10-20T07:00:00-03:00'),START)
        self.assertEqual(path_key('C:\\Users\\User\\Repo'),path_key('/mnt/c/users/user/repo'))
        with self.assertRaises(ValueError):timestamp('2026-10-20T10:00:00')

    def test_cycle_does_not_loop_and_holds_descendant_claim(self):
        self.put([event('a',agent='a',parent='b'),event('b',agent='b',parent='a')])
        result=report(self.store,Selection(agents=('a',),descendants=True))
        self.assertEqual(result['summary']['events'],2)
        self.assertEqual(result['status'],'partial')

    def test_compare_rejects_different_activity_definitions(self):
        with self.assertRaises(ValueError):compare_periods(self.store,Selection(),Selection(bucket_seconds=300))

    def test_cli_round_trip(self):
        p=self.file('events.jsonl',[event().data()]);config=self.root/'config.json'
        config.write_text(json.dumps({'sources':[{'name':'cli','kind':'normalized','roots':[str(p)]}]}))
        db=self.root/'cli.sqlite';output=self.root/'report.json'
        self.assertEqual(main(['ingest','--config',str(config),'--db',str(db)]),0)
        self.assertEqual(main(['report','--db',str(db),'--activity','observed','--output',str(output)]),0)
        self.assertEqual(json.loads(output.read_text())['summary']['activity']['agent_hours'],1)

    def test_interrupted_import_holds_complete_rate_claim(self):
        self.put([event()]);self.store.set_import_state('running')
        result=report(self.store,Selection())
        self.assertEqual(result['status'],'partial')
        self.assertIsNone(result['summary']['input_tokens_per_agent_hour'])

    def test_zai_routed_codex_keeps_client_distinct_and_sums_hours(self):
        raw=self.codex([100,200]);raw[0]['payload']['model_provider']='zai-coding'
        raw[2]['payload']['model']='zai-coding/glm-5.3'
        p=self.file('zai.jsonl',raw)
        parsed=parse(p,{'kind':'codex','repo':'repo'},{})
        self.assertTrue(all(e.client=='codex' and e.provider=='z.ai' for e in parsed.events))
        self.assertAlmostEqual(activity(parsed.events,Selection())['agent_hours'],1/6)

    def test_model_change_on_one_agent_does_not_multiply_global_hours(self):
        self.put([event('a'),event('b',model='other')])
        result=report(self.store,Selection(group_by=('model',)))
        self.assertAlmostEqual(result['summary']['activity']['agent_hours'],1/6)
        self.assertAlmostEqual(sum(x['activity']['agent_hours'] for x in result['groups']),1/3)
        self.assertTrue(any('may overlap' in x for x in result['notes']))

    def test_aggregate_and_detail_overlap_is_held_instead_of_double_counted(self):
        self.put([event('detail'),event('total',time_kind='aggregate',agent=None)])
        result=report(self.store,Selection())
        self.assertEqual(result['coverage']['withheld_overlap_events'],2)
        self.assertIsNone(result['summary']['tokens']['input_total'])
        self.assertEqual(result['status'],'partial')
        own=report(self.store,Selection(agents=('root',)))
        self.assertEqual(own['summary']['tokens']['input_total'],100)

    def test_report_keeps_import_state_in_same_read_snapshot(self):
        self.store.db.execute('PRAGMA journal_mode=WAL')
        self.put([event()]);self.store.set_import_state('running')
        original_events=self.store.events
        writer=Store(self.root/'accounting.sqlite')
        def finish_after_read():
            values=original_events()
            writer.set_import_state('complete')
            return values
        self.store.events=finish_after_read
        try:
            result=report(self.store,Selection())
            self.assertEqual(result['status'],'partial')
            self.assertEqual(result['coverage']['import_state']['state'],'running')
            self.assertEqual(writer.import_state()['state'],'complete')
        finally:
            writer.close()

    def test_role_change_in_one_bucket_does_not_make_group_hours_additive(self):
        self.put([event('a',role='builder'),event('b',role='reviewer')])
        result=report(self.store,Selection(group_by=('role',)))
        self.assertFalse(result['group_activity_totals_are_additive'])
        self.assertAlmostEqual(result['summary']['activity']['agent_hours'],1/6)

    def test_bad_accounting_role_does_not_silently_exclude_sources(self):
        with self.assertRaises(ValueError):
            ingest(self.store,{'sources':[{'name':'bad','kind':'router','accounting_role':'primray'}]})

    def test_list_estimate_cache_ttl_reasoning_and_unknown_ttl_range(self):
        book={'models':{'test':{'unit':'test_units','tiers':[{
            'input':4,'read':.2,'write_5m':5,'write_1h':8,'output':20}]}}}
        t=Tokens(160,50,10,7,reasoning=2,write_5m=4,write_1h=6)
        unit,lo,hi=value(event(tokens=t),book)
        self.assertEqual((unit,lo,hi),('test_units',Decimal('0.000618'),Decimal('0.000618')))
        unit,lo,hi=value(event(tokens=Tokens(160,50,10,7)),book)
        self.assertEqual((lo,hi),(Decimal('0.000600'),Decimal('0.000630')))

    def test_stream_final_totals_include_helper_once_and_missing_final_is_partial(self):
        rows=[{'type':'system','subtype':'init','session_id':'sid'},
              {'type':'assistant','message':{'usage':{'input_tokens':99999,'output_tokens':99999}}},
              {'type':'result','session_id':'sid','total_cost_usd':.3,'modelUsage':{
                  'claude-main':{'inputTokens':100,'cacheReadInputTokens':20,'outputTokens':10,'costUSD':.2},
                  'claude-helper':{'inputTokens':40,'outputTokens':5,'costUSD':.1}}}]
        p=self.file('stream.jsonl',rows)
        parsed=parse(p,{'kind':'claude_stream','repo':'repo'},{})
        self.assertEqual(sum(e.tokens.input_total for e in parsed.events),160)
        self.assertEqual(sum(e.tokens.output_total for e in parsed.events),15)
        self.assertEqual(sum(Decimal(e.cli_usd) for e in parsed.events),Decimal('.3'))
        self.assertFalse(parsed.issues)
        p=self.file('unfinished.jsonl',rows[:-1])
        ingest(self.store,{'sources':[{'name':'unfinished','kind':'claude_stream','roots':[str(p)]}]})
        self.assertEqual(report(self.store,Selection())['status'],'partial')

    def test_new_snapshot_reflects_changes_after_previous_report(self):
        self.put([event(amount=100)])
        self.assertEqual(report(self.store,Selection())['summary']['tokens']['input_total'],100)
        self.put([event(amount=200)])
        self.assertEqual(report(self.store,Selection())['summary']['tokens']['input_total'],200)

    def test_main_session_filename_fallback_keeps_parallel_sessions_distinct(self):
        rows=[{'type':'assistant','timestamp':'2026-10-20T10:00:00Z',
               'message':{'id':'one','model':'claude-test','usage':{'input_tokens':10,'output_tokens':1}}}]
        a=parse(self.file('session-a.jsonl',rows),{'kind':'claude','repo':'repo'},{})
        b=parse(self.file('session-b.jsonl',rows),{'kind':'claude','repo':'repo'},{})
        self.assertNotEqual(a.events[0].agent,b.events[0].agent)
        self.assertAlmostEqual(activity(a.events+b.events,Selection())['agent_hours'],1/3)

    def test_normalized_bad_agent_identifier_is_quarantined(self):
        data=event().data();data['agent']=123
        path=self.file('bad-id.jsonl',[data])
        ingest(self.store,{'sources':[{'name':'bad-id','kind':'normalized','roots':[str(path)]}]})
        result=report(self.store,Selection())
        self.assertEqual(result['summary']['events'],0)
        self.assertEqual(result['status'],'partial')

    def claude_row(self, mid, at, sid, output=5, cwd=None):
        row={'type':'assistant','timestamp':at,'sessionId':sid,'requestId':'req-'+mid,
             'message':{'id':mid,'model':'claude-test','usage':{'input_tokens':10,'cache_creation_input_tokens':0,
                        'cache_read_input_tokens':0,'output_tokens':output}}}
        if cwd:row['cwd']=cwd
        return row

    def test_claude_rows_without_response_identity_are_a_visible_issue(self):
        rows=[self.claude_row('m1','2026-10-20T12:00:00Z','s1',output=o) for o in (3,50)]
        for i,row in enumerate(rows):del row['message']['id'],row['requestId'];row['uuid']=f'u{i}'
        rows.append(self.claude_row('m2','2026-10-20T12:05:00Z','s1'));del rows[-1]['message']['id']
        self.file('s1.jsonl',rows)
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root)],'pattern':'s*.jsonl','repo':'repo'}],'prices':BOOK})
        result=report(self.store,Selection())
        self.assertEqual(result['status'],'partial')
        self.assertEqual([(i['code'],i['line']) for i in result['coverage']['source_issues']],
                         [('missing_response_identity',1),('missing_response_identity',2)])
        self.assertEqual((result['summary']['events'],result['summary']['known_token_subtotals']['output_total']),(1,5))

    def test_resumed_copy_is_owned_by_earliest_observation_without_breaking_trust(self):
        self.file('s1.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1'),
                              self.claude_row('m2','2026-10-20T12:20:00Z','s1')])
        self.file('s2.jsonl',[self.claude_row('m1','2026-10-20T13:00:00Z','s2'),
                              self.claude_row('m2','2026-10-20T13:00:00Z','s2'),
                              self.claude_row('m3','2026-10-20T13:10:00Z','s2')])
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root)],'pattern':'s*.jsonl','repo':'repo'}],'prices':BOOK})
        result=report(self.store,Selection(repos=('repo',)))
        self.assertEqual(result['status'],'ready_with_missing_metrics')  # claude-test is not in BOOK
        self.assertEqual((result['summary']['events'],result['coverage']['conflicts']),(3,[]))
        self.assertEqual(result['coverage']['copied_responses_reattributed'],2)
        self.assertEqual(result['coverage']['unattributed_repo_events'],0)
        self.assertAlmostEqual(result['summary']['activity']['agent_hours'],0.5)
        self.assertIn('copied responses reattributed: 2.',main_markdown(self.root/'accounting.sqlite'))
        m1=next(e for e in self.store.events()[0] if e.at==timestamp('2026-10-20T12:00:00Z'))
        self.assertEqual((m1.run,m1.agent),('s1','claude:s1:main'))

    def test_cross_session_copy_with_different_usage_is_a_conflict(self):
        self.file('s1.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1',output=3)])
        self.file('s2.jsonl',[self.claude_row('m1','2026-10-20T13:00:00Z','s2',output=50)])
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root)],'pattern':'s*.jsonl','repo':'repo'}]})
        result=report(self.store,Selection())
        self.assertEqual((result['status'],[c['code'] for c in result['coverage']['conflicts']]),('partial',['attribution_conflict']))

    def test_one_agents_blocks_under_two_cwds_merge_as_a_copy(self):
        self.file('s1.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1',output=3,cwd='/work/a'),
                              self.claude_row('m1','2026-10-20T12:00:01Z','s1',output=50,cwd='/work/a/gone')])
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root/'s1.jsonl')]}],
                           'repo_aliases':{'/work/a':'repo-a','/work/a/gone':'repo-gone'}})
        result=report(self.store,Selection())
        self.assertEqual((result['status'],result['coverage']['copied_responses_reattributed']),('ready_with_missing_metrics',1))
        e=self.store.events()[0][0]
        self.assertEqual((e.repo,e.tokens.output_total),('repo-a',50))

    def test_colliding_keys_outside_claude_transcripts_stay_conflicts(self):
        late='2026-10-20T10:30:00Z'
        cases={'equal usage':[event(run='ra',agent='a'),event(run='rb',agent='b',at=timestamp(late))],
               'cumulative usage':[event(run='ra',agent='a',tokens=Tokens(10,0,0,5)),
                                   event(run='rb',agent='b',at=timestamp(late),tokens=Tokens(900,0,0,400))],
               'two repos':[event(repo='repo-a'),event(repo='repo-b')]}
        for name,rows in cases.items():
            with self.subTest(name):
                p=self.file(name.replace(' ','-')+'.jsonl',[e.data() for e in rows])
                ingest(self.store,{'sources':[{'name':'n','kind':'normalized','roots':[str(p)]}],'prices':BOOK})
                result=report(self.store,Selection(repos=('repo-b',) if name=='two repos' else ()))
                self.assertEqual(result['status'],'partial')
                self.assertEqual([i['code'] for i in result['coverage']['source_issues']],['attribution_conflict'])

    def test_aggregate_copies_with_different_intervals_conflict_in_any_order(self):
        def agg(end,out):
            return event('k',agent=None,time_kind='aggregate',at=None,start=START,end=START+end,tokens=Tokens(100,0,0,out),
                         activity_start=None,activity_end=None,activity_basis=None)
        short,longer=agg(1800,10),agg(5400,200)
        for copies in (False,True):
            self.assertEqual(reconcile([short,longer],copies),reconcile([longer,short],copies))
            self.assertEqual(reconcile([short,longer],copies)[1],['attribution_conflict'])
        self.assertEqual(reconcile([short,agg(1800,10)],True),(short,[]))

    def test_copies_that_disagree_on_speed_or_geo_are_kept_unpriced_without_breaking_trust(self):
        book={**BOOK,'models':{'claude-test':BOOK['models']['test']}}
        def run(speeds,layout):
            d=self.root/f'{layout}-{"-".join(speeds)}';d.mkdir()
            rows=[self.claude_row('m1','2026-10-20T12:00:00Z',sid) for sid in ('zz-original','aa-resumed')]
            for row,speed in zip(rows,speeds):row['message']['usage']['speed']=speed
            rows[0:0]=[self.claude_row('m0','2026-10-20T11:30:00Z','zz-original')]
            if layout=='one session':
                rows[2]['sessionId']='zz-original';(d/'s.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
            else:
                for sid in ('zz-original','aa-resumed'):(d/f'{sid}.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows if r['sessionId']==sid))
            store=Store(d/'db.sqlite')
            try:
                ingest(store,{'sources':[{'name':'c','kind':'claude','roots':[str(d)],'repo':'repo'}],'prices':book})
                return report(store,Selection())
            finally:store.close()
        for layout in ('two sessions','one session'):
            same=run(('standard','standard'),layout)
            for speeds in (('fast','standard'),('standard','fast')):
                with self.subTest(layout=layout,speeds=speeds):
                    result=run(speeds,layout)
                    summary,coverage=result['summary'],result['coverage']
                    self.assertEqual((result['status'],coverage['conflicts'],coverage['source_issues']),('ready_with_missing_metrics',[],[]))
                    self.assertEqual((summary['events'],summary['unpriced_events']),(2,1))
                    self.assertEqual([[Path(s).name for s in m['sources']] for m in coverage['speed_geo_mismatches']],
                                     [['s.jsonl'] if layout=='one session' else ['aa-resumed.jsonl','zz-original.jsonl']])
                    for field in ('activity','tokens'):self.assertEqual(summary[field],same['summary'][field])
                    self.assertEqual(summary['list_estimates'][0]['known_min'],'0.00002')  # m0 alone keeps its price
        self.assertEqual(reconcile([event(geo='us'),event(geo='global')],True),(event(geo='mismatch'),['speed_geo_mismatch']))
        self.assertIsNone(value(event(model='test',client='claude_code',speed='mismatch'),BOOK))

    def test_a_model_difference_between_copies_still_breaks_trust(self):
        rows=[self.claude_row('m1','2026-10-20T12:00:00Z',sid) for sid in ('s1','s2')]
        rows[1]['message']['model']='claude-other';rows[1]['message']['usage']['speed']='fast'
        for row in rows:self.file(f"{row['sessionId']}.jsonl",[row])
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root)],'pattern':'s*.jsonl','repo':'repo'}]})
        result=report(self.store,Selection())
        self.assertEqual((result['status'],[c['code'] for c in result['coverage']['conflicts']]),('partial',['identity_conflict']))
        self.assertEqual(result['coverage']['speed_geo_mismatches'],[])

    def test_a_known_speed_or_geo_survives_a_copy_without_it_in_any_order(self):
        book={**BOOK,'models':{'claude-test':BOOK['models']['test']}}
        prices=[]
        for fast_run,plain_run in (('aa-fast','zz-plain'),('zz-fast','aa-plain')):
            with self.subTest(owner=min(fast_run,plain_run)):
                d=self.root/fast_run;d.mkdir()
                fast=self.claude_row('m1','2026-10-20T12:00:00Z',fast_run);fast['message']['usage'].update(speed='fast',inference_geo='us')
                for sid,row in ((fast_run,fast),(plain_run,self.claude_row('m1','2026-10-20T12:00:00Z',plain_run))):
                    (d/f'{sid}.jsonl').write_text(json.dumps(row)+'\n')
                store=Store(d/'db.sqlite')
                try:
                    ingest(store,{'sources':[{'name':'c','kind':'claude','roots':[str(d)],'repo':'repo'}],'prices':book})
                    result=report(store,Selection());e=store.events()[0][0]
                finally:store.close()
                prices.append(result['summary']['list_estimates'][0]['known_min'])
                self.assertEqual((result['status'],e.run,e.speed,e.geo),('ready',min(fast_run,plain_run),'fast','us'))
        self.assertEqual(prices,[str(Decimal('0.00002')*2*Decimal('1.1'))]*2)  # (10 input + 5 output x 2) per M, x2 fast, x1.1 us
        fast,plain=event(speed='fast',run='b',agent='b'),event(run='a',agent='a')
        self.assertEqual(reconcile([fast,plain],True)[0].speed,'fast')
        self.assertEqual(reconcile([plain,fast],True),reconcile([fast,plain],True))
        standard=event(speed='standard',run='c',agent='c')  # Known against known is a mismatch behind an unknown owner too.
        self.assertEqual(reconcile([plain,fast,standard],True),
                         (replace(plain,speed='mismatch'),['copied_response_reattributed','speed_geo_mismatch']))

    def modifier_copies(self, first, second):
        """Two sessions holding one Claude response at one time, with these usage fields; aa-one owns it."""
        d=Path(tempfile.mkdtemp(dir=self.root))
        for sid,fields in (('aa-one',first),('zz-two',second)):
            row=self.claude_row('m1','2026-10-20T12:00:00Z',sid);row['message']['usage'].update(fields)
            (d/f'{sid}.jsonl').write_text(json.dumps(row)+'\n')
        store=Store(d/'db.sqlite')
        try:
            ingest(store,{'sources':[{'name':'c','kind':'claude','roots':[str(d)],'repo':'repo'}],
                          'prices':{**BOOK,'models':{'claude-test':BOOK['models']['test']}}})
            return report(store,Selection())
        finally:store.close()

    def test_an_explicit_empty_speed_or_geo_is_the_standard_value(self):
        p=self.file('s1.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1')])
        for fields,expected in (({'speed':'','inference_geo':''},('standard','global')),({},('unknown','unknown')),
                                ({'speed':None,'inference_geo':None},('unknown','unknown'))):
            with self.subTest(fields=fields):
                row=self.claude_row('m1','2026-10-20T12:00:00Z','s1');row['message']['usage'].update(fields)
                p.write_text(json.dumps(row)+'\n')
                e=parse(p,{'kind':'claude','repo':'repo'},{}).events[0]
                self.assertEqual((e.speed,e.geo),expected)
        for empty,standard in (({'speed':''},{'speed':'standard'}),({'inference_geo':''},{'inference_geo':'global'})):
            for pair in ((empty,standard),(standard,empty)):
                with self.subTest(pair=pair):
                    result=self.modifier_copies(*pair)
                    self.assertEqual((result['status'],result['coverage']['speed_geo_mismatches']),('ready',[]))
                    self.assertEqual(result['summary']['list_estimates'][0]['known_min'],'0.00002')

    def test_an_explicit_empty_speed_or_geo_against_fast_or_us_is_a_mismatch(self):
        for empty,special in (({'speed':''},{'speed':'fast'}),({'inference_geo':''},{'inference_geo':'us'})):
            for pair in ((empty,special),(special,empty)):
                with self.subTest(pair=pair):
                    result=self.modifier_copies(*pair)
                    self.assertEqual((result['status'],result['coverage']['conflicts']),('ready_with_missing_metrics',[]))
                    self.assertEqual((result['summary']['unpriced_events'],len(result['coverage']['speed_geo_mismatches'])),(1,1))
                    self.assertEqual(result['summary']['activity']['agent_hours'],1/6)

    def resumed_pair(self, original, resumed):
        rows=[self.claude_row(m,f'2026-10-20T12:{s}:00Z',original) for m,s in (('m1','00'),('m2','05'),('m3','09'),('m4','15'))]
        copies=[self.claude_row(m,f'2026-10-20T12:{s}:00Z',resumed) for m,s in (('m3','09'),('m4','15'),('m5','30'))]
        return rows,copies  # The resumed session re-stamps m3 and m4 with its own ID and the same timestamps.

    def test_resumed_session_with_smaller_id_does_not_own_the_original_responses(self):
        rows,copies=self.resumed_pair('zz-original','aa-resumed')
        layouts={'two files':{'zz-original.jsonl':rows,'aa-resumed.jsonl':copies},'one file':{'mixed.jsonl':copies+rows}}
        results=[]
        for name,files in layouts.items():
            with self.subTest(name):
                d=self.root/name.replace(' ','-');d.mkdir()
                for f,r in files.items():(d/f).write_text(''.join(json.dumps(x)+'\n' for x in r))
                store=Store(d/'db.sqlite')
                try:
                    ingest(store,{'sources':[{'name':'c','kind':'claude','roots':[str(d)],'repo':'repo'}]})
                    result=report(store,Selection(group_by=('run',)))
                finally:store.close()
                hours=result['summary']['activity']
                self.assertEqual({g['group']['run']:g['events'] for g in result['groups']},{'zz-original':4,'aa-resumed':1})
                self.assertEqual((hours['agent_hours'],hours['parallelism'],result['coverage']['copied_responses_reattributed']),(0.5,1.0,2))
                self.assertEqual(result['coverage']['conflicts'],[])
                results.append((result['groups'],result['summary']))
        self.assertEqual(results[0],results[1])

    def test_tie_goes_to_own_activity_then_to_the_smallest_attribution(self):
        a,b=event(run='a-run',agent='a'),event(run='b-run',agent='b')
        for own,winner in (({'a-run':START+60,'b-run':START+30},'b-run'),({'b-run':START+900},'b-run'),({},'a-run')):
            with self.subTest(own=own):
                start=lambda run,rivals:own.get(run)
                self.assertEqual(reconcile([a,b],True,start),reconcile([b,a],True,start))
                self.assertEqual(reconcile([a,b],True,start)[0].run,winner)
        self.assertEqual(reconcile([a,replace(b,at=START-1)],True,lambda run,rivals:START)[0].run,'b-run')  # time first

    def test_copies_under_two_runs_in_one_file_are_settled_at_read_time(self):
        self.file('mixed.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','new',cwd='/b'),
                                 self.claude_row('m1','2026-10-20T11:00:00Z','old',cwd='/a')])
        ingest(self.store,{'sources':[{'name':'c','kind':'claude','roots':[str(self.root/'mixed.jsonl')],'repo':'repo'}]})
        result=report(self.store,Selection())
        self.assertEqual((result['status'],result['coverage']['source_issues']),('ready_with_missing_metrics',[]))
        self.assertEqual(result['coverage']['copied_responses_reattributed'],1)
        self.assertEqual(self.store.events()[0][0].run,'old')

    def test_copy_owner_tie_break_ignores_order_and_identity_conflicts_still_break_trust(self):
        a,b=event(run='b-run',agent='b'),event(run='a-run',agent='a')
        for copies in (False,True):
            self.assertEqual(reconcile([a,b],copies),reconcile([b,a],copies))
            self.assertEqual(reconcile([a,b],copies)[0].run,'a-run')
        self.put([event(),event(model='other')])
        self.assertEqual(report(self.store,Selection())['status'],'partial')

    def sessions(self, own, copies, cwd=None):
        """own: session -> [(response, minute after 10:00)]; copies: session -> responses it copied, which
        keep their original time but carry the copier's session ID. The copied block comes first, as on resume."""
        at={m:t for s in own for m,t in own[s]}
        row=lambda m,s:self.claude_row(m,f'2026-10-20T10:{at[m]:02d}:00Z',s,cwd=(cwd or {}).get(s,'/w/repo-a'))
        return {s:[row(m,s) for m in copies.get(s,[])]+[row(m,s) for m,_ in own[s]] for s in own}

    def copy_layouts(self, files):
        """Ingest the same rows under four layouts and orders; return the summary, equal in all of them."""
        names,base=list(files),Path(tempfile.mkdtemp(dir=self.root))
        rows={**files,'mixed':[x for s in names for x in files[s]][::-1]}
        layouts={'one file per session':[[names]],'one mixed file, rows reversed':[[['mixed']]],  # stages, registrations, files
                 'one registration per file, reversed':[[[s] for s in names[::-1]]],'staged, last session first':[[names[-1:]],[names]]}
        found={}
        for layout,stages in layouts.items():
            d=base/layout.replace(' ','-').replace(',','');d.mkdir()
            store=Store(d/'db.sqlite')
            try:
                for stage in stages:
                    for s in {s for registration in stage for s in registration}:
                        (d/f'{s}.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in rows[s]))
                    ingest(store,{'sources':[{'name':f'c{i}','kind':'claude','roots':[str(d/f'{s}.jsonl') for s in registration]}
                                             for i,registration in enumerate(stage)],
                                  'repo_aliases':{'/w/repo-a':'repo-a','/w/repo-c':'repo-c'}})
                by_run,by_repo=(report(store,Selection(group_by=(g,))) for g in ('run','repo'))
            finally:store.close()
            hours=by_run['summary']['activity']
            found[layout]={'status':by_run['status'],'conflicts':by_run['coverage']['conflicts'],
                           'runs':{g['group']['run']:g['events'] for g in by_run['groups']},
                           'repo output':{g['group']['repo']:g['tokens']['output_total'] for g in by_repo['groups']},
                           'hours':(round(hours['agent_hours'],4),round(hours['project_active_hours'],4))}
        for layout,result in found.items():
            with self.subTest(layout=layout):self.assertEqual(result,found['one file per session'])
        return found['one file per session']

    def test_full_copy_chain_keeps_each_response_with_the_session_that_produced_it(self):
        own={'zz-a':[('a1',0),('a2',10),('a3',20)],'mm-b':[('b1',30),('b2',40)],'aa-c':[('c1',50)]}
        result=self.copy_layouts(self.sessions(own,{'mm-b':['a1','a2','a3'],'aa-c':['a1','a2','a3','b1','b2']}))
        self.assertEqual(result['runs'],{'zz-a':3,'mm-b':2,'aa-c':1})  # The tuple alone would give aa-c all 6.
        self.assertEqual((result['status'],result['conflicts'],result['hours']),('ready_with_missing_metrics',[],(1.0,1.0)))

    def test_a_full_copy_pair_leaves_the_original_its_responses(self):
        own={'zz-original':[('m1',0),('m2',10)],'aa-resumed':[('m3',20)]}
        result=self.copy_layouts(self.sessions(own,{'aa-resumed':['m1','m2']}))
        self.assertEqual(result['runs'],{'zz-original':2,'aa-resumed':1})

    def test_handoffs_inside_a_bucket_count_each_agent_as_the_contract_says(self):
        # Buckets: zz-a {10:00}, mm-b {10:00, 10:10}, aa-c {10:10}: four agent-buckets over two project buckets.
        own={'zz-a':[('a1',0),('a2',6)],'mm-b':[('b1',8),('b2',16)],'aa-c':[('c1',18)]}
        result=self.copy_layouts(self.sessions(own,{'mm-b':['a1','a2'],'aa-c':['a1','a2','b1','b2']}))
        self.assertEqual(result['hours'],(0.6667,0.3333))  # aa-c owning all five would give 0.3333 agent-hours.
        self.assertEqual(result['runs'],{'zz-a':2,'mm-b':2,'aa-c':1})

    def test_a_copier_in_another_repo_does_not_take_the_original_repos_tokens(self):
        own={'zz-a':[('a1',0),('a2',10),('a3',20)],'mm-b':[('b1',30),('b2',40)],'aa-c':[('c1',50)]}
        files=self.sessions(own,{'mm-b':['a1','a2','a3'],'aa-c':['a1','a2','a3','b1','b2']},cwd={'aa-c':'/w/repo-c'})
        self.assertEqual(self.copy_layouts(files)['repo output'],{'repo-a':25,'repo-c':5})

    def test_a_copy_that_adds_nothing_is_undecidable_and_falls_to_the_smallest_tuple(self):
        """Regression guard: two runs holding the same responses tie on time, containment and own activity
        (neither has any), so the smallest run ID owns them, in every layout. This also passes before D9."""
        for original,copier in (('aa-original','zz-copy'),('zz-original','aa-copy')):
            with self.subTest(original=original):
                own={original:[('m1',0),('m2',10)],copier:[]}
                result=self.copy_layouts(self.sessions(own,{copier:['m1','m2']}))
                self.assertEqual(result['runs'],{min(original,copier):2})

    def test_a_tail_copy_that_adds_nothing_leaves_the_original_all_its_responses(self):
        for original,resumed in (('zz-original','aa-resumed'),('aa-original','zz-resumed')):
            with self.subTest(original=original):
                own={original:[('m1',0),('m2',10),('m3',20),('m4',30)],resumed:[]}
                result=self.copy_layouts(self.sessions(own,{resumed:['m3','m4']}))
                self.assertEqual(result['runs'],{original:4})  # The resumed set is a strict subset, but of a tail.

    def test_a_copier_may_continue_from_the_copied_runs_latest_time_but_not_before_it(self):
        # zz-a holds a1 and a2; mm-b copies both and adds b1 at a2's time (it continued) or a minute
        # before it (then zz-a is a tail copy of mm-b, which has own activity and owns all three).
        for minute,runs in ((10,{'zz-a':2,'mm-b':1}),(9,{'mm-b':3})):
            with self.subTest(b1=minute):
                own={'zz-a':[('a1',0),('a2',10)],'mm-b':[('b1',minute)]}
                self.assertEqual(self.copy_layouts(self.sessions(own,{'mm-b':['a1','a2']}))['runs'],runs)

    def test_a_tail_chain_leaves_the_middle_session_its_own_responses(self):
        # X copies W's last response and adds x1, x2; Y copies x1, x2 and adds y1. X holds nothing that
        # no other run holds, but against Y alone it holds w1, which comes before y1.
        for w,x,y in (('zz-w','mm-x','aa-y'),('aa-w','mm-x','zz-y')):
            with self.subTest(y=y):
                own={w:[('w0',0),('w1',10)],x:[('x1',20),('x2',30)],y:[('y1',40)]}
                result=self.copy_layouts(self.sessions(own,{x:['w1'],y:['x1','x2']}))
                self.assertEqual(result['runs'],{w:2,x:2,y:1})

    def test_own_activity_is_measured_against_the_runs_still_contending(self):
        # X copies all of Z and continues (step 2 drops it); W copies Z's last response and continues.
        # Against W alone, Z's own activity starts at z1, before w1: z3 stays with Z.
        own={'zz-z':[('z1',0),('z2',10),('z3',20)],'mm-x':[('x1',30)],'aa-w':[('w1',40)]}
        result=self.copy_layouts(self.sessions(own,{'mm-x':['z1','z2','z3'],'aa-w':['z3']}))
        self.assertEqual(result['runs'],{'zz-z':3,'mm-x':1,'aa-w':1})

    def test_partial_selection_withholds_activity_totals_everywhere(self):
        p=self.file('busy.jsonl',[event(str(i),agent=str(i)).data() for i in range(3)])
        ingest(self.store,{'sources':[{'name':'busy','kind':'normalized','roots':[str(p)]},
                                      {'name':'gone','kind':'normalized','roots':[str(self.root/'missing')]}],'prices':BOOK})
        result=report(self.store,Selection(activity='observed',group_by=('agent',)))
        self.assertEqual(result['status'],'partial')
        for hours in [result['summary']['activity']]+[g['activity'] for g in result['groups']]:
            self.assertEqual((hours['agent_hours'],hours['project_active_hours'],hours['parallelism']),(None,None,None))
        self.assertEqual((result['summary']['activity']['known_agent_hours'],result['summary']['activity']['known_project_active_hours']),(3,1))
        self.assertEqual([g['activity']['known_agent_hours'] for g in result['groups']],[1,1,1])
        self.assertIsNone(result['summary']['input_tokens_per_agent_hour'])
        db,out=str(self.root/'accounting.sqlite'),self.root/'out.md'
        self.assertEqual(main(['report','--db',db,'--activity','observed','--format','markdown','--output',str(out)]),0)
        self.assertIn('Agent-hours: unknown; known subtotal: 3.000000.',out.read_text())
        self.assertIn('Project activity hours: unknown; parallelism: unknown.',out.read_text())
        periods=['--before-start','2026-10-20T09:00:00Z','--before-end','2026-10-20T11:00:00Z',
                 '--after-start','2026-10-20T11:00:00Z','--after-end','2026-10-20T13:00:00Z']
        self.assertEqual(main(['compare','--db',db,'--activity','observed',*periods,'--output',str(out)]),0)
        changes=json.loads(out.read_text())['changes']
        self.assertEqual(changes['input_tokens_per_agent_hour'],{'before':None,'after':None,'change_percent':None})
        self.assertEqual(main(['compare','--db',db,'--activity','observed',*periods,'--format','markdown','--output',str(out)]),0)
        self.assertIn('Agent-hours: unknown',out.read_text())

    def test_known_zero_usage_is_priced_at_zero_whatever_the_model(self):
        self.put([event('paid'),event('zero',model='<synthetic>',tokens=Tokens(0,0,0,0))])
        result=report(self.store,Selection(activity='observed'))
        quote=result['summary']['list_estimates'][0]
        self.assertEqual((result['status'],result['summary']['unpriced_events'],result['summary']['zero_usage_events']),('ready',0,1))
        self.assertEqual((quote['known_min'],quote['priced_events'],quote['complete_for_selection']),('0.0001',1,True))
        self.assertEqual(Decimal(quote['per_agent_hour_min']),Decimal('0.0001'))
        self.put([event('paid'),event('zero',model='<synthetic>',tokens=Tokens(0,0,None,0))])
        result=report(self.store,Selection(activity='observed'))
        self.assertEqual((result['status'],result['summary']['unpriced_events']),('ready_with_missing_metrics',1))
        self.assertIsNone(result['summary']['list_estimates'][0]['per_agent_hour_min'])

    def test_zero_usage_counts_once_and_renders_as_zero(self):
        book={'models':{'test':BOOK['models']['test'],'other':{'unit':'other_units','tiers':[{'input':1,'read':0,'output':1}]}}}
        self.put([event('a'),event('b',model='other'),event('z',model='<synthetic>',tokens=Tokens(0,0,0,0))])
        summary=report(self.store,Selection(),book)['summary']
        self.assertEqual([(q['unit'],q['priced_events']) for q in summary['list_estimates']],[('other_units',1),('test_units',1)])
        self.assertEqual(summary['zero_usage_events'],1)
        self.store.put_config({'sources':[{'name':'test'}],'prices':BOOK})
        self.assertIn('| model=<synthetic> | 1 | 0.166667 | 0 | 0 | 0 (zero usage) | 0 (zero usage) |',
                      main_markdown_grouped(self.root/'accounting.sqlite','model'))
        self.put([event('z',model='<synthetic>',tokens=Tokens(0,0,0,0))])
        self.assertIn('- 0 (zero usage).',main_markdown(self.root/'accounting.sqlite'))

    def test_failed_source_keeps_its_accounting_role(self):
        p=self.file('events.jsonl',[event().data()]);missing=str(self.root/'no-router')
        for role,status,unavailable in ((None,'ready',0),('corroborating','ready',0),('primary','partial',1)):
            with self.subTest(role=role):
                router={'name':'router','kind':'router','roots':[missing],**({'accounting_role':role} if role else {})}
                ingest(self.store,{'sources':[{'name':'n','kind':'normalized','roots':[str(p)]},router],'prices':BOOK})
                result=report(self.store,Selection(activity='observed'))
                self.assertEqual((result['status'],result['coverage']['unavailable_primary_sources']),(status,unavailable))
        empty=self.root/'empty';empty.mkdir()
        ingest(self.store,{'sources':[{'name':'n','kind':'normalized','roots':[str(p)]},{'name':'router','kind':'router','roots':[str(empty)]}],'prices':BOOK})
        self.assertEqual(report(self.store,Selection(activity='observed'))['status'],'ready')

    def test_disappeared_file_takes_the_registrations_current_role(self):
        d=self.root/'router';d.mkdir()
        gone=self.file('router/a.jsonl',[{'requestId':'q1','model':'m','at':'2026-10-20T10:00:00Z','inputTokens':1,'outputTokens':1}])
        router={'name':'router','kind':'router','roots':[str(d)]}
        ingest(self.store,{'sources':[{**router,'accounting_role':'primary'}]})
        gone.unlink();self.file('router/b.jsonl',[{'requestId':'q2','model':'m','at':'2026-10-20T10:00:00Z','inputTokens':1,'outputTokens':1}])
        ingest(self.store,{'sources':[{**router,'accounting_role':'corroborating'}]})
        result=report(self.store,Selection())
        self.assertEqual((result['status'],result['coverage']['unavailable_primary_sources']),('ready',0))
        ingest(self.store,{'sources':[{**router,'accounting_role':'primary'}]})
        self.assertEqual(report(self.store,Selection())['coverage']['unavailable_primary_sources'],1)

    def test_non_object_sidecar_or_report_is_a_file_issue_not_a_crash(self):
        sub=self.root/'s1'/'subagents';sub.mkdir(parents=True)
        p=self.file('s1/subagents/agent-x.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1')])
        p.with_suffix('.meta.json').write_text('[1,2]')
        (self.root/'attempt').mkdir();(self.root/'attempt'/'report.json').write_text('[1,2,3]')
        for source in ({'name':'c','kind':'claude','roots':[str(sub)],'repo':'repo'},
                       {'name':'g','kind':'glm_report','roots':[str(self.root/'attempt')],'repo':'repo'}):
            with self.subTest(kind=source['kind']):
                receipt=ingest(self.store,{'sources':[source]})
                result=report(self.store,Selection())
                self.assertEqual((receipt['unavailable_sources'],self.store.import_state()['state']),(1,'complete'))
                self.assertEqual((result['status'],result['coverage']['unavailable_primary_sources']),('partial',1))
                self.assertEqual([i['code'] for i in result['coverage']['source_issues']],['source_import_failed:ValueError'])

    def test_per_file_handler_records_shape_errors_but_not_defects(self):
        p=self.file('events.jsonl',[event().data()]);config={'sources':[{'name':'n','kind':'normalized','roots':[str(p)]}]}
        with mock.patch('workflow_auditor.adapters.parse',side_effect=AttributeError('shape')):
            ingest(self.store,config)
        self.assertEqual(self.store.import_state()['state'],'complete')
        self.assertEqual(self.store.sources()[0]['issues'],[{'code':'source_import_failed:AttributeError'}])
        with mock.patch('workflow_auditor.adapters.parse',side_effect=RuntimeError('defect')),self.assertRaises(RuntimeError):
            ingest(self.store,config)
        self.assertEqual(self.store.import_state()['state'],'running')
        self.assertEqual(report(self.store,Selection())['status'],'partial')

    def test_custom_agent_types_map_to_roles_only_through_config(self):
        """Regression guard: this also passes before R13, which only removed private names from the role table."""
        p=self.file('agent-x.jsonl',[self.claude_row('m1','2026-10-20T12:00:00Z','s1')])
        meta=p.with_suffix('.meta.json')
        for agent_type,config,expected in (('reviewer',{},('reviewer','agent_type')),
                                           ('team-reviewer',{},('unknown','unclassified')),
                                           ('team-reviewer',{'type_roles':{'team-reviewer':'reviewer'}},('reviewer','agent_type'))):
            with self.subTest(agent_type=agent_type,config=config):
                meta.write_text(json.dumps({'agentType':agent_type}))
                e=parse(p,{'kind':'claude','repo':'repo'},config).events[0]
                self.assertEqual((e.role,e.role_basis),expected)


class DatabasePathTests(unittest.TestCase):
    """Readers never create or alter a file; writers refuse files that are not auditor databases."""
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.events=self.root/'events.jsonl';self.events.write_text(json.dumps(event().data())+'\n')
        self.config=self.root/'config.json'
        self.config.write_text(json.dumps({'sources':[{'name':'n','kind':'normalized','roots':[str(self.events)]}]}))
        self.foreign=self.root/'foreign.sqlite'
        with sqlite3.connect(self.foreign) as db:db.execute('CREATE TABLE t(x)')
        self.text=self.root/'text.sqlite';self.text.write_text('not a database')

    def tearDown(self):self.tmp.cleanup()

    def cli(self,*argv):
        err=io.StringIO()
        with redirect_stderr(err),redirect_stdout(io.StringIO()):code=main(list(map(str,argv)))
        return code,err.getvalue()

    def state(self,path):
        if not path.exists():return None
        with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as db:
            try:return sorted(r[0] for r in db.execute("SELECT name FROM sqlite_master"))
            except sqlite3.DatabaseError:return path.read_bytes()

    def test_report_and_compare_refuse_bad_paths_without_touching_them(self):
        periods=['--before-start','2026-10-20T00:00:00Z','--before-end','2026-10-20T12:00:00Z',
                 '--after-start','2026-10-20T12:00:00Z','--after-end','2026-10-21T00:00:00Z']
        for path,message in ((self.root/'absent.sqlite','does not exist'),(self.text,'Not an SQLite database'),
                             (self.foreign,'Not a workflow-auditor database')):
            for argv in (['report','--db',path],['compare','--db',path,*periods]):
                with self.subTest(path=path.name,command=argv[0]):
                    before=self.state(path)
                    code,err=self.cli(*argv)
                    self.assertEqual(code,2);self.assertIn(message,err)
                    self.assertEqual(self.state(path),before)

    def test_ingest_validates_config_first_and_refuses_foreign_files(self):
        bad=self.root/'bad.json';bad.write_text(json.dumps({'sources':[{'name':'x','kind':'nope','roots':[]}]}))
        for config in (bad,self.root/'missing.json'):
            with self.subTest(config=config.name):
                self.assertEqual(self.cli('ingest','--config',config,'--db',self.root/'new.sqlite')[0],2)
                self.assertFalse((self.root/'new.sqlite').exists())
        for path in (self.foreign,self.text):
            with self.subTest(db=path.name):
                before=self.state(path)
                self.assertEqual(self.cli('ingest','--config',self.config,'--db',path)[0],2)
                self.assertEqual(self.state(path),before)

    def test_empty_selection_and_unwritable_database_are_worded_plainly(self):
        db=self.root/'ok.sqlite'
        self.assertEqual(self.cli('ingest','--config',self.config,'--db',db)[0],0)
        out=self.root/'empty.md'
        self.assertEqual(self.cli('report','--db',db,'--repo','none','--format','markdown','--output',out)[0],0)
        self.assertIn('List estimates:\n\n- no events.\n',out.read_text())
        if hasattr(os,'geteuid') and os.geteuid()==0:self.skipTest('file modes do not bind root')
        db.chmod(0o444)
        code,err=self.cli('ingest','--config',self.config,'--db',db)
        self.assertEqual(code,2);self.assertIn('Cannot open database (attempt to write a readonly database)',err)

    def test_report_reads_a_write_protected_database_without_writing(self):
        if hasattr(os,'geteuid') and os.geteuid()==0:self.skipTest('file modes do not bind root')
        folder=self.root/'protected';folder.mkdir();db=folder/'ok.sqlite'
        self.assertEqual(self.cli('ingest','--config',self.config,'--db',db)[0],0)
        digest=hashlib.sha256(db.read_bytes()).hexdigest()
        db.chmod(0o444);folder.chmod(0o555)
        try:code,err=self.cli('report','--db',db,'--output',self.root/'r.json')
        finally:folder.chmod(0o755)
        self.assertEqual((code,err),(0,''))
        self.assertEqual(json.loads((self.root/'r.json').read_text())['summary']['events'],1)
        self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(),digest)
        self.assertEqual(sorted(p.name for p in folder.iterdir()),['ok.sqlite'])

    def test_interrupted_transaction_is_not_reported_as_a_foreign_file(self):
        db=self.root/'hot.sqlite'
        self.assertEqual(self.cli('ingest','--config',self.config,'--db',db)[0],0)
        subprocess.run([sys.executable,'-c',f"""
import os, sqlite3
c = sqlite3.connect({str(db)!r}, isolation_level=None); c.execute("BEGIN IMMEDIATE"); c.execute("CREATE TABLE junk(x)")
c.execute("PRAGMA cache_size=1"); [c.execute("INSERT INTO junk VALUES (randomblob(4096))") for _ in range(200)]
os._exit(0)"""],check=True)  # A writer killed mid-transaction leaves a hot journal.
        self.assertTrue(Path(f'{db}-journal').exists())
        code,err=self.cli('report','--db',db)
        self.assertEqual(code,2);self.assertIn('interrupted transaction; run ingest',err)
        self.assertTrue(Path(f'{db}-journal').exists())
        self.assertEqual(self.cli('ingest','--config',self.config,'--db',db)[0],0)
        self.assertEqual(self.cli('report','--db',db,'--output',self.root/'r.json')[0],0)

    def test_validate_history_reports_missing_or_foreign_snapshot_plainly(self):
        script=Path(__file__).resolve().parent.parent/'validate_history.py'
        for db,message in ((self.root/'absent.sqlite','No snapshot'),(self.foreign,'Not a workflow-auditor database')):
            with self.subTest(db=db.name):
                before=self.state(db)
                out=subprocess.run([sys.executable,str(script),'--db',str(db)],capture_output=True,text=True,cwd=self.root)
                self.assertEqual(out.returncode,2);self.assertIn(message,out.stderr)
                self.assertNotIn('Traceback',out.stderr);self.assertEqual(self.state(db),before)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()),['config.json','events.jsonl','foreign.sqlite','text.sqlite'])


    def test_shipped_synthetic_history_validates_and_a_changed_expectation_fails(self):
        repo=Path(__file__).resolve().parent.parent
        examples=repo/'examples'/'history'
        db=self.root/'example.sqlite'
        run=lambda *argv:subprocess.run([sys.executable,*map(str,argv)],capture_output=True,text=True,cwd=repo)
        self.assertEqual(run('audit.py','ingest','--config',examples/'sources.json','--db',db).returncode,0)
        out=run('validate_history.py','--fixtures',examples,'--db',db)
        self.assertEqual(out.returncode,0,out.stderr)
        result=json.loads(out.stdout)
        self.assertEqual((result['checks'],result['passed'],result['cost_checks'],result['cost_checks_passed']),(14,14,1,1))
        changed=self.root/'changed';changed.mkdir()
        fixtures=json.loads((examples/'history-fixtures.json').read_text())
        fixtures[0]['expected']['output_total']+=1
        (changed/'history-fixtures.json').write_text(json.dumps(fixtures))
        (changed/'cost-fixtures.json').write_text((examples/'cost-fixtures.json').read_text())
        self.assertEqual(run('validate_history.py','--fixtures',changed,'--db',db).returncode,1)
        empty=self.root/'empty';empty.mkdir()
        out=run('validate_history.py','--fixtures',empty,'--db',db)
        self.assertEqual(out.returncode,2);self.assertIn('No history-fixtures.json',out.stderr)

    def test_public_check_flags_paths_emails_and_terms_without_printing_them(self):
        script=Path(__file__).resolve().parent.parent/'tools'/'check_public.py'
        repo=self.root/'public';repo.mkdir()
        git=lambda *argv:subprocess.run(['git','-C',str(repo),'-c','user.name=Test','-c',
            'user.email=test@users.noreply.github.com',*argv],capture_output=True,text=True,check=True)
        git('init','-q')
        (repo/'clean.md').write_text('See ~/.claude/projects, C:\\Users\\User\\Repo and test@example.com.\n')
        git('add','.');git('commit','-q','-m','clean')
        terms=self.root/'terms.txt';terms.write_text('# private names\nhiddenproject\n')
        check=lambda:subprocess.run([sys.executable,str(script),'--repo',str(repo),'--terms',str(terms)],
                                    capture_output=True,text=True)
        self.assertEqual(check().returncode,0)
        (repo/'notes.md').write_text('one\n/ho'+'me/alice/work\nmail bob@'+'corp.test\nthe HiddenProject repo\n')
        git('add','notes.md')
        out=check()
        self.assertEqual(out.returncode,1)
        for kind in ('notes.md:2','personal path','notes.md:3','email address','notes.md:4','line 2 of the term list'):
            self.assertIn(kind,out.stdout)
        for raw in ('alice','bob@corp','HiddenProject','hiddenproject'):self.assertNotIn(raw,out.stdout)
        git('reset','-q');(repo/'notes.md').unlink()
        subprocess.run(['git','-C',str(repo),'-c','user.name=Test','-c','user.email=me@'+'corp.test',
                        'commit','-q','--allow-empty','-m','hiddenproject fix'],check=True)
        out=check()
        self.assertEqual(out.returncode,1)
        self.assertIn('author email address is not a noreply address',out.stdout)
        self.assertIn('metadata line 3: private term',out.stdout)

class PublicCheckTests(unittest.TestCase):
    SCRIPT=Path(__file__).resolve().parent.parent/'tools'/'check_public.py'
    TERM='hidden'+'project'
    USER='ali'+'ce'
    HOME='/ho'+'me/'
    MAIL='bob@'+'corp.test'

    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);self.root=Path(tmp.name)
        self.env={k:v for k,v in os.environ.items() if k not in('WORKFLOW_AUDITOR_PRIVATE_TERMS','GIT_NO_REPLACE_OBJECTS')}
        self.env.update(GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_NOSYSTEM='1',GIT_CEILING_DIRECTORIES=str(self.root),
            GIT_AUTHOR_NAME='T',GIT_AUTHOR_EMAIL='t@users.noreply.github.com',
            GIT_COMMITTER_NAME='T',GIT_COMMITTER_EMAIL='t@users.noreply.github.com')
        self.terms=self.root/'terms.txt';self.terms.write_text('# synthetic\nhiddenproject\nhidden-project\nsecret-lab\n')
        spec=importlib.util.spec_from_file_location('check_public',self.SCRIPT)
        self.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.module)

    def git(self,repo,*argv,data=None,env=None):
        return subprocess.run(['git','-C',str(repo),*argv],input=data,capture_output=True,check=True,
                              env=env or self.env).stdout.decode().strip()

    def run_check(self,*argv,env=None):
        return subprocess.run([sys.executable,str(self.SCRIPT),*map(str,argv)],capture_output=True,text=True,
                              env=env or self.env)

    def check(self,repo,terms=None):
        return self.run_check('--repo',repo,'--terms',terms or self.terms)

    def repo(self,name):
        """A repository with one clean commit, which must pass before any fault is added."""
        path=self.root/name;path.mkdir();self.git(path,'init','-q')
        self.commit(path,{'a.txt':'clean\n'},'init')
        out=self.check(path)
        self.assertEqual(out.returncode,0,out.stdout+out.stderr)
        return path

    def commit(self,repo,files=None,message='x'):
        for name,content in (files or {}).items():
            (repo/name).parent.mkdir(parents=True,exist_ok=True);(repo/name).write_text(content)
        self.git(repo,'add','-A');self.git(repo,'commit','-q','--allow-empty','-m',message)

    def blob(self,repo,content='clean\n'):
        return self.git(repo,'hash-object','-w','--stdin',data=content.encode())

    def tree(self,repo,*entries):
        return self.git(repo,'mktree',data=''.join(f'{mode} {kind} {sha}\t{name}\n' for mode,kind,sha,name in entries).encode())

    def raw(self,repo,kind,content):
        """Write an object that Git would not create itself, and return its id."""
        return self.git(repo,'hash-object','-t',kind,'-w','--literally','--stdin',data=content.encode())

    def hidden(self,out,*more):
        shown=(out.stdout+out.stderr).lower()
        for raw in (self.TERM,self.USER,self.MAIL,'corp.test',*more):self.assertNotIn(raw.lower(),shown)

    def expect(self,repo,*patterns,terms=None):
        """The check fails with each finding line given as a regex, and prints none of the private text."""
        out=self.check(repo,terms)
        self.assertEqual(out.returncode,1,out.stdout+out.stderr)
        for pattern in patterns:self.assertRegex(out.stdout,'(?m)^'+pattern)
        self.assertNotIn('Traceback',out.stderr);self.hidden(out)
        return out

    def test_public_check_reads_path_names_refs_and_tag_objects(self):
        T,H,U,M=self.TERM,self.HOME,self.USER,self.MAIL
        path=r'path of blob [0-9a-f]{12} \((?:commit|staged)[0-9a-f ]*\): '
        with self.subTest('a path that exists only in an old commit'):  # mutation: scan HEAD paths only
            r=self.repo('old');self.commit(r,{f'{T}.txt':'one\n'});self.git(r,'rm','-q',f'{T}.txt');self.commit(r)
            self.expect(r,path+'private term')
        with self.subTest('a top-level folder named like a home folder'):  # mutation: drop the "/" prefix
            r=self.repo('toplevel');self.commit(r,{f'{H[1:]}{U}/b.txt':'two\n'})
            self.expect(r,path+'personal path')
        with self.subTest('a shared blob under a clean and a term path'):  # mutation: take paths from rev-list
            r=self.repo('shared');self.commit(r,{f'{T}.txt':''});self.git(r,'rm','-q',f'{T}.txt');self.commit(r,{'empty.txt':''})
            self.expect(r,path+'private term')
        with self.subTest('a staged-only path'):  # mutation: drop ls-files
            r=self.repo('staged');(r/f'{T}.md').write_text('three\n');self.git(r,'add','.')
            self.expect(r,r'path of blob [0-9a-f]{12} \(staged\): private term')
        with self.subTest('a branch name'):  # mutation: drop for-each-ref
            r=self.repo('branch');self.git(r,'branch','feature/'+T)
            self.expect(r,r'ref 1 \(at [0-9a-f]{12}\): private term')
        with self.subTest('a tag message'):  # mutation: read commits only, not tags
            r=self.repo('tag-message');self.git(r,'tag','-a','v1','-m',f'for {T} ask {M}')
            self.expect(r,r'tag [0-9a-f]{12} metadata line 3: private term',r'tag [0-9a-f]{12} metadata line 3: email address')
        with self.subTest('a non-noreply tagger'):  # mutation: leave tagger out of IDENTITY
            r=self.repo('tagger');self.git(r,'tag','-a','v1','-m','ok',env={**self.env,'GIT_COMMITTER_EMAIL':M})
            self.expect(r,r'tag [0-9a-f]{12}: tagger email address is not a noreply address')
        with self.subTest('a nested tag whose inner ref is gone'):  # mutation: read only tags that a ref names
            r=self.repo('nested');self.git(r,'tag','-a','inner','-m','inner '+T);inner=self.git(r,'rev-parse','inner')
            self.git(r,'tag','-a','outer','-m','clean',inner);self.git(r,'tag','-d','inner')
            self.expect(r,r'tag [0-9a-f]{12} metadata line 3: private term')
        with self.subTest('a tag name that differs from its ref'):  # mutation: skip the tag header
            r=self.repo('tag-name');self.git(r,'tag','-a',T+'-v1','-m','clean');sha=self.git(r,'rev-parse',T+'-v1')
            self.git(r,'update-ref','refs/tags/v1',sha);self.git(r,'update-ref','-d','refs/tags/'+T+'-v1')
            self.expect(r,r'tag [0-9a-f]{12} metadata line 1: private term')
        with self.subTest('a line break in a path'):  # mutation: scan the path without joining its lines
            r=self.repo('newline');self.commit(r,{f'x\n{H[1:]}{U}/y.txt':'four\n'})
            self.expect(r,path+'personal path')
        with self.subTest('a finding in a blob under a term path'):  # mutation: label a blob by its first path
            r=self.repo('withheld');self.commit(r,{f'{T}.txt':f'{H}{U}/x\n'})
            self.expect(r,r'\(path withheld\):1 \(blob [0-9a-f]{12}\): personal path')

    def test_public_check_reads_trees_that_only_a_tag_or_ref_points_to(self):
        T,H,U=self.TERM,self.HOME,self.USER
        with self.subTest('an annotated tag on a tree'):  # mutation: ignore trees that tags point to
            r=self.repo('tag-tree');self.git(r,'tag','-a','t','-m','clean',self.tree(r,('100644','blob',self.blob(r),T+'.txt')))
            self.expect(r,r'path of blob [0-9a-f]{12} \(tag [0-9a-f]{12}\): private term')
        with self.subTest('a lightweight tag on a tree'):  # mutation: drop the trees that refs point to
            r=self.repo('ref-tree');leaf=self.tree(r,('100644','blob',self.blob(r),'x.txt'))
            top=self.tree(r,('040000','tree',self.tree(r,('040000','tree',leaf,U)),H[1:-1]));self.git(r,'tag','lw',top)
            self.expect(r,r'path of tree [0-9a-f]{12} \(ref\): personal path')
        with self.subTest('an empty subtree'):  # mutation: list trees without -t
            r=self.repo('empty-tree');empty=self.tree(r)
            top=self.tree(r,('100644','blob',self.blob(r),'a.txt'),('040000','tree',empty,T))
            self.git(r,'update-ref','HEAD',self.git(r,'commit-tree','-p','HEAD','-m','x',top))
            self.expect(r,r'path of tree [0-9a-f]{12} \(commit [0-9a-f]{12}\): private term')

    def test_public_check_reads_every_commit_and_tag_header(self):
        T,M=self.TERM,self.MAIL
        def variant(name,extra,message='clean\n'):
            r=self.repo(name);head=self.git(r,'rev-parse','HEAD');tree=self.git(r,'rev-parse','HEAD^{tree}')
            side=self.git(r,'commit-tree','-p',head,'-m','side',tree)
            ident='T <t@users.noreply.github.com> 1700000000 +0000'
            commit=self.raw(r,'commit',f'tree {tree}\nparent {head}\nparent {side}\nauthor {ident}\ncommitter {ident}\n'
                            +extra.format(side=side)+f'\n{message}')
            self.git(r,'update-ref','HEAD',commit);return r
        with self.subTest('a merged tag'):  # mutation: skip header lines that are indented
            r=variant('mergetag',f'mergetag object {{side}}\n type commit\n tag v9\n tagger Bob <{M}> 1700000000 +0000\n \n release of {T}\n')
            self.expect(r,r'commit [0-9a-f]{12}: tagger email address is not a noreply address',
                        r'commit [0-9a-f]{12} metadata line 8: private term')
        with self.subTest('another header'):  # mutation: read only the author and committer headers
            r=variant('extra',f'x-reviewed-by {M}\n')
            self.expect(r,r'commit [0-9a-f]{12} metadata line 3: email address')
        with self.subTest('a signature is not read'):  # mutation: read gpgsig lines
            r=variant('signed',f'gpgsig -----BEGIN PGP SIGNATURE-----\n \n {T}\n -----END PGP SIGNATURE-----\n')
            self.assertEqual(self.check(r).returncode,0)
        with self.subTest('a message holding the log record separator'):
            r=variant('separator','',f'one\x1e{T}\n')
            self.expect(r,r'commit [0-9a-f]{12} metadata line 4: private term')
        with self.subTest('a message in another encoding'):  # mutation: ignore the encoding header
            names=self.root/'names.txt';names.write_text('cr\u00e8me\n');r=self.repo('latin1')
            head=self.git(r,'rev-parse','HEAD');tree=self.git(r,'rev-parse','HEAD^{tree}')
            ident='T <t@users.noreply.github.com> 1700000000 +0000'
            raw=f'tree {tree}\nparent {head}\nauthor {ident}\ncommitter {ident}\nencoding ISO-8859-1\n\nthe cr\u00e8me recipe\n'
            self.git(r,'update-ref','HEAD',self.git(r,'hash-object','-t','commit','-w','--literally','--stdin',data=raw.encode('latin-1')))
            self.expect(r,r'commit [0-9a-f]{12} metadata line 4: private term',terms=names)
        with self.subTest('an encoding that agrees with UTF-8 reports once'):  # mutation: keep duplicate lines from the two readings
            out=self.expect(variant('latin1-twice','encoding ISO-8859-1\n',f'{T}\n'),r'commit [0-9a-f]{12} metadata line 4: private term')
            shown=out.stdout.splitlines();self.assertEqual(len(shown),len(set(shown)))
        with self.subTest('a UTF-16 commit and a non-noreply author'):  # mutation: decode the headers with the declared encoding only
            r=self.repo('utf16');self.git(r,'-c','i18n.commitEncoding=UTF-16','commit','-q','--allow-empty','-m',f'for {T}',
                                          env={**self.env,'GIT_AUTHOR_EMAIL':M})
            self.assertEqual(self.git(r,'log','-1','--format=%ae'),M)
            self.expect(r,r'commit [0-9a-f]{12}: author email address is not a noreply address',
                        r'commit [0-9a-f]{12} metadata line \d+: private term')
        with self.subTest('a UTF-32 tag and a non-noreply tagger'):  # mutation: the same, for tags
            r=self.repo('utf32');head=self.git(r,'rev-parse','HEAD')
            tag=self.raw(r,'tag',f'object {head}\ntype commit\ntag v1\ntagger Bob <{M}> 1700000000 +0000\nencoding UTF-32\n\nclean\n')
            self.git(r,'update-ref','refs/tags/v1',tag)
            self.expect(r,r'tag [0-9a-f]{12}: tagger email address is not a noreply address')
        for number,codec in enumerate(('undefined','utf\x008')):  # mutations: catch LookupError only; catch UnicodeError but not ValueError
            with self.subTest('an encoding that Python cannot use',codec=codec):
                clean=self.check(variant(f'unusable{number}',f'encoding {codec}\n'))
                self.assertEqual(clean.returncode,0,clean.stdout+clean.stderr);self.assertNotIn('Traceback',clean.stderr)
                self.expect(variant(f'unusable-term{number}',f'encoding {codec}\n',f'{T}\n'),r'commit [0-9a-f]{12} metadata line 4: private term')

    def test_public_check_ignores_replace_refs(self):
        T,H,U=self.TERM,self.HOME,self.USER
        with self.subTest('a replaced blob'):  # mutation: leave GIT_NO_REPLACE_OBJECTS out of the environment
            r=self.repo('replace-blob');self.commit(r,{'notes.md':f'see {H}{U}/x for {T}\n'})
            original=self.git(r,'rev-parse','HEAD:notes.md');self.git(r,'replace',original,self.blob(r))
            self.assertEqual(self.git(r,'cat-file','blob',original),'clean')
            self.expect(r,r'notes\.md:1 \(blob [0-9a-f]{12}\): personal path')
        with self.subTest('a replaced commit'):
            r=self.repo('replace-commit');self.commit(r,{},f'about {T}')
            head=self.git(r,'rev-parse','HEAD');clean=self.git(r,'commit-tree','-p','HEAD^','-m','clean',self.git(r,'rev-parse','HEAD^{tree}'))
            self.git(r,'replace',head,clean)
            self.assertEqual(self.git(r,'log','-1','--format=%s'),'clean')
            self.expect(r,r'commit [0-9a-f]{12} metadata line 3: private term')

    def test_public_check_needs_the_top_level_of_a_repository(self):
        with self.subTest('a folder inside a repository'):  # mutation: skip the same-folder test
            r=self.repo('nested-dir');self.commit(r,{'sub/k.txt':'k\n'});(r/f'{self.TERM}.md').write_text('five\n');self.git(r,'add','.')
            out=self.run_check('--repo',r/'sub','--terms',self.terms)
            self.assertEqual(out.returncode,2,out.stdout);self.assertIn('top-level',out.stderr);self.hidden(out)
        with self.subTest('a bare repository'):  # mutation: treat a bare repository as a work tree
            r=self.repo('source');bare=self.root/'bare.git';self.git(r,'clone','-q','--bare',str(r),str(bare))
            out=self.run_check('--repo',bare,'--terms',self.terms);self.assertEqual(out.returncode,0,out.stderr)

    def test_public_check_terms_match_any_join_but_not_longer_words(self):
        def hits(terms,text):return any(self.module.term_pattern(t).search(text) for t in terms)
        listed=('hiddenproject','hidden-project','secret-lab')
        # mutations: plain re.escape; (?i) over the whole pattern; no (?=[A-Z]); no end lookahead; no own-separator
        for text in ('HiddenProject','hidden_project','hidden project','hidden.project','HiddenprojectRunner','runHiddenProject',
                     'hiddenproject2','secretlab','SecretLab','SECRET-LAB','secret-lab-runs','the hiddenproject repo'):
            with self.subTest(match=text):self.assertTrue(hits(listed,text))
        for text in ('hiddenprojectx','xhiddenproject','hiddenprojects','mysecretlab','secret-labor'):
            with self.subTest(refuse=text):self.assertFalse(hits(listed,text))
        with self.subTest('a CamelCase entry'):  # mutation: do not split an entry at a case change
            self.assertTrue(hits(('SecretLab',),'secret_lab'))
        with self.subTest('a separator of the entry itself'):  # mutation: join with [-_.\s]* only
            self.assertTrue(hits(('acme/widget',),'acme/widget'))
        with self.subTest('an entry that ends in a digit'):
            self.assertTrue(hits(('lab2',),'lab2'));self.assertFalse(hits(('lab2',),'lab23'))
        with self.subTest('an entry with no letters or digits'):self.assertIsNone(self.module.term_pattern('-- _'))

    def test_public_check_flags_every_home_form_but_placeholders(self):
        H,U,A='ho'+'me','Us'+'ers','ali'+'ce'
        flagged=lambda text:any(kind=='personal path' for _,kind in self.module.findings(text,[]))
        forms=[f'/{H}/{A}',f'/{U}/{A}',f'C:\\{U}\\{A}',f'C:\\\\{U}\\\\{A}',f'C:/{U}/{A}',f'/mnt/c/{U}/{A}',f'file:///{H}/{A}',
               f'\\\\wsl$\\Ubuntu\\{H}\\{A}',f'\\\\wsl.localhost\\Ubuntu\\{H}\\{A}',f'%2F{H}%2F{A}',f'"\\u002f{H}\\u002f{A}"',
               f'\\/{H}\\/{A}',f'C%3A%5C{U}%5C{A}',f'C:\\u005c{U}\\u005c{A}',f'-{H}-{A}-Projects-x',f'/tmp/claude-1000/-{H}-{A}-r',
               f'-mnt-c-{U}-{A}-x',f'-{U}-{A}-x',f'C--{U}-{A}-x',f'~{A}/projects','/ro'+'ot/.claude']
        for text in forms:  # mutations: drop any pattern or SEP alternative (\u005c in particular)
            with self.subTest(flag=text):self.assertTrue(flagged(text))
        for text in (f'/{H}/user/',f'/{H}/User/x',f'C:/{U}/...',f'/{H}/<user>',f'%2F{H}%2Fuser%2Fx',f'C:\\{U}\\%USERNAME%\\x',
                     f'-{H}-user-x',f'foo-{H}-{A}-',f'--{H}-{A}','~/.claude','~user/','~~strike~~','~5/','https://h/~a/',
                     '/var/ro'+'ot/','./ro'+'ot/','~/ro'+'ot/'):  # mutations: no % in the stop set; skip the placeholder test
            with self.subTest(pass_=text):self.assertFalse(flagged(text))

    def test_public_check_exit_codes_say_whether_it_could_check(self):
        r=self.repo('exit');(r/'list.txt').write_text('hiddenproject\n');good=self.terms
        def listing(name,content,encoding='utf-8'):path=self.root/name;path.write_bytes(content.encode(encoding));return path
        (self.root/'link-in.txt').symlink_to(r/'list.txt');(r/'link-out.txt').symlink_to(good)
        bare=self.root/'bare.git';self.git(r,'clone','-q','--bare',str(r),str(bare))
        empty=self.root/'empty';empty.mkdir();self.git(empty,'init','-q')
        plain=self.root/'plain';plain.mkdir()
        R=['--repo',r];ENV='WORKFLOW_AUDITOR_PRIVATE_TERMS'
        cases=[('no list',R,{},2,'no private-term list'),('only --no-terms',R+['--no-terms'],{},0,'were not checked'),
               ('--no-terms and --terms',R+['--no-terms','--terms',good],{},2,'contradicts'),
               ('--no-terms and the environment',R+['--no-terms'],{ENV:str(good)},2,'contradicts'),
               ('only the environment',R,{ENV:str(good)},0,''),('an empty environment variable',R,{ENV:''},2,'no private-term list'),
               ('a list inside the repository',R+['--terms',r/'list.txt'],{},2,'inside the repository'),
               ('the same, written with ..',R+['--terms',r/'..'/r.name/'list.txt'],{},2,'inside the repository'),
               ('a link outside to a list inside',R+['--terms',self.root/'link-in.txt'],{},2,'inside the repository'),
               ('a link inside to a list outside',R+['--terms',r/'link-out.txt'],{},2,'inside the repository'),
               ('a list in .git',R+['--terms',r/'.git'/'description'],{},2,'inside the repository'),
               ('a missing list',R+['--terms',self.root/'none.txt'],{},2,'cannot read the term list'),
               ('a directory as list',R+['--terms',self.root],{},2,'cannot read the term list'),
               ('a Latin-1 list',R+['--terms',listing('l1.txt','caf\u00e9\n','latin-1')],{},2,'not UTF-8'),
               ('a UTF-16 list',R+['--terms',listing('u16.txt','hiddenproject\n','utf-16')],{},2,'not UTF-8'),
               ('a UTF-8 BOM',R+['--terms',listing('bom.txt','\ufeffhiddenproject\n')],{},0,''),
               ('comments only',R+['--terms',listing('c.txt','# only\n\n')],{},2,'no terms'),
               ('punctuation only',R+['--terms',listing('p.txt','---\n__\n')],{},2,'no terms'),
               ('not a repository',['--repo',plain,'--terms',good],{},2,'git rev-parse failed'),
               ('a missing --repo named like a term',['--repo',self.root/self.TERM,'--terms',good],{},2,'git rev-parse failed'),
               ('an empty repository',['--repo',empty,'--terms',good],{},0,''),('a bare repository',['--repo',bare,'--terms',good],{},0,''),
               ('an unknown option',R+['--bogus'],{},2,'unrecognized')]
        for name,argv,extra,code,words in cases:  # mutations: no list as []; no zero-term check, UnicodeError, inside() or note
            with self.subTest(name):
                out=self.run_check(*argv,env={**self.env,**extra})
                self.assertEqual(out.returncode,code,out.stdout+out.stderr);self.assertIn(words,out.stderr)
                self.assertNotIn('Traceback',out.stderr);self.hidden(out)

if __name__=='__main__':unittest.main()
