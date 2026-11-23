"""Offline quota relay acceptance. Every T3 launch and WeCom delivery is intercepted."""
import sys as _sys
from pathlib import Path as _Path
_BRIDGE_DIR = str(_Path(__file__).resolve().parent.parent / 'bridge')
if _BRIDGE_DIR not in _sys.path:
    _sys.path.insert(0, _BRIDGE_DIR)
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import ambiguity_gate
import private_bot_control
import t3_client as client
import t3_dispatch as dispatch
import t3_quota_relay as relay
import test_t3_bridge as fixtures


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.cfg = dict(quota_relay_enabled=True, tasks_root=str(self.root/'tasks'),
                        t3_project_id='p', t3_home=str(self.root/'fixture-home'))
        self.configure()
        self.project = dict(project_id='p', title='研发', workspace_root=str(self.root))
        self.projects = self.mock(client, 'list_active_projects', return_value=[self.project])
        self.catalog = self.mock(client, 'live_catalog', return_value=fixtures.FAKE_CATALOG)
        self.ask = self.mock(private_bot_control, 'ask_owner', return_value={'success':True})
        self.alert = self.mock(private_bot_control, 'send_markdown', return_value={'success':True})
        self.auth = self.mock(private_bot_control, 'status', return_value={'authenticated':True})
        self.rpc = self.mock(client, '_ws_rpc_call', side_effect=self.launch_rpc)
        self.start = self.mock(client, 'start_thread', side_effect=AssertionError('unexpected CLI launch'))
        self.runtime = self.mock(client, 'inspect_thread_runtime', return_value={
            'run_status':'failed', 'first_user':'核对原始目标', 'subagents':[{'child_thread_id':'kid','status':'running'}],
            'error_items':[{'message':'Antigravity stopped this turn because of an error.'}]})
        self.threads = {}
        self.list = self.mock(client, 'list_threads', side_effect=lambda root,p,**kw:
                              [v['thread'] for v in self.threads.values() if v['thread']['projectId']==p])
        self.show = self.mock(client, 'show_thread', side_effect=lambda root,tid,**kw:self.threads[tid])
        with relay._db(self.root):
            pass

    def mock(self, module, name, **kw):
        return self.enterContext(patch.object(module, name, **kw))

    def launch_rpc(self, root, method, payload, **kwargs):
        self.assertEqual(method, 'orchestration.launchThread')
        return {'_tag':'Success','value':{'threadId':payload['threadId'],'projection':{'runs':[{'id':'new-run'}]}}}

    def configure(self, **kwargs):
        self.cfg.update(kwargs)
        (self.root/'t3-bridge-config.json').write_text(json.dumps(self.cfg),encoding='utf-8')

    def thread(self, tid='opus', model='claude-opus', banner='⚠ Individual quota reached. Resets in 3h49m40s.', **kwargs):
        th = dict(threadId=tid, projectId='p', latestRunId='r1', title='源线程', status='failed', updatedAt=dispatch.stamp(),
                  model=model, providerInstanceId='codex' if model.startswith('gpt-6') else 'antigravity',
                  runtimeMode='approval-required', interactionMode='plan')
        th.update(kwargs)
        self.threads[tid] = {'thread':th,'messages':[{'role':'assistant','text':banner,'runId':'r1'}]}
        return th

    def sweep(self):
        relay.sweep_all_t3_threads(self.root)

    def records(self):
        with relay._db(self.root) as db:
            return [dict(r) for r in db.execute('SELECT * FROM t3_quota_relays ORDER BY id')]

    def cool(self, pool):
        with relay._db(self.root) as db:
            db.execute('INSERT OR REPLACE INTO quota_pool_health VALUES(?,?,?,?,?)',
                       (pool,time.time()+300,'fixture','prior',dispatch.stamp()))

    def test_opus_tail_to_neon_preserves_worktree_modes_and_children(self):
        subprocess.run(['git','init','--quiet','-b','feature',str(self.root)], check=True)
        self.thread(worktreePath=str(self.root), branch='feature')
        self.sweep()
        self.sweep()
        row, = self.records()
        self.assertEqual((row['status'],row['source_pool'],row['target_model'],row['notified']),
                         ('relayed',relay.SHARED,'gemini-4-neon',1))
        payload = self.rpc.call_args.args[2]
        self.assertEqual(payload['workspaceStrategy'],{'type':'existing_worktree','worktreePath':str(self.root)})
        self.assertEqual((payload['runtimeMode'],payload['interactionMode']),('approval-required','plan'))
        self.assertEqual(payload['modelSelection']['options'],[{'id':'thought_level','value':'xhigh'}])
        self.assertEqual(self.rpc.call_count,1)
        self.assertEqual(self.ask.call_args.args[1],row['target_thread_id'])
        self.assertEqual(self.ask.call_count,1)
        prompt = Path(row['checkpoint_path']).read_text(encoding='utf-8')
        self.assertEqual(prompt,payload['initialMessage']['text'])
        for value in ('接棒第一负责人','第 1 棒','核对原始目标','kid','t3_thread_read','P4','UEBridge','feature'):
            self.assertIn(value,prompt)
        data = json.loads((self.root/'quota-relay-status.json').read_text(encoding='utf-8'))
        self.assertAlmostEqual(data['cooldowns'][relay.SHARED]-time.time(),13780,delta=5)
        self.assertEqual(data['daily_count'],1)

    def test_gemini_to_astra(self):
        self.thread(model='gemini-4-neon')
        self.sweep()
        payload = self.rpc.call_args.args[2]
        self.assertEqual(payload['modelSelection'],dict(instanceId='codex',model='gpt-6.2-astra',
                                                       options=[dict(id='reasoningEffort',value='high')]))
        self.assertIn('codex-video-perception',payload['initialMessage']['text'])

    def test_codex_to_neon(self):
        for error in ({"error":{"type":"usage_limit_reached"}},{"statusCode":429},"HTTP 429","You've hit your usage limit"):
            with self.subTest(error=error):
                signal = relay.classify_quota_failure('failed','codex','gpt-6.2-sol',error_text=error)
                self.assertTrue(signal['is_quota'])
        self.thread(model='gpt-6.2-astra',banner='You’ve hit your usage limit.')
        self.sweep()
        payload = self.rpc.call_args.args[2]
        self.assertIn('@oai/sky',payload['initialMessage']['text'])
        self.assertEqual(payload['modelSelection']['model'],'gemini-4-neon')

    def test_both_exhausted_and_gpt_oss_shares_opus_pool(self):
        self.thread(model='gpt-oss-120b')
        self.cool(relay.GEMINI)
        self.cool(relay.CODEX)
        self.sweep()
        self.sweep()
        row, = self.records()
        self.assertEqual((row['status'],row['source_pool'],row['notified']),('exhausted_all',relay.SHARED,1))
        self.assertIn('UTC',self.alert.call_args.args[2])
        self.alert.assert_called_once()
        self.rpc.assert_not_called()
        self.start.assert_not_called()

    def test_provider_signals_and_reset_duration(self):
        for raw,want in [('Try again in 2 hours 15 minutes',8100),('retry after 300s',300),
                         ('resets in 45m',2700),('Resets in 1s',60),('resets in 8 hours',21600),('later',1200)]:
            self.assertEqual(relay.parse_reset_seconds(raw),want)
        for status in ('completed','idle','running','cancelled','interrupted'):
            self.assertFalse(relay.classify_quota_failure(status,'antigravity','claude-4',
                             error_items=[{'code':'RESOURCE_EXHAUSTED'}],assistant_text='quota reached')['is_quota'])
        for text in ('UE compile failed: C2065','Traceback: ValueError','Discussed HTTP 429 earlier.','example: quota reached'):
            self.assertFalse(relay.classify_quota_failure('failed','antigravity','gemini-4-neon',assistant_text=text)['is_quota'])
        self.assertFalse(relay.classify_quota_failure('failed','other','model',error_text='HTTP 429')['is_quota'])
        structured = relay.classify_quota_failure('failed','codex','gpt-6.2-astra',
                     error_text={'error':{'type':'usage_limit_reached','resets_in_seconds':320}})
        self.assertEqual((structured['is_quota'],structured['reset_seconds']), (True,320))

    def test_stale_assistant_message_is_never_evidence(self):
        th = self.thread(banner='build failed')
        self.threads[th['threadId']]['messages'].insert(0,dict(role='assistant',runId='old',text='quota reached'))
        self.sweep()
        self.assertEqual(self.records(),[])
        info = dict(thread=th,messages=self.threads[th['threadId']]['messages'][:1])
        outcome = client.wait_for_thread(self.root,'opus',show_fn=lambda *a,**kw:info)
        self.assertEqual(outcome['reply'],'')

    def test_missing_catalog_and_option_downgrade(self):
        self.cool(relay.GEMINI)
        chosen = relay.choose_relay_target(self.root,relay.SHARED)
        self.assertEqual(chosen['model'],'gpt-6.2-astra')
        self.catalog.return_value = {'providers':{'codex':{'enabled':True}},'models':[
            {'provider':'codex','id':'gpt-6.2-astra','option_id':'reasoningEffort','efforts':['low']}]}
        self.assertEqual(relay.choose_relay_target(self.root,relay.SHARED)['options'],{})
        self.catalog.return_value['providers']['codex']['enabled'] = False
        self.assertEqual(relay.choose_relay_target(self.root,relay.SHARED)['reason'],'catalog_unavailable')

    def test_cutoff_child_ownership_and_other_projects(self):
        self.projects.return_value.append(dict(self.project,project_id='p2'))
        self.thread('watermark',updatedAt=datetime.fromtimestamp(time.time()-5,timezone.utc).isoformat())
        self.thread('child',parentThreadId='parent')
        self.thread('old',updatedAt='2000-01-01T00:00:00Z')
        self.thread('owned')
        self.thread('second',projectId='p2')
        with dispatch.db(self.root) as db:
            db.execute("INSERT INTO t3_jobs(msgid,row_json,context_json,context_hash,state,created,updated,t3_thread_id,pid)"
                       " VALUES('job','{}','{}','x','running','1','1','owned',9000)")
        self.sweep()
        self.assertEqual([r['source_thread_id'] for r in self.records()],['second'])
        self.assertEqual({c.args[1] for c in self.show.call_args_list},{'second'})

    def test_recency_even_if_enabled_long_ago(self):
        with relay._db(self.root) as db:
            db.execute("UPDATE quota_relay_meta SET value='2000-01-01T00:00:00Z'")
        self.thread('old',updatedAt=datetime.fromtimestamp(time.time()-1805,timezone.utc).isoformat())
        self.sweep()
        self.assertEqual(self.records(),[])

    def test_old_run_recently_renamed_and_user_interrupt(self):
        self.thread()
        self.runtime.return_value['completed_at'] = '2000-01-01T00:00:00Z'
        self.sweep()
        del self.runtime.return_value['completed_at']
        self.runtime.return_value['user_stopped'] = True
        self.sweep()
        self.rpc.assert_not_called()
        self.assertEqual(self.records(),[])

    def test_branch_changed_preserves_current_checkout(self):
        subprocess.run(['git','init','--quiet','-b','different',str(self.root)], check=True)
        self.thread(branch='original')
        self.sweep()
        self.rpc.assert_not_called()
        self.assertEqual(self.records()[0]['reason'],'source_branch_changed')

    def test_hops_and_daily_cap(self):
        self.thread()
        self.sweep()
        target = self.records()[0]['target_thread_id']
        self.thread(target,model='gemini-4-neon')
        self.sweep()
        second = self.records()[1]
        self.assertEqual((second['root_thread_id'],second['hop_count']),('opus',2))
        self.thread(second['target_thread_id'],model='gpt-6.2-astra',banner='usage_limit_reached')
        self.sweep()
        self.assertEqual(self.records()[2]['reason'],'max_hops')
        with relay._db(self.root) as db:
            db.execute('DELETE FROM quota_pool_health')
        self.configure(quota_relay_daily_cap=2)
        self.thread('fourth')
        self.sweep()
        self.assertEqual(self.records()[-1]['reason'],'daily_cap')
        self.assertEqual(self.rpc.call_count,2)

    def test_simultaneous_sweeps_are_at_most_once(self):
        self.thread()
        barrier = threading.Barrier(2)
        self.list.side_effect = lambda *a,**kw:(barrier.wait(timeout=5),[self.threads['opus']['thread']])[1]
        with ThreadPoolExecutor(2) as workers:
            for f in [workers.submit(self.sweep),workers.submit(self.sweep)]:
                f.result(10)
        self.assertEqual(self.rpc.call_count,1)
        self.assertEqual(self.ask.call_count,1)
        self.assertEqual(len(self.records()),1)

    def test_newer_source_run_and_uncertain_launch(self):
        th = self.thread()
        self.show.side_effect = [self.threads['opus'],dict(thread=dict(th,latestRunId='r2'))]
        self.sweep()
        self.assertEqual(self.records()[0]['reason'],'source_changed')
        self.rpc.assert_not_called()
        self.thread('second')
        self.show.side_effect = lambda root,tid,**kwargs:self.threads[tid]
        self.rpc.side_effect = TimeoutError('ack missing')
        self.sweep()
        self.sweep()
        self.assertEqual(self.records()[1]['status'],'launch_failed')
        self.rpc.assert_called_once()

    def test_notifications_are_optional_and_do_not_fail_launch(self):
        self.ask.side_effect = OSError('offline')
        self.thread()
        self.sweep()
        self.assertEqual((self.records()[0]['status'],self.records()[0]['notified']),('relayed',0))
        self.configure(quota_relay_notify_owner=False)
        self.thread('two')
        self.sweep()
        self.ask.assert_called_once()


class ClientTests(unittest.TestCase):
    def test_start_cli_option_mapping_pairs_and_legacy_argv(self):
        with patch.object(client,'_run_cli',return_value=subprocess.CompletedProcess([],0,'{"threadId":"new"}','')) as run:
            for value,expected in [(None,[]),({},[]),({'thought_level':'xhigh'},['--option','thought_level=xhigh']),
                                   ([('',1),('a',None),('b',''),('b',False)],['--option','b=false'])]:
                client.start_thread(None,'p','antigravity','gemini','t','prompt',wait=False,options=value)
                argv = run.call_args.args[1]
                self.assertEqual(argv[argv.index('--json')+1:],expected+['p','-'])

    def test_projection_reads_wal_scopes_items_and_survives_unavailable_db(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/'userdata').mkdir()
            (root/'t3-bridge-config.json').write_text(json.dumps(dict(t3_home=temp,t3_project_id='fallback')), encoding='utf-8')
            self.assertEqual(client.list_active_projects(root)[0]['project_id'],'fallback')
            path = root/'userdata/statev2.sqlite'
            self.assertFalse(path.exists())
            db = sqlite3.connect(path)
            try:
                db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE projection_projects(project_id,title,workspace_root,deleted_at,archived_at);
                INSERT INTO projection_projects VALUES('p','P','x',NULL,NULL),('other','O','y',NULL,1);
                CREATE TABLE orchestration_v2_projection_runs(thread_id,run_id,ordinal,provider_instance_id,status,completed_at,payload_json);
                INSERT INTO orchestration_v2_projection_runs VALUES('t','r',1,'codex','failed',NULL,'{"modelSelection":{"model":"gpt-6.2-astra"}}');
                CREATE TABLE orchestration_v2_projection_turn_items(thread_id,run_id,type,ordinal,payload_json);
                INSERT INTO orchestration_v2_projection_turn_items VALUES('t','r','error',1,'{"failure":{"code":"usage_limit_reached"}}'),
                ('t','r','assistant_message',2,'{"text":"this run"}'),('t','old','assistant_message',7,'{"text":"old"}');
                CREATE TABLE orchestration_v2_projection_subagents(thread_id,child_thread_id,provider_instance_id,status,completion_delivery_state,payload_json);
                INSERT INTO orchestration_v2_projection_subagents VALUES('t','k','codex','running','pending','{"title":"child"}');
                CREATE TABLE orchestration_v2_projection_threads(thread_id,payload_json);
                CREATE TABLE orchestration_v2_projection_messages(thread_id,role,created_at,payload_json);
                INSERT INTO orchestration_v2_projection_messages VALUES('t','user',1,'{"text":"objective"}');
                """)
                db.execute('BEGIN IMMEDIATE')
                self.assertEqual([p['project_id'] for p in client.list_active_projects(root)],['p'])
                rt = client.inspect_thread_runtime(root,'t',run_id='r')
                self.assertEqual((rt['run_model'],rt['assistant_text'],rt['first_user']),('gpt-6.2-astra','this run','objective'))
                self.assertEqual(rt['subagents'][0]['child_thread_id'],'k')
                self.assertEqual(rt['error_items'][0]['code'],'usage_limit_reached')
                with self.assertRaises(sqlite3.OperationalError):
                    with client.contextlib.closing(client._projection(root)) as reader:
                        reader.execute("DELETE FROM projection_projects")
            finally:
                db.close()


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.fx = fixtures.FeiyuT3BridgeTests()
        self.addCleanup(self.fx.doCleanups)
        self.fx.setUp()
        self.root = self.fx.root
        cfg = client.load_config(self.root)
        self.fx.put('t3-bridge-config.json',dict(cfg,quota_relay_enabled=True,t3_home=str(self.root/'fake-home')))
        self.fx.drop_msg('quota-msg','请核对本地文档。')
        fixtures.work.reconcile(self.root)
        self.cid = fixtures.work.key('quota-msg')
        self.assertTrue(self.fx.tick_without_process())
        self.th = dict(threadId='triage',latestRunId='r',status='failed',providerInstanceId='antigravity',
                       model='gemini-4-hydrogen',projectId='proj-feiyu-test')
        self.info = dict(thread=self.th,messages=[dict(role='assistant',runId='r',text='⚠ Individual quota reached')])
        self.enterContext(patch.object(client,'show_thread',return_value=self.info))
        self.enterContext(patch.object(private_bot_control,'ask_owner',side_effect=AssertionError))
        self.enterContext(patch.object(private_bot_control,'send_markdown',side_effect=AssertionError))

    def test_failed_reply_triggers_inline_rebind_then_normal_receipt(self):
        def start(root, project, provider, model, title, prompt, **kwargs):
            if model == 'gemini-4-hydrogen':
                work = Path(self.fx.t3_job(self.cid)['workspace'])
                (work/'t3-response.json').write_text('{"action":"BLOCK"}',encoding='utf-8')
                (work/'execution-proof.json').write_text('{"executedModel":"old-proof"}',encoding='utf-8')
                return dict(exit_code=1,thread_id='triage',run_id='r',status='failed',
                            reply='⚠ Individual quota reached',raw=self.info)
            self.assertEqual((model,kwargs['options']),('gpt-6.2-sol',{'reasoningEffort':'medium'}))
            self.assertIn('<<<FEIYU_JSON>>>',prompt)
            self.assertIn('task-control.json',prompt)
            return dict(exit_code=0,thread_id='handoff',run_id='r2',raw={'link':'t3:handoff'})
        def wait(root,tid,**kw):
            job = self.fx.t3_job(self.cid)
            self.assertEqual((job['t3_thread_id'],job['last_event'],job['t3_archive_after']),('handoff','t3_quota_relayed',None))
            return dict(exit_code=0,thread_id=tid,status='completed',
                        reply=ambiguity_gate.JSON_START+'{"action":"COMPLETED","final_answer":"核对完成"}'+ambiguity_gate.JSON_END)
        with patch.object(client,'start_thread',side_effect=start), patch.object(client,'wait_for_thread',side_effect=wait):
            self.assertTrue(dispatch.run_job(self.root,self.cid,self.fx.t3_job(self.cid)['lease']))
        job = self.fx.t3_job(self.cid)
        self.assertEqual((job['state'],job['t3_thread_id'],job['requested_model']),('completed','handoff','gpt-6.2-sol'))
        receipt = json.loads((Path(job['workspace'])/'completion.json').read_text(encoding='utf-8'))
        self.assertEqual(receipt['t3_thread_id'],'handoff')
        self.assertEqual(receipt['used_models'][0]['model'],'gpt-6.2-sol')
        fixtures.work.sync_t3(self.root)
        sends = []
        for _ in range(2):
            fixtures.chat_feedback.promote(self.root)
            while fixtures.chat_feedback.deliver_one(self.root, lambda args:self.fx.progress_cli(sends,args)):
                pass
        finals = [s['markdown']['content'] for s in sends if '办理结果' in s['markdown']['content']]
        self.assertEqual(len(finals),1)
        self.assertIn('核对完成',finals[0])

    def test_failed_nonquota_turn_cannot_parse_stale_protocol(self):
        self.info['messages'][0]['text'] = 'compiler error'
        with patch.object(ambiguity_gate,'parse_t3_response',side_effect=AssertionError) as parse, patch.object(
                client,'start_thread',return_value=dict(exit_code=1,thread_id='triage',run_id='r',status='failed',
                                                        reply='compiler error',raw=self.info)):
            self.assertFalse(dispatch.run_job(self.root,self.cid,self.fx.t3_job(self.cid)['lease']))
        parse.assert_not_called()
        self.assertEqual(self.fx.t3_job(self.cid)['state'],'failed')

    def test_recovery_propagates_failed_run_without_waiting_in_audit(self):
        job_work = self.root/'work'
        job_work.mkdir()
        with dispatch.db(self.root) as db:
            db.execute("UPDATE t3_jobs SET workspace=?,state='running',t3_thread_id='triage',worker_identity=NULL WHERE msgid=?",
                       (str(job_work),self.cid))
        with patch.object(client,'start_thread',return_value=dict(exit_code=0,thread_id='next',run_id='r2')), \
             patch.object(client,'wait_for_thread',side_effect=AssertionError):
            dispatch.reconcile(self.root)
        job = self.fx.t3_job(self.cid)
        self.assertEqual((job['state'],job['t3_thread_id']),('running','next'))

    def test_proactive_fallback_default_only(self):
        with relay._db(self.root) as db:
            db.execute('INSERT INTO quota_pool_health VALUES(?,?,?,?,?)',(relay.GEMINI,time.time()+300,'test','t',dispatch.stamp()))
        job = self.fx.t3_job(self.cid)
        for light,model in [(True,'gpt-6.2-sol'),(False,'gpt-6.2-astra')]:
            provider,actual,opts = dispatch._default_launch(self.root,job,{},'antigravity','gemini-4-neon',lightweight=light)
            self.assertEqual((provider,actual),('codex',model))
            self.assertIn('reasoningEffort',opts['options'])
        result = dispatch._default_launch(self.root,job,{'explicit_model':{'model':'gemini-4-neon'}},
                                          'antigravity','gemini-4-neon',lightweight=False)
        self.assertEqual(result,('antigravity','gemini-4-neon',{}))

    def test_lost_ack_recovers_only_preallocated_target(self):
        self.th.update(runtimeMode='approval-required', interactionMode='default')
        work = self.root/'job-work'
        work.mkdir()
        with dispatch.db(self.root) as db:
            db.execute("UPDATE t3_jobs SET workspace=?,state='running',t3_thread_id='triage' WHERE msgid=?",
                       (str(work),self.cid))
        targets = {}
        def lost_ack(root,method,payload,**kwargs):
            tid = payload['threadId']
            targets[tid] = dict(thread=dict(threadId=tid,projectId=self.th['projectId'],providerInstanceId='codex',
                                            model='gpt-6.2-sol',latestRunId='new',status='running'))
            raise TimeoutError('ack lost')
        with patch.object(client,'_ws_rpc_call',side_effect=lost_ack) as launch, patch.object(
             client,'show_thread',side_effect=lambda root,tid,**kw: self.info if tid=='triage' else targets[tid]):
            dispatch.reconcile(self.root)
        launch.assert_called_once()
        tid, = targets
        self.assertEqual(self.fx.t3_job(self.cid)['t3_thread_id'],tid)
        with relay._db(self.root) as db:
            self.assertEqual(db.execute('SELECT status FROM t3_quota_relays').fetchone()[0],'relayed')


if __name__=='__main__':
    unittest.main()
