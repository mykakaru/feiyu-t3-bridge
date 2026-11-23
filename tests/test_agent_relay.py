"""End-to-end and unit tests for Agent-to-Owner WeCom ask & reply relay."""
from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_BRIDGE_DIR = str(_Path(__file__).resolve().parent.parent / 'bridge')
if _BRIDGE_DIR not in _sys.path:
    _sys.path.insert(0, _BRIDGE_DIR)

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import agent_relay
import business_queue as work
import chat_feedback
import clarifications
import group_relay
from identity import CLI_OWNER
import private_bot_control
import t3_client
import t3_dispatch

OWNER_WS = 'U_OWNER_0001'
OTHER_USER = 'Colleague999'

FAKE_CATALOG = {
    'providers': {
        'antigravity': {'id': 'antigravity', 'display_name': 'Antigravity', 'enabled': True},
    },
    'models': [
        {
            'provider': 'antigravity',
            'id': 'gemini-4-hydrogen',
            'label': 'Gemini 4 Hydrogen',
            'option_id': 'thought_level',
            'efforts': ['low', 'medium', 'high', 'xhigh'],
            'default': 'high',
            'service_tiers': [],
            'default_service_tier': None,
        },
        {
            'provider': 'antigravity',
            'id': 'gemini-4-neon',
            'label': 'Gemini 4 Neon',
            'option_id': 'thought_level',
            'efforts': ['low', 'medium', 'high', 'xhigh'],
            'default': 'xhigh',
            'service_tiers': [],
            'default_service_tier': None,
        },
    ],
}


class AgentRelayTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.tasks_root = self.root / 'tasks'
        self.tasks_root.mkdir()
        (self.root / 'queue').mkdir()
        (self.root / 'media-state').mkdir()
        self.put('business-migration.json', {'groups': [], 'historical_ids': []})
        self.put('request-flow.json', {'enabled_since_epoch': 1.0})
        self.put('identity_mapping.json', {
            'owner_ws_userids': [OWNER_WS],
            'verified_at': '2026-11-14T18:14:39.851745+00:00',
            'verification': 'authenticated ChatGPT owner confirmed this exact private message',
            'owner_confirmation': '是我发的',
            'correlated_msgid': 'seed-mid',
        })
        self.put('t3-bridge-config.json', {
            'version': 2,
            'enabled': True,
            'engine': 't3',
            't3_project_id': 'proj-feiyu-test',
            'tasks_root': str(self.tasks_root),
            'entry_model': {'provider': 'antigravity', 'model': 'gemini-4-hydrogen', 'effort': 'high'},
            'escalation_model': {'provider': 'antigravity', 'model': 'gemini-4-neon', 'effort': 'xhigh'},
            'clarification_mode': 'strict',
        })
        self.enterContext(patch.object(t3_client, 'live_catalog', return_value=FAKE_CATALOG))
        self.enterContext(patch.object(
            t3_client,
            'server_status',
            return_value={'ok': True, 'reason': 'ready', 'port': 3773, 'pid': 1234, 'cli_exists': True, 'version': '0.1.10'},
        ))
        self.enterContext(patch.object(t3_dispatch, 'wake'))

    def put(self, name, obj):
        (self.root / name).write_text(json.dumps(obj, ensure_ascii=False), encoding='utf-8')

    def drop_msg(self, mid, text, *, sender=OWNER_WS, chat=None, chattype='single', quotes=None, attachments=None, ts=None, bot_channel='private_bot', msgtype='text'):
        parts = [{'type': 'text', 'text': text}] if text else []
        for q in (quotes or []):
            parts.append({'type': 'text', 'text': q, 'quoted': True})
        row = {
            'msgid': mid,
            'bot_channel': bot_channel,
            'received_at': work.utc(ts) if ts is not None else work.utc(),
            'chattype': chattype,
            'sender': sender,
            'chatid': chat or sender,
            'msgtype': msgtype,
            'delivery': 'pending',
            'parts': parts,
            'attachments': attachments or [],
        }
        self.put('queue/' + hashlib.sha256(mid.encode()).hexdigest() + '.json', row)
        return row

    def test_outbound_ask_cli_multiline_chinese_quotes_and_reuse_session_code(self):
        tid = 'mcp:ebb0d9b2-b598-4c60-815b-3f7f55ad8672'
        msg_file = self.root / 'ask.txt'
        msg_body = '【空手 Loco 二轮研究 · 需要拍板】\nQ1 “新动捕缺口”怎么处理？\nA 补录\nB 改编合成'
        msg_file.write_text(msg_body, encoding='utf-8')

        # 1. Dry-run via CLI using --text-file (PowerShell-safe)
        proc = subprocess.run(
            [
                sys.executable,
                str(Path(_BRIDGE_DIR) / 'private_bot_control.py'),
                'ask',
                '--root', str(self.root),
                '--thread-id', tid,
                '--title', '空手 Loco 二轮研究',
                '--text-file', str(msg_file),
                '--dry-run',
            ],
            capture_output=True,
            text=True,
            encoding='utf-8',
            check=True,
        )
        dry = json.loads(proc.stdout)
        self.assertTrue(dry['success'])
        self.assertTrue(dry['dry_run'])
        self.assertRegex(dry['session_code'], r'^A-[0-9A-F]{4}$')
        self.assertIn('“新动捕缺口”', dry['text'])
        self.assertTrue(dry['text'].endswith(f'—— 来自 Agent〔{dry["session_code"]}〕· 空手 Loco 二轮研究 | threadId: {tid}'))

        # 2. Real ask_owner with mocked send_markdown -> registers mapping & reuses session code on 2nd ask
        sent_payloads = []

        def fake_send(root, chatid, text, *, timeout=25.0):
            sent_payloads.append((chatid, text))
            return {'success': True, 'errcode': 0, 'errmsg': 'ok', 'req_id': f'pb_test_{len(sent_payloads)}', 'ws_req_id': f'ws_{len(sent_payloads)}'}

        with patch.object(private_bot_control, 'send_markdown', side_effect=fake_send):
            res1 = private_bot_control.ask_owner(self.root, tid, msg_body, title='空手 Loco 二轮研究')
            res2 = private_bot_control.ask_owner(self.root, tid, '第二轮补充问题')

        self.assertTrue(res1['success'])
        self.assertTrue(res1['registered'])
        self.assertEqual(res1['session_code'], res2['session_code'])
        self.assertEqual(sent_payloads[0][0], OWNER_WS)
        self.assertIn(f'threadId: {tid}', sent_payloads[0][1])

        with work.db(self.root) as c:
            rows = c.execute('SELECT * FROM agent_outbound_asks ORDER BY id').fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['req_id'], 'pb_test_1')
        self.assertEqual(rows[0]['thread_id'], tid)
        self.assertEqual(rows[1]['session_code'], res1['session_code'])

    def test_quote_reply_routes_to_origin_thread_and_sends_short_receipt(self):
        tid = 'mcp:ebb0d9b2-b598-4c60-815b-3f7f55ad8672'
        sent_texts = []

        def fake_pb_send(root, chatid, text, *, timeout=25.0):
            sent_texts.append((chatid, text))
            return {'success': True, 'errcode': 0, 'errmsg': 'ok', 'req_id': f'pb_r_{len(sent_texts)}'}

        with patch.object(private_bot_control, 'send_markdown', side_effect=fake_pb_send):
            ask = private_bot_control.ask_owner(
                self.root,
                tid,
                '【空手 Loco 二轮研究】\nQ1 选 A 还是 B？\nQ2 停步怎么落？',
                title='空手 Loco 二轮研究',
            )
        asked_wire_text = sent_texts[0][1]
        code = ask['session_code']

        # Owner quotes the Agent-ask message in private_bot chat
        owner_reply = 'Q1 选 A，Q2 统一落回标准 Idle01，先按这个来。'
        self.drop_msg('m_quote_1', owner_reply, quotes=[asked_wire_text], bot_channel='private_bot')

        forwarded_calls = []

        def fake_show(root, thread_id, **kwargs):
            return {'thread': {'threadId': thread_id, 'archived': False, 'status': 'completed'}}

        def fake_t3_send(root, thread_id, message, *, wait=True, timeout=None, runner=None):
            forwarded_calls.append({'thread_id': thread_id, 'message': message, 'wait': wait})
            return {'exit_code': 0, 'thread_id': thread_id, 'run_id': 'run:test:13', 'status': 'queued', 'reply': ''}

        with patch.object(t3_client, 'show_thread', side_effect=fake_show), \
             patch.object(t3_client, 'send_message', side_effect=fake_t3_send):
            work.reconcile(self.root)

        # Must forward to the exact original thread with wait=False (queues cleanly)
        self.assertEqual(len(forwarded_calls), 1)
        self.assertEqual(forwarded_calls[0]['thread_id'], tid)
        self.assertFalse(forwarded_calls[0]['wait'])
        self.assertIn(owner_reply, forwarded_calls[0]['message'])
        self.assertIn('Q1 选 A 还是 B', forwarded_calls[0]['message'])
        self.assertIn(code, forwarded_calls[0]['message'])

        # Must NOT create any new Feiyu T3 job
        if (self.root / 'local-tasks.sqlite3').is_file():
            with t3_dispatch.db(self.root) as tc:
                self.assertEqual(tc.execute('SELECT COUNT(*) FROM t3_jobs').fetchone()[0], 0)

        # Receipt delivered via private_bot says 已转交原 Agent〔A-XXXX〕
        delivered_receipts = []
        with patch.object(private_bot_control, 'status', return_value={'authenticated': True, 'running': True}), \
             patch.object(private_bot_control, 'send_markdown', side_effect=lambda r, ch, txt, **kw: (delivered_receipts.append((ch, txt)), {'success': True, 'errcode': 0})[1]):
            chat_feedback.deliver_one(self.root, lambda args: {'success': True}, first_only=True)

        self.assertEqual(len(delivered_receipts), 1)
        self.assertEqual(delivered_receipts[0][0], OWNER_WS)
        self.assertIn(f'已转交原 Agent〔{code}〕', delivered_receipts[0][1])

    def test_direct_session_code_and_thread_id_reply_routing(self):
        tid = 'mcp:11111111-2222-3333-4444-555555555555'
        reg = agent_relay.register_ask(
            self.root,
            thread_id=tid,
            body='请问方案 A 和 B 选哪个？',
            chatid=OWNER_WS,
            title='方案确认',
            req_id='pb_direct_1',
        )
        code = reg['session_code']

        self.drop_msg('m_code_1', f'{code} 选方案 B，明天再补测试。')
        self.drop_msg('m_cmd_1', f'【回复 {code}】再补一条：把日志一并保留。')
        self.drop_msg('m_tid_1', f'threadId: {tid} 收到，按这个继续。')

        sent = []
        with patch.object(t3_client, 'show_thread', return_value={'thread': {'threadId': tid, 'archived': False, 'status': 'idle'}}), \
             patch.object(t3_client, 'send_message', side_effect=lambda r, t, m, **kw: (sent.append((t, m)), {'exit_code': 0, 'thread_id': t, 'run_id': 'r1'})[1]):
            work.reconcile(self.root)

        self.assertEqual(len(sent), 3)
        self.assertTrue(all(t == tid for t, _ in sent))
        self.assertIn('选方案 B，明天再补测试。', sent[0][1])
        self.assertIn('再补一条：把日志一并保留。', sent[1][1])
        self.assertIn('收到，按这个继续。', sent[2][1])

    def test_archived_or_missing_thread_falls_back_to_new_task_with_notice(self):
        tid_arch = 'mcp:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        tid_miss = 'mcp:99999999-8888-7777-6666-555555555555'
        reg_arch = agent_relay.register_ask(self.root, thread_id=tid_arch, body='原问题1', chatid=OWNER_WS, req_id='pb_arch')
        reg_miss = agent_relay.register_ask(self.root, thread_id=tid_miss, body='原问题2', chatid=OWNER_WS, req_id='pb_miss')

        q_arch = f'原问题1\n\n—— 来自 Agent〔{reg_arch["session_code"]}〕 threadId: {tid_arch}'
        q_miss = f'原问题2\n\n—— 来自 Agent〔{reg_miss["session_code"]}〕 threadId: {tid_miss}'
        self.drop_msg('m_arch_reply', '帮我按方案 A 继续做一下', quotes=[q_arch])
        self.drop_msg('m_miss_reply', '帮我按方案 B 继续做一下', quotes=[q_miss])

        def fake_show(root, thread_id, **kwargs):
            if thread_id == tid_arch:
                return {'thread': {'threadId': thread_id, 'archived': True, 'status': 'completed'}}
            raise RuntimeError('t3_thread_show_failed_1')

        with patch.object(t3_client, 'show_thread', side_effect=fake_show), \
             patch.object(t3_client, 'send_message') as mock_send:
            work.reconcile(self.root)
            mock_send.assert_not_called()

        # Both fell back to normal T3 new tasks!
        with t3_dispatch.db(self.root) as tc:
            jobs = tc.execute('SELECT msgid, state FROM t3_jobs').fetchall()
        self.assertEqual(len(jobs), 2)
        self.assertTrue(all(j['state'] == 'queued' for j in jobs))

        # Check receipts include the clear fallback explanation
        receipts = []
        with patch.object(private_bot_control, 'status', return_value={'authenticated': True, 'running': True}), \
             patch.object(private_bot_control, 'send_markdown', side_effect=lambda r, ch, txt, **kw: (receipts.append(txt), {'success': True, 'errcode': 0})[1]):
            while chat_feedback.deliver_one(self.root, lambda args: {'success': True}, first_only=True):
                pass
        joined = '\n'.join(receipts)
        self.assertIn(f'原 Agent〔{reg_arch["session_code"]}〕原线程已归档，已自动转为新任务办理', joined)
        self.assertIn(f'原 Agent〔{reg_miss["session_code"]}〕原线程不存在，已自动转为新任务办理', joined)

    def test_security_non_owner_and_group_never_forwarded(self):
        tid = 'mcp:12345678-1234-1234-1234-1234567890ab'
        reg = agent_relay.register_ask(self.root, thread_id=tid, body='内部问题', chatid=OWNER_WS, req_id='pb_sec')
        q = f'内部问题\n\n—— 来自 Agent〔{reg["session_code"]}〕 threadId: {tid}'

        # Colleague single chat & group chat attempts
        self.drop_msg('m_colleague_single', '我也来回复选A', sender=OTHER_USER, chat=OTHER_USER, chattype='single', quotes=[q])
        self.drop_msg('m_owner_group', f'@大肥鱼 {reg["session_code"]} 群里测试', sender=OWNER_WS, chat='wrkGroup12345678', chattype='group', quotes=[q], bot_channel='feiyu')

        with patch.object(t3_client, 'show_thread') as mock_show, \
             patch.object(t3_client, 'send_message') as mock_send:
            work.reconcile(self.root)
            mock_show.assert_not_called()
            mock_send.assert_not_called()

    def test_compat_feiyu_completion_quote_and_session_lookup_not_hijacked(self):
        # 1. Quoting a Feiyu 办理结果〔xxxxxxx〕 that ends with 会话ID：mcp:... MUST open a new task (NOT relay)
        feiyu_result_quote = (
            '办理结果〔2d35ee6〕\n已完成群友秒推卡片忽略功能。\n\n---\n'
            '会话ID：mcp:2caa4323-2a02-49d5-bec8-f8902fd4e145'
        )
        self.drop_msg('m_feiyu_q', '那是不是也可以直接引用写直回', quotes=[feiyu_result_quote])

        # 2. Asking a new agent to read an archived session ID MUST open a new task (NOT relay)
        self.drop_msg('m_lookup', '请查一下会话ID mcp:2caa4323-2a02-49d5-bec8-f8902fd4e145 之前聊了什么并总结')

        with patch.object(t3_client, 'show_thread') as mock_show, \
             patch.object(t3_client, 'send_message') as mock_send:
            work.reconcile(self.root)
            mock_show.assert_not_called()
            mock_send.assert_not_called()

        with t3_dispatch.db(self.root) as tc:
            self.assertEqual(tc.execute('SELECT COUNT(*) FROM t3_jobs').fetchone()[0], 2)

    def test_compat_open_clarification_and_group_draft_yield_to_quoted_agent_ask(self):
        work.initialize(self.root)
        # Seed an open Feiyu clarification Q-XXXX and an open group draft D-XXXX
        self.drop_msg('m_seed_work', '先建个任务')
        work.reconcile(self.root)
        seed_cid = work.key('m_seed_work')
        with work.db(self.root, True) as c:
            clarifications.open_question(
                c, seed_cid, thread_id='mcp:seed-clar', chat=OWNER_WS, sender=OWNER_WS,
                origin='agent_gate', reason='测试追问', questions=[{'question': '选哪个', 'options': ['A', 'B']}],
                bot_channel='private_bot',
            )
            group_relay.create_draft(
                c, case_id=seed_cid, bot_channel='private_bot',
                owner_chat=OWNER_WS, owner_sender=OWNER_WS,
                target_chat_id='wrkTestGroup99999', target_chat_name='测试群',
                body='测试草稿正文', state='pending_confirm',
            )

        tid = 'mcp:77777777-6666-5555-4444-333333333333'
        reg = agent_relay.register_ask(self.root, thread_id=tid, body='请确认发还是不发 / 选A还是B', chatid=OWNER_WS, req_id='pb_prio')
        q = f'请确认发还是不发 / 选A还是B\n\n—— 来自 Agent〔{reg["session_code"]}〕 threadId: {tid}'

        # Reply "A" quoting the Agent-ask must go to the Agent, NOT the open Q-XXXX clarification!
        self.drop_msg('m_reply_short_a', 'A', quotes=[q])
        # Reply "发" quoting the Agent-ask must go to the Agent, NOT approve the unrelated D-XXXX draft!
        self.drop_msg('m_reply_short_fa', '发', quotes=[q])

        sent = []
        with patch.object(t3_client, 'show_thread', return_value={'thread': {'threadId': tid, 'archived': False, 'status': 'idle'}}), \
             patch.object(t3_client, 'send_message', side_effect=lambda r, t, m, **kw: (sent.append((t, m)), {'exit_code': 0, 'thread_id': t, 'run_id': 'r2'})[1]):
            work.reconcile(self.root)

        self.assertEqual(len(sent), 2)
        self.assertTrue(all(t == tid for t, _ in sent))
        # Verify the unrelated clarification and draft are STILL untouched/open!
        with work.db(self.root) as c:
            self.assertTrue(clarifications.has_open(c, seed_cid))
            draft = c.execute("SELECT state FROM group_drafts WHERE case_id=?", (seed_cid,)).fetchone()
            self.assertEqual(draft['state'], 'pending_confirm')

    def test_transient_t3_server_outage_defers_then_succeeds(self):
        tid = 'mcp:55555555-4444-3333-2222-111111111111'
        reg = agent_relay.register_ask(self.root, thread_id=tid, body='临时断连测试', chatid=OWNER_WS, req_id='pb_out')
        q = f'临时断连测试\n\n—— 来自 Agent〔{reg["session_code"]}〕 threadId: {tid}'
        self.drop_msg('m_outage', '同意继续', quotes=[q], ts=1000.0)

        with patch.object(t3_client, 'server_status', return_value={'ok': False, 'reason': 'server_port_unreachable'}):
            work.reconcile(self.root, now=1001.0)

        with work.db(self.root) as c:
            fwd = c.execute('SELECT state, attempts, due FROM agent_relay_forwards WHERE mid=?', ('m_outage',)).fetchone()
        self.assertEqual(fwd['state'], 'pending')
        self.assertEqual(fwd['attempts'], 1)
        self.assertGreater(fwd['due'], 1001.0)

        sent = []
        with patch.object(t3_client, 'server_status', return_value={'ok': True, 'reason': 'ready'}), \
             patch.object(t3_client, 'show_thread', return_value={'thread': {'threadId': tid, 'archived': False, 'status': 'idle'}}), \
             patch.object(t3_client, 'send_message', side_effect=lambda r, t, m, **kw: (sent.append((t, m)), {'exit_code': 0, 'thread_id': t, 'run_id': 'r3'})[1]):
            work.reconcile(self.root, now=fwd['due'] + 1.0)

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][0], tid)


if __name__ == '__main__':
    unittest.main()
