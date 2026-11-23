"""Private file delivery regressions; only synthetic recipients/CLI calls and temporary roots."""
import sys as _sys
from pathlib import Path as _Path
_BRIDGE_DIR = str(_Path(__file__).resolve().parent.parent / 'bridge')
if _BRIDGE_DIR not in _sys.path:
    _sys.path.insert(0, _BRIDGE_DIR)
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import business_queue
import chat_feedback
from identity import CLI_OWNER
import local_dispatch
import private_files as files
import wecom_cli


class PrivateFiles(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.cid = "w_fixture"
        (self.root/"command-routing.json").write_text(json.dumps({"enabled":True, "tasks_root": str(self.root/"tasks")}))
        self.work = files.workspace(self.root, self.cid)
        self.path = self.work/"outputs"/"鸣潮 增订(五色).pdf"
        self.path.parent.mkdir(parents=True)
        self.path.write_bytes(b"%PDF-1.4\nfixture original\n%%EOF\n")
        self.other = self.path.with_name("附表.xlsx")
        self.other.write_bytes(b"second fixture")
        business_queue.initialize(self.root)
        self.messages, self.uploads = [], {}
        self.damage = False
        self.unknown_send = False
        self.reject_upload = False
        self.throttle = False
        self.allow = True
        self.after_read = lambda: None
        self.barrier = None
        self.download_folders = []
        self.extended_path = False

    def cli(self, args, *, upload_directory=None):
        if upload_directory is not None:
            self.assertEqual(args[:2], ['media','upload'])
            self.assertEqual(Path(json.loads(args[-1])['file_path']).parent, upload_directory)
        if args == ['identity','whoami']:
            return {'extra_identity_context':'授权真人用户身份：\nID：'+CLI_OWNER}
        if args == ['message','aibot','sessions','list']:
            return {'sessions':[{'chat_type':'single','chat_id':CLI_OWNER}]}
        payload = json.loads(args[-1])
        if args[:2] == ["media", "upload"]:
            if self.reject_upload:
                return {"success":False,"errcode":1}
            self.assertEqual(payload["type"], "file")
            path = Path(payload["file_path"])
            key = hashlib.sha256(path.read_bytes()).hexdigest()
            self.uploads[key] = (path.name, path.read_bytes())
            return {"media_id":key, "type":"file"}
        if args[:2] == ["media","download"]:
            folder = Path(args[args.index("--output-dir")+1])
            self.download_folders.append(folder)
            name, data = self.uploads[payload["media_id"]]
            saved = folder/name
            saved.write_bytes(data+b"x" if self.damage else data)
            self.after_read()
            if self.barrier: self.barrier.wait(5)
            return {"file_path":("\\\\?\\" if self.extended_path else "")+str(saved)}
        self.assertEqual(args[:4],["message","aibot","send","--json"])
        self.assertEqual(payload["chat_id"],CLI_OWNER)
        self.messages.append(payload)
        if payload['msg_type']=='file' and self.unknown_send:
            raise TimeoutError()
        if payload['msg_type']=='markdown' and self.throttle:
            self.throttle=False
            return {"success":False,"errcode":45009}
        return {"success":True}

    def render(self,text):
        return files.private_result(self.root,self.cid,CLI_OWNER,text,files.references(text),self.cli,lambda:self.allow)

    def setup_receipt(self, text):
        with business_queue.db(self.root,True) as c:
            c.execute("INSERT INTO cases(id,chat,sender,at,epoch,category,updated) VALUES(?,?,?,'t',?,'work',?)",
                      (self.cid,CLI_OWNER,CLI_OWNER,time.time(),time.time()))
            c.execute("INSERT INTO lanes(case_id,target,state,result) VALUES(?,'local','completed',?)",(self.cid,text))
            c.execute("INSERT INTO request_routes VALUES(?,'original','private')",(self.cid,))
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due) VALUES(?,'final','ready',0)",(self.cid,))

    def dispatch(self):
        return chat_feedback.deliver_one(self.root,self.cli)

    def test_historic_link_forms_and_chinese_name(self):
        legacy=self.work/self.path.name
        shutil.copyfile(self.path, legacy)
        for link in (f"[打开]({legacy.as_posix()})",f"[打开](<{legacy.as_posix()}>)",
                     f"![PDF]({legacy.as_uri()})",f"[PDF]({quote(legacy.as_posix(), safe=':/')})"):
            with self.subTest(link=link):
                text, ok = self.render(link)
                self.assertTrue(ok,text)
                self.assertEqual(text, "附件《"+legacy.name+"》")
        self.assertEqual(len(self.messages),1)
        self.assertTrue(all(not d.exists() for d in self.download_folders))

    def test_related_file_types_and_encoded_names(self):
        ref = "outputs/"+self.path.name
        body = f"[一](<{ref}>)\n[二](<outputs/{self.other.name}>)\n[又一次](<{ref}>)\n[在线表](https://doc.weixin.qq.com/sheet/example)"
        self.setup_receipt(body)
        self.assertTrue(self.dispatch())
        self.assertFalse(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages], ['file','file','markdown'])
        sent = self.messages[-1]['markdown']['content']
        self.assertIn("https://doc.weixin.qq.com/sheet/example",sent)
        self.assertNotIn("outputs/",sent)
        self.assertIn(self.path.name,sent)
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts").fetchone()[0],'accepted')
            rows = list(c.execute("SELECT sha256,bytes,state FROM private_file_receipts ORDER BY bytes DESC"))
            self.assertEqual(tuple(rows[0]),(files.sha(self.path),self.path.stat().st_size,'accepted'))

    def test_incomplete_never_echoes_success_or_local_target(self):
        text = "文件已送达 [打开](<outputs/missing.pdf>)"
        self.setup_receipt(text)
        self.assertTrue(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages],['markdown'])
        self.assertIn("交付未完成",self.messages[0]['markdown']['content'])
        self.assertNotIn("文件已送达",self.messages[0]['markdown']['content'])
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts").fetchone()[0],'file_incomplete')

    def test_source_citation_does_not_replace_text_answer_with_file_error(self):
        self.setup_receipt('答案是 `Name_ToCrouch`。[命名规范](F:/Project/Rules/animation-naming.md:71)')
        self.assertTrue(self.dispatch())
        self.assertFalse(self.dispatch())
        self.assertEqual(len(self.messages), 1)
        sent = self.messages[0]['markdown']['content']
        self.assertIn('答案是 `Name_ToCrouch`。命名规范（animation-naming.md:71）', sent)
        self.assertFalse(self.uploads)
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts").fetchone()[0], 'accepted')

    def test_missing_attachment_preserves_business_answer(self):
        self.setup_receipt('核对结果为通过。\n\n文件已送达 [附件](outputs/missing.pdf)')
        self.assertTrue(self.dispatch())
        self.assertFalse(self.dispatch())
        sent = self.messages[0]['markdown']['content']
        self.assertIn('核对结果为通过。', sent)
        self.assertIn('文件交付未完成', sent)
        self.assertNotIn('文件已送达', sent)
        self.assertFalse(self.uploads)

    def fixture_old_file_notice(self, answer, *, confirmed=True):
        self.setup_receipt(answer)
        notice = (f'事项〔{self.cid[-7:]}〕文件交付未完成。\n源文件不在本事项的交付目录。'
                  '完整办理结果可在大肥鱼面板按事项号查看。')
        raw = f'办理结果〔{self.cid[-7:]}〕\n{answer}'
        digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
        with business_queue.db(self.root, True) as c:
            c.execute("UPDATE chat_receipts SET state='file_incomplete',attempts=1 WHERE phase='final'")
            c.execute('INSERT INTO private_feedback_text VALUES(?,?,?,?,?,0)',
                      (self.cid, digest(CLI_OWNER), digest(raw), digest(notice), notice))
            c.execute("INSERT INTO feedback_attempts VALUES(?,'final',1,?,?,?,?,?,?)",
                      (self.cid, time.time(), digest(CLI_OWNER), digest(raw), 'file_incomplete',
                       chat_feedback.flow.receipt_detail({'success': confirmed}, notice, CLI_OWNER), time.time()))
            chat_feedback.flow.alert(c, self.cid, 'final', 'send_file_incomplete')

    def test_confirmed_error_only_is_corrected_once(self):
        self.fixture_old_file_notice('正确答案。[依据](F:/Project/Rules/animation-naming.md:71)')
        with business_queue.db(self.root, True) as c:
            self.assertTrue(chat_feedback.queue_answer_recovery(c, self.root, self.cid))
            self.assertFalse(chat_feedback.queue_answer_recovery(c, self.root, self.cid))
        self.assertTrue(self.dispatch())
        self.assertFalse(self.dispatch())
        self.assertEqual(len(self.messages), 1)
        self.assertIn('正确答案。依据（animation-naming.md:71）', self.messages[0]['markdown']['content'])
        with business_queue.db(self.root) as c:
            self.assertEqual([tuple(row) for row in c.execute('SELECT phase,state FROM chat_receipts ORDER BY phase')],
                             [('final','file_incomplete'),('final_recovery','accepted')])
            self.assertIsNotNone(c.execute('SELECT resolved FROM feedback_alerts').fetchone()[0])

    def test_unconfirmed_notice_is_not_replayed(self):
        self.fixture_old_file_notice('正确答案。[依据](F:/Project/Rules/animation-naming.md:71)', confirmed=False)
        with business_queue.db(self.root, True) as c:
            self.assertFalse(chat_feedback.queue_answer_recovery(c, self.root, self.cid))
        self.assertFalse(self.dispatch())
        self.assertFalse(self.messages)

    def test_external_or_internal_paths_never_open_or_upload(self):
        sources = [r"\\server\share\s.pdf","C:/Windows/file.pdf","../sibling.pdf",
                   "outputs/../../x.pdf","outputs/test.pdf:secret","worker.json",
                   "https://127.0.0.1:1000/temp.pdf","http://[::1]/private","sandbox:/mnt/data/file.pdf"]
        for s in sources:
            with self.subTest(s=s):
                self.assertFalse(self.render(f"[原文](<{s}>)")[1])
                self.assertFalse(self.uploads or self.messages)

    def test_damage_or_rejected_upload_sends_no_card(self):
        link=f"[打开](<{self.path}>)"
        self.damage=True
        self.assertFalse(self.render(link)[1])
        self.damage=False
        self.reject_upload=True
        self.assertFalse(self.render(link)[1])
        self.assertFalse(self.messages)

    def test_scope_recheck_before_visible_send(self):
        self.after_read=lambda:setattr(self,'allow',False)
        self.assertFalse(self.render(f"[PDF](<{self.path}>)")[1])
        self.assertFalse(self.messages)

    @unittest.skipUnless(os.name=="nt","Windows extended paths")
    def test_official_cli_extended_length_path_in_same_temp_folder(self):
        self.extended_path=True
        self.assertTrue(self.render(f"[原件](<{self.path}>)")[1])
        self.assertEqual(self.messages[0]['msg_type'],'file')

    def test_real_cli_adapter_scopes_only_upload_to_verified_output_directory(self):
        from types import SimpleNamespace
        calls=[]
        def run(argv,**options):
            args=argv[2:]
            calls.append((args[:2],options.get('cwd')))
            payload=json.loads(args[-1])
            if args[:2]==['media','upload'] and options.get('cwd')!=Path(payload['file_path']).parent:
                return SimpleNamespace(returncode=1,stdout='{"error":{"type":"PermissionError","code":893006}}')
            return SimpleNamespace(returncode=0,stdout=json.dumps(self.cli(args)))
        with patch.object(local_dispatch.subprocess,'run',side_effect=run):
            files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,local_dispatch.cli_call,lambda:True)
            self.assertEqual(calls,[(['media','upload'],self.path.parent),(['media','download'],None),(['message','aibot'],None)])
            with self.assertRaisesRegex(ValueError,'scope_requires_media_upload'):
                local_dispatch.cli_call(['identity','whoami'],upload_directory=self.work)
            args=['media','upload','--json',json.dumps({'file_path':str(self.path)})]
            with self.assertRaisesRegex(ValueError,'upload_source_directory_mismatch'):
                local_dispatch.cli_call(args,upload_directory=self.root)
            self.assertEqual(local_dispatch.cli_call(args),{'success':False,'errcode':893006})

    def test_wecom_cli_adapter_scopes_upload_and_download_to_verified_directories(self):
        from types import SimpleNamespace
        calls=[]
        def run(argv,**options):
            args=argv[2:]
            calls.append((args[:2],options.get('cwd')))
            if args[:2]==['media','upload']:
                payload=json.loads(args[-1])
                if options.get('cwd')!=Path(payload['file_path']).parent:
                    return SimpleNamespace(returncode=1,stdout='{"error":{"type":"PermissionError","code":893006}}')
            if args[:2]==['media','download']:
                out_dir=Path(args[args.index('--output-dir')+1])
                if options.get('cwd')!=out_dir:
                    return SimpleNamespace(returncode=1,stdout='{"error":{"type":"PermissionError","code":893006}}')
            return SimpleNamespace(returncode=0,stdout=json.dumps(self.cli(args)))
        self.setup_receipt(f"[总览图](<outputs/{self.path.name}>)")
        with patch.object(wecom_cli.subprocess,'run',side_effect=run):
            self.assertTrue(chat_feedback.deliver_one(self.root,wecom_cli.cli_call))
            self.assertIn((['media','upload'],self.path.parent),calls)
            self.assertTrue(any(op==['media','download'] and cwd==self.download_folders[0] for op,cwd in calls))
            self.assertEqual([m['msg_type'] for m in self.messages],['file','markdown'])
            with self.assertRaisesRegex(ValueError,'scope_requires_media_upload'):
                wecom_cli.cli_call(['identity','whoami'],upload_directory=self.work)
            args=['media','upload','--json',json.dumps({'file_path':str(self.path)})]
            with self.assertRaisesRegex(ValueError,'upload_source_directory_mismatch'):
                wecom_cli.cli_call(args,upload_directory=self.root)
            self.assertEqual(wecom_cli.cli_call(args),{'success':False,'errcode':893006})
            dl_args=['media','download','--output-dir',str(self.work),'--json',json.dumps({'media_id':'m'})]
            with self.assertRaisesRegex(ValueError,'download_target_directory_mismatch'):
                wecom_cli.cli_call(dl_args,download_directory=self.root)
            with self.assertRaisesRegex(ValueError,'scope_requires_media_download'):
                wecom_cli.cli_call(['identity','whoami'],download_directory=self.work)
            self.assertEqual(wecom_cli.cli_call(dl_args),{'success':False,'errcode':893006})
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts WHERE phase='final'").fetchone()[0],'accepted')

    def test_unknown_send_file_is_never_duplicated(self):
        self.unknown_send=True
        link=f"[附件](<{self.path}>)"
        self.assertFalse(self.render(link)[1])
        self.unknown_send=False
        self.assertFalse(self.render(link)[1])
        self.assertEqual(len(self.messages),1)

    def test_retry_of_text_keeps_same_file_card(self):
        self.setup_receipt(f"[附件](<{self.path}>)")
        self.throttle=True
        self.assertTrue(self.dispatch())
        with business_queue.db(self.root,True) as c:
            c.execute("UPDATE chat_receipts SET due=0 WHERE state='ready'")
        self.assertTrue(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages],['file','markdown','markdown'])

    def test_throttled_text_cannot_publish_a_new_card_or_change_accepted_wire_plan(self):
        self.setup_receipt(f"[附件](<{self.path}>)")
        self.reject_upload=True
        self.throttle=True
        self.assertTrue(self.dispatch())
        self.reject_upload=False
        with business_queue.db(self.root, True) as c:
            c.execute("UPDATE chat_receipts SET due=0 WHERE state='ready'")
        self.assertTrue(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages],['markdown','markdown'])
        self.assertEqual(self.messages[0]['markdown'],self.messages[1]['markdown'])
        with business_queue.db(self.root) as c:
            frozen=c.execute('SELECT wire_sha256,files_ok FROM private_feedback_text').fetchone()
            detail=json.loads(c.execute("SELECT detail FROM chat_receipts WHERE phase='final'").fetchone()[0])
            self.assertEqual(tuple(frozen),(detail['body_sha256'],0))
            self.assertEqual(c.execute("SELECT state FROM chat_receipts WHERE phase='final'").fetchone()[0],'file_incomplete')

    def test_transient_media_error_can_retry_before_visible_send(self):
        requests=[]
        def once(args):
            requests.append(tuple(args[:2]))
            if args[:2]==['media','upload'] and len(requests)==1:
                raise TimeoutError()
            return self.cli(args)
        with patch.object(files.time,'sleep'):
            files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,once,lambda:True)
        self.assertEqual(requests.count(('media','upload')),2)
        self.assertEqual([m['msg_type'] for m in self.messages],['file'])

    def test_media_retry_rechecks_owner_and_does_not_repeat_denials(self):
        calls=[]
        def failing(args):
            calls.append(tuple(args[:2]))
            return {'success':False,'errcode':45009}
        with patch.object(files.time,'sleep'):
            with self.assertRaisesRegex(files.FileIssue,'授权已变更'):
                files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,failing,lambda:not calls)
        self.assertEqual(calls,[('media','upload')])
        denied=[]
        def reject(args):
            denied.append(tuple(args[:2]))
            return {'success':False,'errcode':48002}
        with self.assertRaisesRegex(files.FileIssue,'服务码 48002'):
            files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,reject,lambda:True)
        self.assertEqual(denied,[('media','upload')])
        self.assertEqual(self.messages,[])

    def test_file_card_retries_only_definite_throttle_with_same_media(self):
        visible = []
        def throttle(args):
            if args[:3] == ['message','aibot','send']:
                visible.append(json.loads(args[-1]))
                if len(visible) == 1:
                    return {'success': False, 'errcode': 45009}
            return self.cli(args)
        with patch.object(files.time, 'sleep'):
            files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,throttle,lambda:True)
        self.assertEqual(visible, [self.messages[0], self.messages[0]])
        with business_queue.db(self.root) as c:
            self.assertEqual([tuple(r) for r in c.execute("SELECT attempt,state,errcode FROM private_card_attempts ORDER BY attempt")],
                             [(1,'throttled',45009),(2,'accepted',None)])
        files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,throttle,lambda:True)
        self.assertEqual(len(visible), 2)

    def test_file_card_rechecks_before_retry_after_definite_throttle(self):
        def revoke_at_send(args):
            if args[:3] == ['message','aibot','send']:
                self.allow = False
                return {'success':False,'errcode':429}
            return self.cli(args)
        with patch.object(files.time,'sleep'), self.assertRaisesRegex(files.FileIssue, '授权已变更'):
            files.file_card(self.root,self.cid,CLI_OWNER,self.work,self.path,revoke_at_send,lambda:self.allow)
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT count(*) FROM private_card_attempts").fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT state FROM private_file_receipts").fetchone()[0], 'disabled')

    def test_legacy_text_retry_cannot_prepare_new_file_for_original_failure(self):
        self.setup_receipt(f"[附件](<{self.path}>)")
        self.reject_upload = self.throttle = True
        self.assertTrue(self.dispatch())
        self.reject_upload = False
        with business_queue.db(self.root, True) as c:
            c.execute("DELETE FROM private_feedback_text")
            c.execute("UPDATE chat_receipts SET due=0 WHERE state='ready'")
        self.assertTrue(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages],['markdown'])
        with business_queue.db(self.root) as c:
            self.assertEqual(tuple(c.execute("SELECT state,detail FROM chat_receipts WHERE phase='final'").fetchone()),
                             ('unknown','private_retry_binding_changed'))

    def test_file_replacement_cannot_change_original_send_on_retry(self):
        link=f"[附件](<{self.path}>)"
        self.assertTrue(self.render(link)[1])
        self.path.write_bytes(b"new revision")
        self.assertFalse(self.render(link)[1])
        self.assertEqual(len(self.messages),1)

    def test_cleaned_source_does_not_erase_an_accepted_file_receipt(self):
        link=f"[附件](<{self.path}>)"
        self.assertTrue(self.render(link)[1])
        self.path.unlink()
        self.assertTrue(self.render(link)[1])
        self.assertEqual(len(self.messages),1)

    def test_remote_url_and_nonfile_navigation_stay_in_text(self):
        plain = "[在线原件](https://doc.weixin.qq.com/doc/example) 和 [任务](codex://threads/abc)"
        self.assertEqual(self.render(plain),(plain,True))
        self.assertFalse(self.messages)

    def test_technical_paths_in_prose_and_examples_keep_their_meaning(self):
        examples = ["工程目录是 C:/Dev/Project，请用现有工程。",
                    "源码路径为 \x60C:/Dev/Source/Main.cpp\x60。",
                    "\x60\x60\x60text\n[示例](C:/fake/input.pdf)\n\x60\x60\x60"]
        for text in examples:
            self.assertEqual(self.render(text),(text,True))

    def test_link_past_text_limit_is_still_delivered(self):
        self.setup_receipt("长"*8000+f"\n[附件](<{self.path}>)")
        self.assertTrue(self.dispatch())
        self.assertEqual([m['msg_type'] for m in self.messages],['file','markdown'])
        self.assertLessEqual(len(self.messages[1]['markdown']['content'].encode()),20480)

    def test_reference_style_and_bare_paths(self):
        result,good=self.render(f"[源文件][pdf]\n\n[pdf]: <{self.path}>\n\x60{self.path}\x60")
        self.assertTrue(good,result)
        self.assertNotIn("[pdf]",result)
        self.assertNotIn(str(self.work),result)
        self.assertEqual(len(self.messages),1)

    def test_two_workers_reserve_one_visible_send(self):
        self.barrier=threading.Barrier(2)
        link=f"[文件](<{self.path}>)"
        with ThreadPoolExecutor(2) as pool:
            outcomes=list(pool.map(self.render,[link,link]))
        self.assertTrue(any(status for _,status in outcomes))
        self.assertEqual(len(self.messages),1)

    def test_followup_uses_same_private_source(self):
        self.setup_receipt(f"[文件](<{self.path}>)")
        self.dispatch()
        case={'id':'new','chat':CLI_OWNER,'sender':CLI_OWNER,'epoch':time.time()+1}
        with business_queue.db(self.root) as c:
            self.assertEqual(files.recent_sources(c,self.root,case)[0]['source'],str(self.path))
            self.assertEqual(files.recent_sources(c,self.root,{**case,'chat':'another'}),[])

    def test_earlier_final_report_uses_file_card(self):
        row={'msgid':self.cid,'chattype':'single','chatid':CLI_OWNER,'sender':CLI_OWNER}
        text=f"这是本次修改并验收的交付原件，请直接打开本私聊附带的文件卡片。[原件](<{self.path}>)"
        (self.work/'result.txt').write_text(text,encoding='utf-8')
        with local_dispatch.db(self.root) as c:
            c.execute("""INSERT INTO jobs(msgid,row_json,state,created,updated,workspace,kind,context_json,report_mode,final_authorized)
                        VALUES(?,?,'completed','t','t',?,'business','{}','final',1)""",
                      (self.cid,json.dumps(row),str(self.work)))
        with patch.object(local_dispatch,'_completion_proof',return_value={'verified':True}), \
             patch.object(local_dispatch,'_eligible',return_value=True),patch.object(local_dispatch,'cli_call',self.cli):
            self.assertEqual(local_dispatch.finish_report(self.root,self.cid),'server_accepted')
            self.assertEqual(local_dispatch.finish_report(self.root,self.cid),'server_accepted')
        self.assertEqual([m['msg_type'] for m in self.messages],['file','markdown'])

    def test_prepare_deliverable_image_over_10mb_switches_to_file(self):
        big_png = self.work / "outputs" / "big_shot.png"
        with big_png.open("wb") as f:
            f.write(b"\x89PNG\r\n\x1a\n" + b"\x00" * 10_000_000)
        with tempfile.TemporaryDirectory() as tmp:
            send_path, mtype, note = files.prepare_deliverable(big_png, tmp)
            self.assertEqual(send_path, big_png)
            self.assertEqual(mtype, "file")
            self.assertIsNone(note)

    def test_prepare_deliverable_large_compressible_file_zips(self):
        big_txt = self.work / "outputs" / "huge_log.txt"
        big_txt.write_bytes(b"A" * 20_500_000)
        with tempfile.TemporaryDirectory() as tmp:
            send_path, mtype, note = files.prepare_deliverable(big_txt, tmp)
            self.assertEqual(mtype, "file")
            self.assertTrue(send_path.name.endswith(".zip"))
            self.assertLessEqual(send_path.stat().st_size, files.WECOM_FILE_MAX_BYTES)
            self.assertIn("已自动打包压缩", note)

    def test_prepare_deliverable_oversized_incompressible_raises_clear_error(self):
        blob = self.work / "outputs" / "random.bin"
        blob.write_bytes(os.urandom(20_500_000))
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(files.FileIssue, "超过企微单文件 20 MB 上限"):
                files.prepare_deliverable(blob, tmp)

    def test_prepare_deliverable_oversized_video_skips_lossy_compression_by_default(self):
        big_mp4 = self.work / "outputs" / "full_quality_4k.mp4"
        big_mp4.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 20_500_000)
        with tempfile.TemporaryDirectory() as tmp, patch.object(files, "_compress_video") as compress_mock:
            with self.assertRaisesRegex(files.FileIssue, "跳过自动压制并保留本地原片"):
                files.prepare_deliverable(big_mp4, tmp)
            compress_mock.assert_not_called()

    def test_prepare_deliverable_oversized_video_explicit_compress_opt_in(self):
        big_mp4 = self.work / "outputs" / "opt_in.mp4"
        big_mp4.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 20_500_000)
        with tempfile.TemporaryDirectory() as tmp:
            fake_out = Path(tmp) / "opt_in.mp4"
            fake_out.write_bytes(b"\x00" * 10_000_000)
            with patch.object(files, "_compress_video", return_value=fake_out) as compress_mock:
                send_path, mtype, note = files.prepare_deliverable(
                    big_mp4, tmp, allow_lossy_video_compress=True
                )
            compress_mock.assert_called_once()
            self.assertEqual(send_path, fake_out)
            self.assertEqual(mtype, "file")
            self.assertIn("已自动压制", note)

    def test_private_bot_retries_upload_error_formats_40006_and_prevents_duplicate_card(self):
        import private_bot_control

        with business_queue.db(self.root, True) as c:
            c.execute("INSERT INTO cases(id,chat,sender,at,epoch,category,updated,bot_channel) VALUES(?,?,?,'t',?,'work',?,'private_bot')",
                      (self.cid, CLI_OWNER, CLI_OWNER, time.time(), time.time()))
            c.execute("INSERT INTO lanes(case_id,target,state,result) VALUES(?,'local','completed',?)",
                      (self.cid, f"[原件](<outputs/{self.path.name}>)"))
            c.execute("INSERT INTO request_routes VALUES(?,'original','private')", (self.cid,))
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due) VALUES(?,'final','ready',0)", (self.cid,))

        file_calls = []
        md_calls = []

        def fake_send_file(root, chatid, file_path, *, media_type='file', **kw):
            file_calls.append((Path(file_path).name, media_type))
            if len(file_calls) == 1:
                return {'success': False, 'errcode': -3, 'errmsg': 'private_bot_send_timeout', 'stage': 'upload'}
            return {'success': True, 'errcode': 0}

        def fake_send_md(root, chatid, text, **kw):
            md_calls.append(text)
            if len(md_calls) == 1:
                return {'success': False, 'errcode': 45009}
            return {'success': True, 'errcode': 0}

        with patch.object(private_bot_control, 'status', return_value={'authenticated': True}), \
             patch.object(private_bot_control, 'send_file', side_effect=fake_send_file), \
             patch.object(private_bot_control, 'send_markdown', side_effect=fake_send_md), \
             patch.object(chat_feedback.time, 'sleep'):
            self.assertTrue(chat_feedback.deliver_one(self.root, self.cli))
            with business_queue.db(self.root, True) as c:
                c.execute("UPDATE chat_receipts SET due=0 WHERE state='ready'")
            self.assertTrue(chat_feedback.deliver_one(self.root, self.cli))

        # Upload retried once -> succeeded; markdown throttle retry did NOT re-send file card!
        self.assertEqual(len(file_calls), 2)
        self.assertEqual(len(md_calls), 2)
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts WHERE phase='final'").fetchone()[0], 'accepted')

    def test_private_bot_formats_40006_and_never_retries_ambiguous_send_timeout(self):
        import private_bot_control

        cid2 = "w_fixture_40006"
        work2 = files.workspace(self.root, cid2)
        p2 = work2 / "outputs" / "doc.pdf"
        p2.parent.mkdir(parents=True)
        p2.write_bytes(b"%PDF-1.4\ntest\n%%EOF\n")
        with business_queue.db(self.root, True) as c:
            c.execute("INSERT INTO cases(id,chat,sender,at,epoch,category,updated,bot_channel) VALUES(?,?,?,'t',?,'work',?,'private_bot')",
                      (cid2, CLI_OWNER, CLI_OWNER, time.time(), time.time()))
            c.execute("INSERT INTO lanes(case_id,target,state,result) VALUES(?,'local','completed',?)",
                      (cid2, f"[原件](<outputs/{p2.name}>)"))
            c.execute("INSERT INTO request_routes VALUES(?,'original','private')", (cid2,))
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due) VALUES(?,'final','ready',0)", (cid2,))

        send_attempts = []

        def fake_40006(root, chatid, file_path, **kw):
            send_attempts.append(1)
            return {'success': False, 'errcode': 40006, 'errmsg': 'invalid media size', 'stage': 'upload'}

        md_texts = []
        with patch.object(private_bot_control, 'status', return_value={'authenticated': True}), \
             patch.object(private_bot_control, 'send_file', side_effect=fake_40006), \
             patch.object(private_bot_control, 'send_markdown', side_effect=lambda r, t, body: md_texts.append(body) or {'success': True, 'errcode': 0}):
            self.assertTrue(chat_feedback.deliver_one(self.root, self.cli))
        self.assertEqual(len(send_attempts), 1)
        self.assertIn("40006：文件大小不符合企微限制", md_texts[0])

        # Ambiguous timeout during 'send' stage must NOT retry
        cid3 = "w_fixture_send_timeout"
        work3 = files.workspace(self.root, cid3)
        p3 = work3 / "outputs" / "doc2.pdf"
        p3.parent.mkdir(parents=True)
        p3.write_bytes(b"%PDF-1.4\ntest2\n%%EOF\n")
        with business_queue.db(self.root, True) as c:
            c.execute("INSERT INTO cases(id,chat,sender,at,epoch,category,updated,bot_channel) VALUES(?,?,?,'t',?,'work',?,'private_bot')",
                      (cid3, CLI_OWNER, CLI_OWNER, time.time(), time.time()))
            c.execute("INSERT INTO lanes(case_id,target,state,result) VALUES(?,'local','completed',?)",
                      (cid3, f"[原件](<outputs/{p3.name}>)"))
            c.execute("INSERT INTO request_routes VALUES(?,'original','private')", (cid3,))
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due) VALUES(?,'final','ready',0)", (cid3,))

        ambiguous_attempts = []
        with patch.object(private_bot_control, 'status', return_value={'authenticated': True}), \
             patch.object(private_bot_control, 'send_file', side_effect=lambda *a, **kw: ambiguous_attempts.append(1) or {'success': False, 'errcode': -3, 'stage': 'send'}), \
             patch.object(private_bot_control, 'send_markdown', return_value={'success': True, 'errcode': 0}):
            self.assertTrue(chat_feedback.deliver_one(self.root, self.cli))
        self.assertEqual(len(ambiguous_attempts), 1)

    def test_owner_native_group_reply_delivers_outputs_as_file_card_without_raw_link(self):
        import group_replies
        import request_flow
        import t3_dispatch

        gchat = "wrkGroupFixture0000000001"
        cid = "w_group_video_fixture"
        gwork = files.workspace(self.root, cid)
        video = gwork / "outputs" / "preview_side_0.1x.mp4"
        video.parent.mkdir(parents=True, exist_ok=True)
        video.write_bytes(b"\x00\x00\x00\x18ftypmp42fixture-video")

        item = {
            "msgid": cid,
            "sender": CLI_OWNER,
            "chatid": gchat,
            "chattype": "group",
            "direct": "@大肥鱼 录一个侧视图视频",
            "received_at": "2026-11-23T05:00:00Z",
            "parts": [{"type": "text", "text": "@大肥鱼 录一个侧视图视频"}],
            "attachments": [],
        }
        grant = group_replies.describe(self.root, item)
        manifest = {
            "case_id": cid,
            "anchor_msgid": cid,
            "chatid": gchat,
            "sender": CLI_OWNER,
            "group_reply": grant,
            "return_route": "native_original",
            "members": [{"msgid": cid, "chatid": gchat, "chattype": "group", "sender": CLI_OWNER, "direct": "@大肥鱼 录一个侧视图视频", "quotes": []}],
        }
        result_body = f"已录好侧视图视频：\n[preview_side_0.1x.mp4](<outputs/{video.name}>)\n\n画质清晰。"
        result_hash = hashlib.sha256(result_body.encode("utf-8")).hexdigest()
        frozen = json.dumps({"manifest": manifest}, ensure_ascii=False)
        with business_queue.db(self.root, True) as c:
            c.execute("INSERT INTO cases(id,chat,sender,anchor,mode,at,epoch,category,frozen,updated) VALUES(?,?,?,?,'t3','t',?,'work',?,?)",
                      (cid, gchat, CLI_OWNER, cid, time.time(), frozen, time.time()))
            c.execute("INSERT INTO sources(mid,chat,sender,epoch,case_id,item) VALUES(?,?,?,?,?,?)",
                      (cid, gchat, CLI_OWNER, time.time(), cid, json.dumps(item, ensure_ascii=False)))
            c.execute("INSERT INTO request_routes VALUES(?,'native_original','group')", (cid,))
            c.execute("INSERT INTO lanes(case_id,target,state,result) VALUES(?,'local','completed',?)", (cid, result_body))
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?, 'ready', 0, ?)",
                      (cid, group_replies.PHASE, result_body))
        with t3_dispatch.db(self.root) as jc:
            jc.execute(
                "INSERT INTO t3_jobs(msgid,row_json,state,created,updated,workspace,t3_thread_id,result_hash,context_json,context_hash) VALUES(?,?,'completed','t','t',?,'thr_1',?,?,?)",
                (cid, json.dumps(item), str(gwork), result_hash, frozen, "ctx_hash_fixture"),
            )

        sent_msgs = []
        throttle_once = [True]

        def group_cli(args, *, upload_directory=None, download_directory=None):
            if args == ["identity", "whoami"]:
                return {"extra_identity_context": "授权真人用户身份：\nID：" + CLI_OWNER}
            if args == ["message", "aibot", "sessions", "list"]:
                return {"sessions": [{"chat_type": "group", "chat_id": gchat, "chat_name": "测试群"}]}
            if args[:2] == ["media", "upload"]:
                payload = json.loads(args[-1])
                self.assertEqual(payload["type"], "file")
                p = Path(payload["file_path"])
                key = hashlib.sha256(p.read_bytes()).hexdigest()
                self.uploads[key] = (p.name, p.read_bytes())
                return {"media_id": key, "type": "file", "errcode": 0}
            if args[:2] == ["media", "download"]:
                folder = Path(args[args.index("--output-dir") + 1])
                payload = json.loads(args[-1])
                name, data = self.uploads[payload["media_id"]]
                saved = folder / name
                saved.write_bytes(data)
                return {"file_path": str(saved), "errcode": 0}
            self.assertEqual(args[:4], ["message", "aibot", "send", "--json"])
            payload = json.loads(args[4])
            self.assertEqual(payload["chat_id"], gchat)
            sent_msgs.append(payload)
            if payload["msg_type"] == "markdown" and throttle_once[0]:
                throttle_once[0] = False
                return {"success": False, "errcode": 45009}
            return {"success": True, "errcode": 0}

        with patch.object(t3_dispatch, "_completion_proof", return_value={"verified": True, "result_hash": result_hash}), \
             patch.object(chat_feedback, "enabled", return_value=True):
            self.assertTrue(chat_feedback.deliver_one(self.root, group_cli))
            with business_queue.db(self.root, True) as c:
                c.execute("UPDATE chat_receipts SET due=0 WHERE case_id=? AND phase=?", (cid, group_replies.PHASE))
            self.assertTrue(chat_feedback.deliver_one(self.root, group_cli))

        self.assertEqual([m["msg_type"] for m in sent_msgs], ["file", "markdown", "markdown"])
        final_md = sent_msgs[-1]["markdown"]["content"]
        self.assertIn(f"附件《{video.name}》", final_md)
        self.assertNotIn("outputs/", final_md)
        with business_queue.db(self.root) as c:
            self.assertEqual(c.execute("SELECT state FROM chat_receipts WHERE case_id=? AND phase=?", (cid, group_replies.PHASE)).fetchone()[0], "accepted")
            self.assertEqual(c.execute("SELECT files_ok FROM group_feedback_text WHERE case_id=? AND phase=?", (cid, group_replies.PHASE)).fetchone()[0], 1)

    def test_group_result_rejects_files_outside_outputs(self):
        legacy = self.work / self.path.name
        shutil.copyfile(self.path, legacy)
        gchat = "wrkGroupFixture0000000002"
        rendered, ok = files.group_result(
            self.root, self.cid, gchat, f"[泄漏](<{legacy.name}>)",
            files.references(f"[泄漏](<{legacy.name}>)"), self.cli, lambda: True,
        )
        self.assertFalse(ok)
        self.assertIn("只允许交付 outputs 目录内的文件", rendered)
        self.assertFalse(self.messages)

    def test_group_relay_delivers_outputs_as_file_card(self):
        import group_relay
        import request_flow

        gchat = "wrkGroupRelayFixture0001"
        cid = "w_group_relay_video"
        rwork = files.workspace(self.root, cid)
        video = rwork / "outputs" / "relay_clip.mp4"
        video.parent.mkdir(parents=True, exist_ok=True)
        video.write_bytes(b"\x00\x00\x00\x18ftypmp42relay-video")
        draft_body = f"查清了，视频如下：[relay_clip.mp4](<outputs/{video.name}>)"
        with business_queue.db(self.root, True) as c:
            group_relay.install(c)
            c.execute("INSERT INTO cases(id,chat,sender,mode,at,epoch,category,updated) VALUES(?,?,?,'t3','t',?,'work',?)",
                      (cid, CLI_OWNER, CLI_OWNER, time.time(), time.time()))
            draft = group_relay.create_draft(
                c, case_id=cid, bot_channel="feiyu", owner_chat=CLI_OWNER, owner_sender=CLI_OWNER,
                target_chat_id=gchat, target_chat_name="目标测试群", body=draft_body, state="approved_direct", now=time.time(),
            )
            c.execute("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',0,?)",
                      (cid, f"group_relay:{draft['draft_id']}", draft_body))

        sent_msgs = []

        def relay_cli(args, *, upload_directory=None, download_directory=None):
            if args == ["message", "aibot", "sessions", "list"]:
                return {"sessions": [{"chat_type": "group", "chat_id": gchat, "chat_name": "目标测试群"}]}
            if args[:2] == ["media", "upload"]:
                p = Path(json.loads(args[-1])["file_path"])
                key = hashlib.sha256(p.read_bytes()).hexdigest()
                self.uploads[key] = (p.name, p.read_bytes())
                return {"media_id": key, "type": "file", "errcode": 0}
            if args[:2] == ["media", "download"]:
                folder = Path(args[args.index("--output-dir") + 1])
                name, data = self.uploads[json.loads(args[-1])["media_id"]]
                saved = folder / name
                saved.write_bytes(data)
                return {"file_path": str(saved), "errcode": 0}
            payload = json.loads(args[4])
            sent_msgs.append(payload)
            return {"success": True, "errcode": 0}

        with patch.object(chat_feedback, "enabled", return_value=True):
            self.assertTrue(chat_feedback.deliver_one(self.root, relay_cli))
        self.assertEqual([m["msg_type"] for m in sent_msgs], ["file", "markdown"])
        self.assertEqual(sent_msgs[0]["chat_id"], gchat)
        self.assertIn(f"附件《{video.name}》", sent_msgs[1]["markdown"]["content"])
        self.assertNotIn("outputs/", sent_msgs[1]["markdown"]["content"])

if __name__=="__main__":
    unittest.main()
