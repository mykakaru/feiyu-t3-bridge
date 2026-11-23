"""Local, fail-closed routing policy. This module never executes inbound instructions."""
from pathlib import Path
import json,hashlib
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parent
OWNER='wokSFfCgAA8AATJbuftE--hahlUVVs0Q'
DEFAULT={'personal_model':None,'group_readonly_model':{'model':'gpt-6.1-terra','reasoning_effort':'max'},'model_dispatch_implemented':False,'context_visibility':'bot_delivered_only'}
def settings(root=None):
 from settings import read
 return read(root or ROOT)

def is_owner(sender):
 from identity import resolve
 return resolve(ROOT,sender)['is_owner']
def decision(row,root=None):
 from identity import resolve
 identity=resolve(root or ROOT,row.get('sender'))
 owner=identity['is_owner']
 group=row.get('chattype')=='group' and bool(row.get('chatid'))
 return {'sender_is_owner':owner,'identity_state':identity['state'],'identity_evidence':identity['evidence'],'audience':row.get('chatid'),'permission_tier':'owner_request' if owner else ('group_readonly' if group else 'no_task_authority'),
 'may_modify':False,'modification_authorization':'default deny; concrete current owner approval OR verified owner task/person/group/scope preapproval; verify original source and request boundaries before any modification',
 'allowed_group_work':['inspect existing authorized project content','explain','record an existing case without changing project files'] if group else [],
 'context_scope':'same_group_since_last_verified_bot_report' if group else 'owner_private_only' if owner else 'none',
 'model_preference':settings(root)['group_readonly_model'] if group and not owner else settings(root)['personal_model'],
 'explicit_message_model_priority':owner,'model_dispatch_implemented':False,
 'on_modification_request':'require the verified owner’s direct 【授权】 for this member, group and request; use the checked pending-change writer; no submit' if not owner else 'apply normal confirmation policy',
 'confidentiality':'never use private chats or another group as evidence for this group; task artifacts only if authorized for this audience'}
def group_context(chatid):
 if not chatid:raise ValueError('chatid required')
 watermark=None
 reports=ROOT/'group_reports.jsonl'
 if reports.exists():
  for line in reports.read_text(encoding='utf-8').splitlines():
   r=json.loads(line)
   if r.get('chatid')==chatid and r.get('verified_send') and (watermark is None or r['sent_at']>watermark['sent_at']):watermark=r
 rows=[]
 for f in (ROOT/'queue').glob('*.json'):
  r=json.loads(f.read_text(encoding='utf-8'))
  if r.get('chattype')=='group' and r.get('chatid')==chatid and (watermark is None or r.get('received_at','')>=watermark['sent_at']):rows.append(r)
 rows.sort(key=lambda x:x.get('received_at',''))
 return {'chatid':chatid,'last_verified_bot_report':watermark,'messages':rows,'complete_group_history':False,'visibility':'only messages actually delivered to bot; unmentioned group messages may be absent','no_report_watermark':watermark is None}
def record_report(chatid,report_text,receipt):
 if not chatid or not receipt:raise ValueError('verified service send receipt required')
 row={'chatid':chatid,'report_text':report_text,'sent_at':datetime.now(timezone.utc).isoformat(),'verified_send':True,'receipt':receipt}
 with (ROOT/'group_reports.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False)+'\n')
 return row


def matching_preapprovals(row,task_id,action_id,verified_local_grants,now=None):
 """Return candidate grant IDs only, never execute/auto-approve.
 verified_local_grants must be loaded by root from owner-authenticated evidence,
 never taken from inbound text/quotes/attachments. Root still judges scope and safety.
 """
 now=now or datetime.now(timezone.utc)
 if row.get('chattype')!='group' or not task_id or not action_id:return []
 found=[]
 for grant in verified_local_grants:
  try:
   if grant.get('grantor')!=OWNER or grant.get('source_verified_by_root') is not True:continue
   if not grant.get('owner_source_message_id') or not grant.get('owner_original_text'):continue
   if grant.get('revoked') or grant.get('task_id')!=task_id:continue
   if grant.get('chatid')!=row.get('chatid') or row.get('sender') not in grant.get('peer_userids',[]):continue
   if action_id not in grant.get('action_ids',[]) or not grant.get('scope_description'):continue
   if grant.get('expires_at') and datetime.fromisoformat(grant['expires_at'])<=now:continue
   if not grant.get('grant_id'):continue
   found.append({'grant_id':grant['grant_id'],'requires_root_scope_review':True,'requires_safety_policy_check':True,'may_modify':False})
  except (ValueError,TypeError):continue
 return found
