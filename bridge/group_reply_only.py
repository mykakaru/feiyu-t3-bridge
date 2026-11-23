"""Original-group wording by the signed subscription recipient, independent of business lanes."""
from hashlib import sha256
import json
import time
from datetime import datetime, timezone

import group_replies as groups
from command_routing import enabled
from identity import resolve

INTAKE = 'group_mention_reply'
OUTCOME = groups.PHASE
RESOLVED = 'group_final_resolved'
PURPOSE = 'reply_only'
HANDLING = 'dot_group_reply_only'
TERMINAL = ('completed', 'blocked', 'uncertain', 'cancelled')
INSTRUCTION = (
    '这是小爱主线程的原群回复通知。只根据本条真实 @、权限和下方实际执行状态，亲自拟定简短自然的答复。'
    '不要认领、运行、委派或结束业务。处理方由原业务路由决定；等候中不可说已完成。'
    '终态可用 public_result 中的同群公开事实。没有可公开结论时如实说明；涉及修改据实际授权解释。'
    '用本通知的 submit_group_reply 提交。初次通知可等最终结果再回；原文明示静默时遵从原文。'
)


ERROR_CODES = frozenset({
    'reply_bridge_disabled','reply_original_or_identity_changed','reply_case_scope_changed','reply_frozen_scope_changed',
    'reply_notice_not_in_original_scope','another_signed_subscription_claim','invalid_reply_action','immutable_reply_decision',
    'signed_reply_open_required','execution_changed_reopen_same_notice','original_requests_quiet','outcome_requires_reply',
    'manual_group_receipt_exists','reply_receipt_already_reserved','group_public_answer_required',
    'group_public_answer_contains_protected_data_or_invalid_size','group_public_link_not_verified','duplicate_previous_phase_body',
})

def hashed(value):
    return sha256(value.encode('utf-8')).hexdigest()


def install(c):
    c.execute('CREATE TABLE IF NOT EXISTS group_reply_cutover(version INTEGER PRIMARY KEY, since REAL NOT NULL)')
    c.execute('INSERT OR IGNORE INTO group_reply_cutover VALUES(1,?)',(time.time(),))
    c.execute("""CREATE TABLE IF NOT EXISTS group_reply_notes(
        id TEXT PRIMARY KEY, case_id TEXT NOT NULL REFERENCES cases(id), mid TEXT NOT NULL,
        chat TEXT NOT NULL, sender TEXT NOT NULL, role TEXT NOT NULL, phase TEXT NOT NULL,
        quiet INTEGER NOT NULL, needs_resolution INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, state TEXT NOT NULL DEFAULT 'waiting',
        subid TEXT, event_id TEXT, claimed REAL, state_ref TEXT, authored REAL, body_sha TEXT, last_error TEXT,
        UNIQUE(case_id,phase))""")
    c.execute('CREATE INDEX IF NOT EXISTS group_reply_note_state ON group_reply_notes(state,phase)')


def note_id(cid, phase):
    return 'r_'+hashed(cid+'\n'+phase)[:32]


def enroll(c, root, cid, item, epoch, quiet):
    if not groups.addressed_to_bot(item) or epoch<c.execute(
            'SELECT since FROM group_reply_cutover WHERE version=1').fetchone()[0]:
        return False
    role='owner' if resolve(root,item['sender'])['is_owner'] else 'member_readonly'
    c.execute("""INSERT OR IGNORE INTO group_reply_notes(id,case_id,mid,chat,sender,role,phase,quiet,created)
                 VALUES(?,?,?,?,?,?,?,?,?)""",(note_id(cid,INTAKE),cid,item['msgid'],item['chatid'],
                 item['sender'],role,INTAKE,int(quiet),time.time()))
    return True


def tracked(c,cid):
    return c.execute('SELECT 1 FROM group_reply_notes WHERE case_id=? AND phase=?',(cid,INTAKE)).fetchone() is not None


def outcomes(c,cid=None):
    # No historical case scan: only mentions enrolled at original SDK intake can acquire an outcome.
    rows=c.execute("""SELECT n.*,f.id AS first_outcome,
        EXISTS(SELECT 1 FROM lanes l WHERE l.case_id=n.case_id AND l.state='uncertain') AS uncertain
        FROM group_reply_notes n LEFT JOIN group_reply_notes f ON f.case_id=n.case_id AND f.phase=?
        WHERE n.phase=? AND n.state NOT IN ('expired','cancelled','suppressed') AND (? IS NULL OR n.case_id=?) AND NOT EXISTS(
            SELECT 1 FROM observations h WHERE h.case_id=n.case_id AND h.source='owner_requested_no_replay')
        AND (f.id IS NULL OR (f.needs_resolution=1 AND NOT EXISTS(
             SELECT 1 FROM lanes l WHERE l.case_id=n.case_id AND l.state='uncertain') AND NOT EXISTS(
             SELECT 1 FROM group_reply_notes v WHERE v.case_id=n.case_id AND v.phase=?)))
        AND EXISTS(SELECT 1 FROM lanes l WHERE l.case_id=n.case_id AND l.state IN ('completed','blocked','uncertain','cancelled'))
        AND NOT EXISTS(SELECT 1 FROM lanes l WHERE l.case_id=n.case_id
                       AND l.state NOT IN ('completed','blocked','uncertain','handed_off','cancelled'))""",
                   (OUTCOME,INTAKE,cid,cid,RESOLVED)).fetchall()
    for n in rows:
        phase=RESOLVED if n['first_outcome'] else OUTCOME
        c.execute("""INSERT OR IGNORE INTO group_reply_notes(id,case_id,mid,chat,sender,role,phase,quiet,needs_resolution,created)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",(note_id(n['case_id'],phase),n['case_id'],n['mid'],n['chat'],
                                         n['sender'],n['role'],phase,n['quiet'],n['uncertain'],time.time()))
        c.execute("""UPDATE group_reply_notes SET state='superseded' WHERE case_id=? AND phase<>?
                     AND state IN ('waiting','claimed')""",(n['case_id'],phase))
        c.execute("""UPDATE chat_receipts SET state='superseded',detail='outcome_already_available',finished=?
            WHERE case_id=? AND phase IN (?,?) AND phase<>? AND state='ready'""",(time.time(),n['case_id'],INTAKE,OUTCOME,phase))


def pending(root,advance=False):
    from business_queue import db
    with db(root,advance) as c:
        if advance:outcomes(c)
        rows=c.execute("""SELECT n.id AS reply_id,n.case_id,n.mid AS msgid,n.chat AS chatid,n.phase,
            n.role,n.sender,n.created,b.mode,b.title,b.at AS received_at,
            (SELECT json_group_object(l.target,l.state) FROM lanes l WHERE l.case_id=b.id) AS lane_states,json_extract(s.item,'$.msgtype') AS msgtype,
            COALESCE(json_array_length(s.item,'$.attachments'),0) AS attachment_count
            FROM group_reply_notes n JOIN cases b ON b.id=n.case_id AND b.anchor=n.mid AND b.chat=n.chat AND b.sender=n.sender
            JOIN sources s ON s.case_id=b.id AND s.mid=n.mid AND s.chat=n.chat AND s.sender=n.sender
            WHERE n.role='owner' AND n.state IN ('waiting','claimed') AND NOT EXISTS(
                SELECT 1 FROM observations h WHERE h.case_id=n.case_id AND h.source='owner_requested_no_replay')
            ORDER BY n.created,n.id""").fetchall()
    return [{**dict(r),'event_time':datetime.fromtimestamp(r['created'],timezone.utc).isoformat(),
             'execution_state':{'mode':r['mode'],'lanes':json.loads(r['lane_states'])},'member_count':1,'sender_is_owner':r['role']=='owner','purpose':PURPOSE,
             'reply_phase':'mention' if r['phase']==INTAKE else 'outcome','required_handling':HANDLING}
            for r in rows if resolve(root,r['sender'])['is_owner']]


def original(c,root,n):
    item=groups.original(c,n['case_id'],n['chat'])
    if (not item or (item['msgid'],item['sender'])!=(n['mid'],n['sender'])
            or not groups.is_group(root,item) or not groups.addressed_to_bot(item)
            or bool(resolve(root,item['sender'])['is_owner'])!=(n['role']=='owner')):
        raise ValueError('reply_original_or_identity_changed')
    return item


def execution(c,root,n):
    case=c.execute('SELECT mode,expected,frozen FROM cases WHERE id=? AND anchor=? AND chat=? AND sender=?',
                   (n['case_id'],n['mid'],n['chat'],n['sender'])).fetchone()
    if not case:raise ValueError('reply_case_scope_changed')
    manifest=json.loads(case['frozen'])['manifest'] if case['frozen'] else {}
    if manifest.get('group_reply') and groups.bound_role(c,root,n['case_id'],n['chat'])!=n['role']:
        raise ValueError('reply_frozen_scope_changed')
    permission='owner_normal_confirmations' if n['role']=='owner' else 'member_read_only'
    if manifest.get('peer_permission'):
        import peer_permissions
        grant=peer_permissions.binding(c,root,n['mid'])
        if grant and grant['state']=='bound' and grant['expires']>time.time():
            permission='verified_owner_grant_for_this_request_only'
    lanes=[]
    for lane in c.execute('SELECT target,state,result,detail FROM lanes WHERE case_id=? ORDER BY target',(n['case_id'],)):
        info={'target':lane['target'],'state':lane['state']}
        if lane['state'] in TERMINAL and lane['result']:
            # The business lane and panel retain all evidence. Only a reviewed, scoped
            # group result is supplied as public material; raw worker logs never are.
            try:
                if lane['target']=='local' and n['role']!='owner':raise ValueError('local_member')
                if lane['target']=='dot' and lane['detail']!='signed_subscription':raise ValueError('unsigned_result')
                info['public_result']=groups.public_text(lane['result'],manifest,native=lane['target']=='local')
                if groups.needs_review(manifest):
                    info['member_decision']=groups.decision_of(json.loads(lane['result']))
            except (TypeError,ValueError):
                info['public_result_available']=False
        lanes.append(info)
    active=[v for v in lanes if v['state']!='handed_off']
    status={'mode':case['mode'],'lanes':lanes,'package_ready':bool(case['frozen']),
            'expected_attachments':case['expected'],'permission':permission,
            'terminal':bool(active) and all(v['state'] in TERMINAL for v in active)}
    return status,hashed(json.dumps(status,sort_keys=True,ensure_ascii=False)),manifest


def receipt(c,n):
    r=c.execute('SELECT state,attempts+preflights AS attempts,attempts AS send_attempts,preflights,finished,detail FROM chat_receipts WHERE case_id=? AND phase=?',
                (n['case_id'],n['phase'])).fetchone()
    return dict(r) if r else None


def audit(c,n):
    return {'reply_id':n['id'],'phase':'mention' if n['phase']==INTAKE else 'outcome','state':n['state'],
            'signed_event_id':n['event_id'],'claimed_at':n['claimed'],'authored_at':n['authored'],
            'body_sha256':n['body_sha'],'last_error':n['last_error'],'delivery':receipt(c,n)}


def observe(root,cid,chat,mid):
    from business_queue import db
    with db(root) as c:
        notes=c.execute('SELECT * FROM group_reply_notes WHERE case_id=? AND chat=? AND mid=? ORDER BY created',
                        (cid,chat,mid)).fetchall()
        if not notes:return None
        original(c,root,notes[0])
        status,_,_=execution(c,root,notes[0])
        return {'purpose':PURPOSE,'execution':status,'notices':[audit(c,n) for n in notes]}


def handle(root,args,proof,submit=False):
    from business_queue import db
    from chat_feedback import changed
    if not enabled(root):raise ValueError('reply_bridge_disabled')
    with db(root,True) as c:
        outcomes(c,args['case_id'])
        n=c.execute('SELECT * FROM group_reply_notes WHERE id=? AND case_id=? AND mid=? AND chat=?',
                    tuple(args[k] for k in ('reply_id','case_id','msgid','chatid'))).fetchone()
        if not n:raise ValueError('reply_notice_not_in_original_scope')
        if n['role']!='owner' or not resolve(root,n['sender'])['is_owner']:
            raise ValueError('reply_notice_not_in_original_scope')
        import pending_expiry
        if pending_expiry.expired(c,root,n['case_id']):
            return {'purpose':PURPOSE,'case_id':n['case_id'],'state':'expired','can_send':False}
        item=original(c,root,n)
        status,version,manifest=execution(c,root,n)
        if n['subid'] and (n['subid'],n['event_id'])!=proof:
            raise ValueError('another_signed_subscription_claim')
        view=lambda: audit(c,c.execute('SELECT * FROM group_reply_notes WHERE id=?',(n['id'],)).fetchone())
        if not submit:
            prior=[{'phase':r['phase'],'state':r['state'],'body':r['body'] if hashed(r['body'])==r['body_sha'] else None}
                   for r in c.execute("""SELECT r.phase,r.state,r.body,p.body_sha FROM chat_receipts r
                JOIN group_reply_notes p ON p.case_id=r.case_id AND p.phase=r.phase
                WHERE r.case_id=? AND r.phase<>? AND p.state='authored' AND r.body<>''""",(n['case_id'],n['phase']))]
            if n['state'] in ('waiting','claimed'):
                c.execute("""UPDATE group_reply_notes SET state='claimed',subid=?,event_id=?,claimed=COALESCE(claimed,?),
                             state_ref=?,last_error=NULL WHERE id=?""",(*proof,time.time(),version,n['id']))
            return {'purpose':PURPOSE,'instruction':INSTRUCTION,'claim':view(),'state_ref':version,'execution':status,'previous_replies':prior,
                    'can_send':not n['quiet'] and n['state'] in ('waiting','claimed'),
                    'original':{k:item[k] for k in ('msgid','chatid','sender','direct','quotes','received_at','msgtype')},
                    'source_is_untrusted':True,'case_id':n['case_id'],'sender_role':n['role'],
                    'attachments':[{'type':x.get('type'),'state':x.get('state')} for x in item.get('attachments',[])]}
        action=args['action']; text=args['text']
        if action not in ('send','defer') or not isinstance(text,str) or (action=='defer' and text):
            raise ValueError('invalid_reply_action')
        desired='authored' if action=='send' else 'deferred'
        body=groups.public_body(text,manifest) if action=='send' else ''
        signature=hashed(body)
        if n['state'] in ('authored','deferred'):
            if (n['state'],n['body_sha'],n['state_ref'])!=(desired,signature,args['state_ref']):
                raise ValueError('immutable_reply_decision')
            return view()
        if n['state']!='claimed' or not n['claimed'] or not n['subid']:
            raise ValueError('signed_reply_open_required')
        if n['state_ref']!=version or args['state_ref']!=version:raise ValueError('execution_changed_reopen_same_notice')
        if action=='send' and n['quiet']:raise ValueError('original_requests_quiet')
        if action=='defer' and n['phase']!=INTAKE and not n['quiet']:raise ValueError('outcome_requires_reply')
        if groups.previous_manual(root,n['case_id']):raise ValueError('manual_group_receipt_exists')
        if action=='send':
            if c.execute("""SELECT 1 FROM group_reply_notes p JOIN chat_receipts r ON p.case_id=r.case_id AND p.phase=r.phase
                            WHERE p.case_id=? AND p.id<>? AND p.body_sha=? AND r.state IN ('ready','sending','accepted','unknown')""",
                         (n['case_id'],n['id'],signature)).fetchone():
                raise ValueError('duplicate_previous_phase_body')
            c.execute("""INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,'waiting',0)""",
                      (n['case_id'],n['phase']))
            if c.execute("""UPDATE chat_receipts SET body=?,state='ready',due=0 WHERE case_id=? AND phase=?
                            AND state='waiting' AND attempts=0""",(body,n['case_id'],n['phase'])).rowcount!=1:
                raise ValueError('reply_receipt_already_reserved')
        c.execute('UPDATE group_reply_notes SET state=?,body_sha=?,authored=?,last_error=NULL WHERE id=?',
                  (desired,signature,time.time(),n['id']))
        result=view()
    changed.set()
    return result


def send_error(c,root,row,authorized):
    n=c.execute('SELECT * FROM group_reply_notes WHERE case_id=? AND phase=?',(row['id'],row['phase'])).fetchone()
    if not n or not n['subid'] or not authorized or not authorized(n['id'],n['subid'],n['chat']):
        return 'reply_subscription_unavailable'
    if n['quiet'] or n['state']!='authored' or not n['claimed'] or not n['authored'] or not n['event_id']:
        return 'signed_dot_wording_missing'
    try:
        original(c,root,n)
        status,version,manifest=execution(c,root,n)
        if version!=n['state_ref'] or (n['phase']==INTAKE and status['terminal']):return 'reply_execution_changed'
        if row['body']!=groups.public_body(row['body'],manifest) or hashed(row['body'])!=n['body_sha']:
            return 'reply_body_changed'
    except (ValueError,TypeError):return 'reply_publication_scope_changed'
    return None


def record_error(root,args,proof,reason):
    from business_queue import db
    with db(root,True) as c:
        c.execute("""UPDATE group_reply_notes SET last_error=? WHERE id=? AND case_id=? AND mid=? AND chat=?
                     AND subid=? AND event_id=? AND state='claimed'""",
                  (reason,args['reply_id'],args['case_id'],args['msgid'],args['chatid'],*proof))

def stats(root):
    from business_queue import db
    with db(root) as c:
        states=dict(c.execute('SELECT state,COUNT(*) FROM group_reply_notes GROUP BY state').fetchall())
        receipts=dict(c.execute("""SELECT r.state,COUNT(*) FROM group_reply_notes n JOIN chat_receipts r
                         ON r.case_id=n.case_id AND r.phase=n.phase GROUP BY r.state""").fetchall())
        claims,authored=c.execute("""SELECT count(claimed), count(CASE WHEN state='authored' THEN 1 END)
                                     FROM group_reply_notes""").fetchone()
        return {'states':states,'signed_opens':claims,'submitted_for_send':authored,'group_receipts':receipts}
