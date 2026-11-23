"""Prospective feedback policy for authenticated requests. Never enroll stored history."""
import hashlib
import json
import re
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import group_replies
from identity import resolve

ACK = 'origin_received'
EXPIRED = 'unstarted_expiry_notice'
OWNER_REVIEW = 'owner_review'
MEMBER_STATUS = 'member_status'
OWNER_AUTHORIZATION = 'owner_authorization'
REVIEW_PENDING = 'member_review_pending'
REVIEW_AUTHORIZED = 'member_review_authorized'
REVIEW_NOTICES = (REVIEW_PENDING, REVIEW_AUTHORIZED)
REVIEW_WAIT_SECONDS = 300
TAG = re.compile(r'【回报[：:]\s*([^】]{1,60})】')
PRIVATE = re.compile(r'(?:结果|结论|回报|报告|答复)(?:请|只|仅|最后|直接|改为|改到|改)?(?:发(?:到|回|给)?|回复到?|回到?|送到)?(?:本人私聊|我的私聊|我私聊|企微私聊|私聊给我|私发给我)|(?:私聊|私发)(?:回报|发)?给我')
OTHER = re.compile(r'(?:结果|结论|回报|报告|答复).{0,9}?(?:发(?:给|到|至|往)|回到|投递到|同步到|贴到)\s*([^，。；！\n]{1,45})')
QUIET = re.compile(r'无需回复|不用回复|不要回复|不用回报|不要回报|不要回群|不用回群|仅面板|只在面板|do not reply|no reply needed',re.I)


def installed(root, epoch):
    try:
        start = json.loads((Path(root)/'request-flow.json').read_text(encoding='utf8'))['enabled_since_epoch']
        return type(start) in (float,int) and 0 < start <= epoch
    except (OSError, KeyError, TypeError, ValueError):
        return False


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS request_routes(
        case_id TEXT PRIMARY KEY REFERENCES cases(id), destination TEXT NOT NULL, origin_kind TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS feedback_alerts(
        case_id TEXT, phase TEXT, reason TEXT, first_seen REAL, updated REAL, resolved REAL,
        PRIMARY KEY(case_id,phase,reason))""")
    c.execute("""CREATE TABLE IF NOT EXISTS feedback_attempts(
        case_id TEXT,phase TEXT,attempt INTEGER,started REAL,target_sha256 TEXT,body_sha256 TEXT,
        state TEXT,detail TEXT,finished REAL,PRIMARY KEY(case_id,phase,attempt))""")


def member_group(root, item):
    return item.get('chattype') == 'group' and not resolve(root, item.get('sender'))['is_owner']


def review_notice_text(cid, phase):
    label = '〔' + cid[-7:] + '〕'
    if phase == REVIEW_PENDING:
        return label + '授权人暂未受理。'
    return '已授权' + label + '，答复将回本群。'


def member_status(c, item, label):
    matches = c.execute("""SELECT b.category,p.review_case AS auto_reply,g.state AS reply_state
        FROM cases b JOIN request_routes r ON r.case_id=b.id
        LEFT JOIN owner_group_publications p ON p.review_case=b.id
        LEFT JOIN chat_receipts g ON g.case_id=b.id AND g.phase=?
        WHERE b.chat=? AND b.sender=? AND substr(b.id,-7)=? AND r.destination=?""",
        (group_replies.PHASE,item['chatid'],item['sender'],label,OWNER_REVIEW)).fetchall()
    if len(matches) != 1:
        return '本群没有与你匹配的事项编号。'
    case = matches[0]
    if case['category'] in ('owner_review', 'cancelled'):
        return None  # Pending requests have one scheduled notice. Declines are silent.
    if case['auto_reply']:
        return {'accepted':'已回报到本群。', 'unknown':'答复发送状态待核实。',
                'disabled':'回报受阻，待授权人处理。', 'rejected':'回报受阻，待授权人处理。',
                'unavailable':'回报受阻，待授权人处理。'}.get(case['reply_state'],'已获授权，结果将回本群。')
    return '已由授权人接手，结果先交本人审核。'


def register_member_request(c, root, item, epoch):
    from business_queue import make_case
    from owner_review import question_text

    if not group_replies.is_group(root, item) or not group_replies.addressed_to_bot(item):
        return
    question = question_text(item)
    if not question:
        return
    lookup = re.fullmatch(r'【进度】\s*([a-f0-9]{7})', question, re.I)
    kind = MEMBER_STATUS if lookup else OWNER_REVIEW
    cid = make_case(c, item, kind, category='control' if lookup else OWNER_REVIEW,
                    note='群友来信由桥登记，等授权人确认是否接手。')
    c.execute('INSERT INTO request_routes VALUES(?,?,?)', (cid, kind, 'group'))
    phase, due = ACK, epoch
    if lookup:
        body = member_status(c, item, lookup[1].lower())
        if body is None:
            return
    else:
        if destination(item, False) == 'quiet':
            return
        crisis = re.search(r'我(?:想|要|准备)(?:去死|自杀|结束自己的生命|伤害自己)', question)
        if crisis:
            body = '如果你现在有伤害自己的危险，请立即联系当地急救，找一位能到你身边的可信任的人陪同。'
        else:
            phase, due = REVIEW_PENDING, epoch + REVIEW_WAIT_SECONDS
            body = review_notice_text(cid, phase)
    c.execute("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
              (cid, phase, due, body))


def still_pending(c, cid):
    return c.execute("SELECT 1 FROM cases WHERE id=? AND category='owner_review'", (cid,)).fetchone() is not None


def destination(item, owner):
    text = item['direct']
    tags = TAG.findall(text)
    if tags:
        if len(tags) != 1:
            return 'needs_destination'
        tag = tags[0].strip()
        if tag in ('仅面板','面板','静默'):
            return 'quiet'
        if tag in ('原路','原群','原私聊','原会话'):
            return 'original'
        if tag in ('本人私聊','我的私聊','私聊给我') and owner:
            return 'owner_private'
        if tag=='执行者原群' and owner and item['chattype']=='group':
            return 'native_original'
        return 'needs_destination'
    if PRIVATE.search(text):
        return 'owner_private' if owner else 'needs_destination'
    if re.search(r'(结果|结论|报告|答复|回报).{0,7}(私聊我|私发我|单独发我|只发给我|私下给我)|别回群.{0,8}私聊我',text):
        return 'owner_private' if owner else 'needs_destination'
    if QUIET.search(text):
        return 'quiet'
    if owner and item['chattype'] == 'single':
        import group_relay
        target_spec = group_relay.parse_group_target(text, item.get('quotes', ()))
        if target_spec:
            return 'cross_group_direct' if target_spec['mode'] == 'direct' else 'cross_group_draft'
    for match in OTHER.finditer(text):
        value = match[1].strip(' ，。！')
        value_low = value.lower()
        same = ('原会话','原路','这里','本会话')
        if item['chattype'] == 'group':
            same += ('原群','本群','这个群','当前群','群里')
        else:
            same += ('原私聊','本私聊','我','本人')
            if item.get('bot_channel') == 'private_bot':
                same += ('claude', '克劳德', 'claude私聊', '克劳德私聊', '克劳德bot', 'claude bot')
            else:
                same += ('大肥鱼', '大肥鱼私聊')
        if value_low not in same:
            return 'needs_destination'
    if (item['chattype']=='single' and re.search(r'(发群里|发到群里|回到群里|原群回报|由大肥鱼发群)',text)
            or re.search(r'(?:结果|答复|回报|结论).{0,10}(?:在小爱|在另.{0,4}会话|用邮件|发邮件|别发原|不要在原|换个群)',text)):
        return 'needs_destination'
    return 'original'


def enroll(c, root, cid, item, epoch):
    if not installed(root,epoch):
        return False
    owner = resolve(root,item['sender'])['is_owner']
    group = group_replies.is_group(root,item) and group_replies.addressed_to_bot(item)
    private = owner and item['chattype']=='single' and item['sender']==item['chatid']
    if not (group or private):
        return False
    dest = destination(item,owner)
    mode = c.execute('SELECT mode FROM cases WHERE id=?',(cid,)).fetchone()[0]
    if dest=='native_original':
        if not (group and owner and mode in ('local','t3')):dest='needs_destination'
    if group and owner and mode=='t3' and dest=='original':
        dest='native_original'
    c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)',(cid,dest,'group' if group else 'private'))
    if dest=='quiet':
        return True
    phase = ACK if group and dest!='owner_private' else 'first'
    c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,'ready',?)",(cid,phase,epoch))
    if dest=='needs_destination':
        alert(c,cid,'final','destination_confirmation_required')
        return True
    if group and dest=='native_original':
        final=group_replies.PHASE
    elif group and dest=='original':
        import group_reply_only as notes
        if not notes.enroll(c,root,cid,item,epoch,False):
            raise ValueError('group_notice_not_enrolled')
        c.execute("UPDATE group_reply_notes SET state='bridge_ack' WHERE case_id=? AND phase=? AND state='waiting'",(cid,notes.INTAKE))
        final = group_replies.PHASE
    else:
        final = 'final'
    c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,'waiting',?)",(cid,final,epoch))
    return True


def route(c,cid):
    return c.execute('SELECT * FROM request_routes WHERE case_id=?',(cid,)).fetchone()


def owner_private(c,root,row):
    r=route(c,row['id'])
    if not r or r['destination']!='owner_private' or r['origin_kind']!='group':
        return False
    item=group_replies.original(c,row['id'],row['chat'])
    return group_replies.owner_mention(root,item) and destination(item,True)=='owner_private'


def group_ack(c,root,row,visible):
    item=group_replies.original(c,row['id'],row['chat'])
    r=route(c,row['id'])
    if not r or r['origin_kind']!='group' or r['destination'] not in (
            'original','native_original','needs_destination','control',
            OWNER_REVIEW,MEMBER_STATUS,OWNER_AUTHORIZATION,'conclusion_note','colleague_followup'):
        return False
    if not group_replies.is_group(root,item) or not group_replies.addressed_to_bot(item):
        return False
    # Static zero-LLM conclusion notes and colleague follow-up receipts never depend on Dot.
    if r['destination'] in ('conclusion_note','colleague_followup'):
        return True
    return bool(group_replies.owner_mention(root,item) or (visible and visible(row['chat'])))


def ack_text(c,row):
    route_row=route(c,row['id'])
    if route_row and route_row['destination'] in (OWNER_REVIEW,MEMBER_STATUS,OWNER_AUTHORIZATION,'conclusion_note','colleague_followup'):
        return row['body']
    if route_row and route_row['destination']=='control':
        from task_controls import control_receipt
        return control_receipt(c,row['id'])
    suffix='，回报目的地待确认' if route_row and route_row['destination']=='needs_destination' else ''
    model_hint=''
    if row['mode']=='t3':
        if row['frozen']:
            try:
                ctx=json.loads(row['frozen'])
                exp=ctx.get('explicit_model')
                if exp:
                    eff=f" ({exp['effort']})" if exp.get('effort') else ''
                    tier=f" {exp['service_tier']}" if exp.get('service_tier') else ''
                    model_hint=f"，已按指定模型 {exp['model']}{eff}{tier} 派入 T3"
                else:
                    m=ctx.get('model') or 'gemini-4-hydrogen'
                    e=ctx.get('reasoning_effort') or 'high'
                    model_hint=f"，已派入 T3（{m} {e}）初步评估"
            except (ValueError,TypeError):
                model_hint='，已派入 T3 评估办理'
        else:
            model_hint='，已派入 T3 评估办理'
        from command_routing import RETIRED_PREFIX
        if RETIRED_PREFIX.match((row['title'] or '').lstrip()):
            model_hint+='（提示：旧路由前缀已停用，现直接发需求即可）'
        note_text=(row['note'] or '').strip() if 'note' in row.keys() else ''
        m_fb=re.match(r'^原事项〔([0-9a-f]{7})〕已结束，已携引用上下文作为新任务办理。$',note_text,re.I)
        m_hist=re.match(r'^引用历史收据〔([0-9a-f]{7})〕上下文，已作为新任务办理。$',note_text,re.I)
        m_relay=re.match(r'^原 Agent〔(A-[0-9a-f]{4,8})〕([^，。]{1,40})，已自动转为新任务办理。$',note_text,re.I)
        if m_fb:
            model_hint+=f'（原事项〔{m_fb.group(1)}〕已结束，已携引用上下文按新任务办理）'
        elif m_hist:
            model_hint+=f'（已携引用历史收据〔{m_hist.group(1)}〕上下文按新任务办理）'
        elif m_relay:
            model_hint+=f'（原 Agent〔{m_relay.group(1).upper()}〕{m_relay.group(2)}，已自动转为新任务办理）'
    return '已收到〔'+row['id'][-7:]+'〕'+model_hint+suffix+'。'


def approved_native(c,root,row):
    r=route(c,row['id'])
    if not r or r['destination']!='native_original':return False
    item=group_replies.original(c,row['id'],row['chat'])
    if (row['mode'] not in ('local','t3') or not group_replies.owner_mention(root,item)
            or destination(item,True) not in ('native_original','original')
            or group_replies.bound_role(c,root,row['id'],row['chat'])!='owner'):
        return False
    if not group_replies.approved_body(c,root,row):return False
    result=c.execute("SELECT result FROM lanes WHERE case_id=? AND target='local' AND state='completed'",(row['id'],)).fetchone()
    if not result:return False
    if row['mode']=='t3':
        import t3_dispatch
        with closing(sqlite3.connect((Path(root)/'local-tasks.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)) as jobs:
            jobs.row_factory=sqlite3.Row
            job=jobs.execute("SELECT * FROM t3_jobs WHERE msgid=? AND state='completed'",(row['id'],)).fetchone()
        if not job:return False
        proof=t3_dispatch._completion_proof(job)
        return bool(proof and proof['result_hash']==hashlib.sha256(result[0].encode()).hexdigest())
    from local_dispatch import _completion_proof, digest
    with closing(sqlite3.connect((Path(root)/'local-tasks.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)) as jobs:
        jobs.row_factory=sqlite3.Row
        job=jobs.execute("SELECT * FROM jobs WHERE msgid=? AND kind='business' AND state='completed'",(row['id'],)).fetchone()
    if not job or digest(json.loads(row['frozen']))!=job['context_hash']:return False
    proof=_completion_proof(job)
    return bool(proof and proof['result_hash']==hashlib.sha256(result[0].encode()).hexdigest())


def alert(c,cid,phase,reason):
    now=time.time()
    c.execute("""INSERT INTO feedback_alerts VALUES(?,?,?,?,?,NULL)
        ON CONFLICT(case_id,phase,reason) DO UPDATE SET updated=excluded.updated,resolved=NULL
        WHERE feedback_alerts.resolved IS NOT NULL""",(cid,phase,reason,now,now))


def expired_notice(c,cid,now):
    r=route(c,cid)
    if not r or r['destination']=='quiet':return
    alert(c,cid,'work','expired_before_start')
    if c.execute("SELECT 1 FROM chat_receipts WHERE case_id=? AND phase IN ('first',?) AND state='accepted'",(cid,ACK)).fetchone():
        c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,'ready',?)",(cid,EXPIRED,now))


def receipt_detail(data, text, target):
    fields={k:data[k] for k in ('success','errcode','req_id','msgid','message_id') if k in data and isinstance(data[k],(str,int,bool))}
    fields.update(body_sha256=hashlib.sha256(text.encode()).hexdigest(),target_sha256=hashlib.sha256(target.encode()).hexdigest())
    return json.dumps(fields,separators=(',',':'))


def feedback_state(c,cid):
    return [dict(row) for row in c.execute('SELECT phase,state,attempts,preflights,detail,finished FROM chat_receipts WHERE case_id=? ORDER BY due',(cid,))]
