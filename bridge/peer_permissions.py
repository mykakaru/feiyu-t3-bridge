"""Verified, same-room, one-request grants. Only the bridge may bind a sender."""
import hashlib
import json
import re
import time
from identity import resolve

TTL = 3600
CONTROL = re.compile(r'^\s*(?:(?:@大肥鱼|【(?:本地任务|本地|agent|任务|dot|小爱)】)[\s\u200b]*)*【(授权|撤销授权)】\s*(.*)$', re.S|re.I)
BOT = re.compile(r'^\s*@大肥鱼[\s\u200b]*')
REF = re.compile(r'(?<!\w)w_[0-9a-f]{32}(?!\w)')
NEXT = re.compile('下一条|下一个请求|下一次')
def install(c):
    c.execute('''CREATE TABLE IF NOT EXISTS peer_grants(
        owner_mid TEXT PRIMARY KEY, chat TEXT NOT NULL, owner TEXT NOT NULL, peer TEXT NOT NULL,
        reference_mid TEXT NOT NULL, scope TEXT NOT NULL, expires REAL NOT NULL, state TEXT NOT NULL,
        target_mid TEXT UNIQUE, case_mid TEXT UNIQUE)''')
    c.execute('CREATE TABLE IF NOT EXISTS peer_controls(mid TEXT PRIMARY KEY,notice TEXT NOT NULL)')
    c.execute('''CREATE TABLE IF NOT EXISTS peer_changes(
        case_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT NOT NULL)''')

def command(root,item):
    if item['chattype']=='group' and resolve(root,item['sender'])['is_owner']:
        text=item['direct'].strip()
        found=CONTROL.fullmatch(text)
        if found:return found
        for tag in ('【撤销授权】','【授权】'):
            if text.endswith(tag):
                return CONTROL.fullmatch(tag+BOT.sub('',text[:-len(tag)]).strip())

def source(c,mid):
    row=c.execute('SELECT item FROM sources WHERE mid=?',(mid,)).fetchone()
    return json.loads(row[0]) if row else {}

def target(c,item,tail,root):
    ids=set()
    for ref in REF.findall(tail):
        for row in c.execute('SELECT anchor FROM cases WHERE id=? AND chat=?',(ref,item['chatid'])):
            ids.add(row[0])
    for quoted in item['quotes']:
        value=quoted.strip()
        if not value:continue
        # A quote only identifies an actual journal entry. Never trust a pasted name.
        for row in c.execute('''SELECT mid FROM sources WHERE chat=? AND mid<>?
            AND json_extract(item,'$.direct') IN (?,?) LIMIT 2''',
                (item['chatid'],item['msgid'],value,'@大肥鱼 '+value)):
            ids.add(row[0])
    if len(ids)!=1:return {}
    record=source(c,ids.pop())
    if (record.get('chattype')!='group' or record.get('chatid')!=item['chatid']
            or resolve(root,record['sender'])['is_owner']):return {}
    return record

def register(c,root,item,epoch,control):
    from peer_pending import project_paths
    chat,mid=item['chatid'],item['msgid']
    kind,tail=control.groups()
    if kind=='撤销授权':
        person=target(c,item,tail,root)
        if (item['quotes'] or tail.strip()) and not person:
            return '撤销对象未能唯一匹配。请引用该群员的实际发言，或单独发【撤销授权】撤销本群全部待办授权。'
        sql="UPDATE peer_grants SET state='revoked' WHERE chat=? AND state IN ('waiting','bound')"
        cursor=c.execute(sql+' AND peer=?',(chat,person['sender'])) if person else c.execute(sql,(chat,))
        return '本群已撤销 '+str(cursor.rowcount)+' 项尚未开始执行的授权。'
    peer=target(c,item,tail,root)
    if not peer:return '授权尚未生效，目标群员或事项未能唯一匹配。请引用本群成员给大肥鱼的实际发言，再发【授权】。'
    upcoming=bool(NEXT.search(tail))
    if upcoming and not project_paths(tail):
        return '请在下一条指令的一次性授权中写出可改的工程文件路径。也可以等群员发出具体请求后，引用该请求并发【授权】。'
    if not upcoming and c.execute('SELECT 1 FROM peer_grants WHERE target_mid=?',(peer['msgid'],)).fetchone():
        return '这条原始指令已有授权记录，不会重复执行。新的操作请由群员另发一条。'
    scope=tail.strip() or peer['direct']
    if epoch+TTL<=time.time():return '授权消息已过期。'
    if upcoming:
        c.execute("UPDATE peer_grants SET state='revoked' WHERE chat=? AND peer=? AND state='waiting'",(chat,peer['sender']))
    c.execute('INSERT INTO peer_grants VALUES(?,?,?,?,?,?,?,?,?,?)',
              (mid,chat,item['sender'],peer['sender'],peer['msgid'],scope,epoch+TTL,
               'waiting' if upcoming else 'bound',None if upcoming else peer['msgid'],None if upcoming else mid))
    return ('已绑定该群员在本群的一条指令，一小时内有效。改动进入独立待处理 CL。'
            if upcoming else '已授权引用的这条指令，正在核对具体改动范围。')

def intake(c,root,item,epoch):
    from group_replies import addressed_to_bot
    from command_routing import enabled
    if item['chattype']!='group' or not enabled(root):return
    control=command(root,item)
    if control:
        notice=register(c,root,item,epoch,control)
        c.execute('INSERT INTO peer_controls VALUES(?,?)',(item['msgid'],notice))
    elif addressed_to_bot(item) and not resolve(root,item['sender'])['is_owner']:
        from peer_pending import project_paths,requested_ops
        row=c.execute("""SELECT * FROM peer_grants WHERE chat=? AND peer=? AND state='waiting'
                           AND expires>? ORDER BY expires DESC LIMIT 1""",(item['chatid'],item['sender'],max(epoch,time.time()))).fetchone()
        if row and requested_ops(item['direct']):
            if set(project_paths(item['direct']))-set(project_paths(row['scope'])):return
            c.execute("UPDATE peer_grants SET state='bound',target_mid=?,case_mid=? WHERE owner_mid=? AND state='waiting'",
                      (item['msgid'],item['msgid'],row['owner_mid']))

def binding(c,root,mid):
    record=c.execute('SELECT * FROM peer_grants WHERE case_mid=?',(mid,)).fetchone()
    if not record:return None
    owner=source(c,record['owner_mid']);peer=source(c,record['target_mid'])
    if (not owner or not peer or not command(root,owner) or owner['sender']!=record['owner']
          or owner['chatid']!=record['chat'] or peer['chatid']!=record['chat']
          or peer['chattype']!='group' or peer['sender']!=record['peer'] or resolve(root,peer['sender'])['is_owner']):
        return None
    return {**dict(record),'request':peer['direct']}

def decorate(c,root,case,manifest,prompt):
    row=c.execute('SELECT notice FROM peer_controls WHERE mid=?',(case['anchor'],)).fetchone()
    grant=binding(c,root,case['anchor'])
    if not row and not grant:return prompt,None
    if not grant:
        manifest['permission_notice']=row[0]
        return ('请把系统核验结果作为 group_reply 回复原群：'+row[0],True)
    from peer_pending import permitted, snapshots
    allowed,ops=permitted(grant)
    manifest['peer_permission']={'grant':grant['owner_mid'],'task':grant['target_mid'],'same_group':grant['chat'],
        'request':grant['request'],'owner_scope':grant['scope'],'paths':sorted(allowed),'operations':sorted(ops),
        'may_submit':False,'dedicated_pending_change':True,'expires':grant['expires']}
    manifest['project_snapshots']=snapshots(allowed)
    return ('''这是由本人【授权】的单次群员请求。以 peer_permission 内本人范围与群员具体请求的交集为准，只办理所需改动。
你负责审查和拟定操作。保留原有审批、项目规范和数据保密要求。不要使用其他工具写项目，不得提交。
在 finish_business_work 的 result 返回 JSON {"pending_actions":[{"op":"edit","path":"Config/Example.ini","sha256":"原文件快照哈希","text":"完整修改后文本"}]}。
新增用 add，sha256 填 absent。删除用 delete，只含 op/path/sha256。由桥创建专用 pending CL，核验原件、范围和实际打开结果后回群。
缺少足够证据、能力或授权时返回 disposition=blocked 及 {"group_reply":"说明实际缺口"}，不要声称完成。
具体请求：'''+grant['request']+'\n本人限定：'+grant['scope'],True)

def pending_result(root,cid,chat,result,disposition):
    """Reserve a signed case once before touching P4."""
    import business_queue as work
    import peer_pending as files
    import group_replies
    from command_routing import enabled
    from pathlib import Path
    def public(text):return json.dumps({'group_reply':text},ensure_ascii=False)
    with work.db(root,True) as c:
        case=c.execute('''SELECT c.*,l.state AS lane_state FROM cases c JOIN lanes l ON l.case_id=c.id
             AND l.target='dot' WHERE c.id=? AND c.chat=?''',(cid,chat)).fetchone()
        if not case or not case['frozen']:raise ValueError('unknown_case')
        manifest=json.loads(case['frozen'])['manifest']
        if 'permission_notice' in manifest:return public(manifest['permission_notice']),disposition
        frozen=manifest.get('peer_permission')
        if not frozen:return result,disposition
        signature=hashlib.sha256(result.encode()).hexdigest()
        previous=c.execute('SELECT * FROM peer_changes WHERE case_id=?',(cid,)).fetchone()
        if previous:
            if previous['request_hash']!=signature:raise ValueError('immutable_pending_request')
            if previous['state']=='executing':raise ValueError('pending_operation_requires_reconciliation')
            return previous['receipt'],previous['state']
        if disposition=='blocked':
            group_replies.public_text(result,manifest)
            return result,disposition
        def authority(conn,phase):
            current=binding(conn,root,case['anchor'])
            if (not enabled(root) or not current or current['owner_mid']!=frozen['grant'] or current['target_mid']!=frozen['task']
                or current['state']!=phase or current['expires']<=time.time() or current['chat']!=chat):
                raise files.Refused('授权已撤销、过期或不再匹配')
            return current
        if case['lane_state']!='running':raise ValueError('signed_claim_required')
        try:
            value=json.loads(result)
            if not isinstance(value,dict) or set(value)!={'pending_actions'}:raise ValueError('使用 pending_actions 交付计划，无法办理时返回 blocked')
            grant=authority(c,'bound')
            plan=files.validate(value['pending_actions'],grant)
        except files.Refused as err:
            return public('本次授权变更未执行。'+str(err)+'。'),'blocked'
        c.execute("INSERT INTO peer_changes VALUES(?,?,'executing','')",(cid,signature))
        c.execute("UPDATE peer_grants SET state='executing' WHERE owner_mid=? AND state='bound'",(grant['owner_mid'],))
    def recheck():
        with work.db(root) as conn:authority(conn,'executing')
    try:
        cl,count=files.apply(root,cid,plan,grant,recheck)
        reply=public(f'已将本次授权的 {count} 个文件放入独立 pending CL {cl}，已核对文件清单和恢复记录，尚未提交。')
        state='completed'
    except Exception as err:
        recovery=work.load(Path(root)/'peer-change-recovery'/cid/'receipt.json',{})
        cl=recovery.get('pending_cl')
        detail=str(err) if isinstance(err,files.Refused) else '执行状态需要核对'
        suffix=f'请按独立 pending CL {cl} 和本次恢复记录核对已有改动。' if cl else '请按本事项记录核对实际状态。'
        reply=public('办理受阻。'+detail+'。'+suffix);state='blocked'
    with work.db(root,True) as c:
        c.execute("UPDATE peer_changes SET state=?,receipt=? WHERE case_id=? AND request_hash=?",(state,reply,cid,signature))
        c.execute("UPDATE peer_grants SET state='closed' WHERE owner_mid=?",(grant['owner_mid'],))
    return reply,state
