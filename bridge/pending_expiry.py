"""Expire only work that has never started. All start reservations take the case DB write lock."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import time


TIMEOUT_SECONDS = 600
AUDIT = 'unstarted_expired'
# A timeout handoff and durable local submission precede the launch lease.
PENDING_CASE = """c.category IN ('work','collecting')
    AND NOT EXISTS (SELECT 1 FROM lanes l WHERE l.case_id=c.id AND
        (l.state IS NULL OR l.state NOT IN ('queued','submitted','handed_off') OR l.result<>'' OR l.evidence<>'[]'))
    AND NOT EXISTS (SELECT 1 FROM observations o WHERE o.case_id=c.id
        AND o.source IN ('signed_subscription_read','original_dot_chat','verified_material_outputs'))"""
START_FIELDS = ('lease','pid','worker_identity','thread_id','native_id','cli_pid','cli_identity',
                'turns','workspace','last_event','last_tool','result_hash')
T3_START_FIELDS = ('lease','pid','worker_identity','t3_thread_id','t3_triage_thread_id','turns','workspace','result_hash')
T3_QUEUE_STATES = ('queued','resume_queued','heavy_queued','heavy_resume_queued')
T3_QUEUE_SQL = "('queued','resume_queued','heavy_queued','heavy_resume_queued')"


def attach_native(c, root):
    if any(row[1]=='expiry_native' for row in c.execute('PRAGMA database_list')):
        return True
    file=Path(root)/'local-tasks.sqlite3'
    if not file.is_file():
        return False
    c.execute('ATTACH DATABASE ? AS expiry_native',(str(file.resolve()),))
    return True


def _queue_timeout_filter(c, root):
    from t3_client import load_config
    if load_config(root).get('queue_expiry_exempt', True) is False or not attach_native(c, root):
        return ''
    if not c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
        return ''
    return (" AND NOT EXISTS (SELECT 1 FROM expiry_native.t3_jobs j WHERE j.msgid=c.id"
            f" AND j.state IN {T3_QUEUE_SQL} AND COALESCE(j.lease,'')='')")


@contextmanager
def native_reservation(root):
    from business_queue import db
    with db(root,True) as c:
        if not attach_native(c,root):
            raise RuntimeError('native_queue_missing')
        yield c


def _expire(c,root,case,now):
    cid=case['id']
    # Never expire a case that has an open or answered clarification flow.
    if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='clarifications'").fetchone():
        if c.execute("SELECT 1 FROM clarifications WHERE case_id=? LIMIT 1", (cid,)).fetchone():
            return False
    jobs=[]; t3_jobs=[]
    if attach_native(c,root):
        if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='jobs'").fetchone():
            columns={r[1] for r in c.execute('PRAGMA expiry_native.table_info(jobs)')}
            if {'msgid','state'}<=columns:
                fields=[k for k in ('msgid','kind','state',*START_FIELDS) if k in columns]
                jobs=c.execute('SELECT '+','.join(fields)+' FROM expiry_native.jobs WHERE msgid IN (?,?)',
                               (cid,case['anchor'])).fetchall()
                for job in jobs:
                    if job['state'] not in ('queued','expired') or any(job[k] for k in START_FIELDS if k in job.keys()):
                        return False
                    if job['msgid']!=cid or 'kind' not in fields or job['kind']!='business':
                        return False
        if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
            t3_jobs=c.execute('SELECT msgid,state,lease,pid,worker_identity,t3_thread_id,t3_triage_thread_id,turns,workspace,result_hash FROM expiry_native.t3_jobs WHERE msgid=?',(cid,)).fetchall()
            for tj in t3_jobs:
                if tj['state'] not in ('queued','expired') or any(tj[k] for k in T3_START_FIELDS):
                    return False
    lanes=dict(c.execute('SELECT target,state FROM lanes WHERE case_id=?',(cid,)))
    detail=json.dumps({'timeout_seconds':TIMEOUT_SECONDS,'received_epoch':case['epoch'],
                       'deadline_epoch':case['epoch']+TIMEOUT_SECONDS,'previous_category':case['category'],
                       'previous_lanes':lanes,'previous_native_states':[j['state'] for j in jobs]+[j['state'] for j in t3_jobs]},sort_keys=True)
    c.execute('INSERT INTO observations(case_id,source,at,detail) VALUES(?,?,?,?)',(cid,AUDIT,now,detail))
    category='expired'
    if case['category']=='collecting':
        category='cancelled'
        c.execute("UPDATE message_batches SET state='expired_unassigned' WHERE cid=?",(cid,))
    c.execute('UPDATE cases SET category=?,updated=? WHERE id=?',(category,now,cid))
    c.execute("""UPDATE lanes SET state='expired',changed=? WHERE case_id=?
                 AND state IN ('queued','submitted','handed_off')""",(now,cid))
    if jobs:
        stamp=datetime.fromtimestamp(now,timezone.utc).isoformat()
        c.execute("""UPDATE expiry_native.jobs SET state='expired',status='expired',updated=?
                     WHERE msgid=? AND kind='business' AND state='queued'""",(stamp,cid))
    if t3_jobs:
        stamp=datetime.fromtimestamp(now,timezone.utc).isoformat()
        c.execute("""UPDATE expiry_native.t3_jobs SET state='expired',updated=?
                     WHERE msgid=? AND state='queued'""",(stamp,cid))
    c.execute("""UPDATE group_reply_notes SET state='expired' WHERE case_id=?
                 AND state IN ('waiting','claimed')""",(cid,))
    c.execute("""UPDATE chat_receipts SET state='disabled',finished=?,detail=? WHERE case_id=?
                 AND state IN ('waiting','ready')""",(now,AUDIT,cid))
    import request_flow
    request_flow.expired_notice(c,cid,now)
    return True


def sweep(c,root,now=None):
    now=time.time() if now is None else now
    candidates=c.execute('SELECT c.id,c.anchor,c.category,c.epoch FROM cases c WHERE c.epoch<? AND '+PENDING_CASE+_queue_timeout_filter(c,root),
                         (now-TIMEOUT_SECONDS,)).fetchall()
    count=sum(_expire(c,root,case,now) for case in candidates)
    # Separate WAL files can recover to different commits after a power loss.
    # The business tombstone always fences launch. Retire only an unleased native queue entry.
    if c.execute('SELECT 1 FROM observations WHERE source=? LIMIT 1',(AUDIT,)).fetchone() and attach_native(c,root):
        columns={r[1] for r in c.execute('PRAGMA expiry_native.table_info(jobs)')}
        if {'msgid','kind','state','status','updated',*START_FIELDS}<=columns:
            clean=' AND '.join("COALESCE("+k+",'') IN ('',0)" for k in START_FIELDS)
            c.execute("""UPDATE expiry_native.jobs SET state='expired',status='expired',updated=?
                WHERE state='queued' AND kind='business' AND msgid IN
                (SELECT case_id FROM observations WHERE source=?) AND """+clean,
                (datetime.fromtimestamp(now,timezone.utc).isoformat(),AUDIT))
    return count


def expired(c,root,cid):
    """Under the caller's write lock, settle the deadline before admitting a side effect."""
    if c.execute('SELECT 1 FROM observations WHERE case_id=? AND source=?',(cid,AUDIT)).fetchone():
        return True
    now=time.time()
    case=c.execute('SELECT c.id,c.anchor,c.category,c.epoch FROM cases c WHERE c.id=? AND c.epoch<? AND '+PENDING_CASE+_queue_timeout_filter(c,root),
                   (cid,now-TIMEOUT_SECONDS)).fetchone()
    return bool(case and _expire(c,root,case,now))


def next_deadline(c,now,*,root=None):
    # Overdue rows that have independent native proof are rechecked by normal reconciliation.
    if root is None:
        db_file=c.execute('PRAGMA database_list').fetchone()[2]
        root=Path(db_file).parent if db_file else None
    queue_filter=_queue_timeout_filter(c,root) if root is not None else ''
    return c.execute('SELECT MIN(c.epoch)+? FROM cases c WHERE c.epoch>=? AND '+PENDING_CASE+queue_filter,
                     (TIMEOUT_SECONDS,now-TIMEOUT_SECONDS)).fetchone()[0]


def event_keys(c):
    return [r[0] for r in c.execute("""SELECT 'case:'||case_id FROM observations WHERE source=?
        UNION ALL SELECT 'reply:'||n.id FROM group_reply_notes n JOIN observations o ON o.case_id=n.case_id
        WHERE o.source=?""",(AUDIT,AUDIT))]


def terminal(cid):
    return {'case_id':cid,'state':'expired','claimed':False,'read_only':True,'manifest':{},
            'instruction':'事项超过 10 分钟仍未启动，已过期。'}
