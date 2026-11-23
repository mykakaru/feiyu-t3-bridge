"""Fetch a bounded ordinary WeCom document explicitly requested by its verified owner."""
from datetime import datetime,timezone
import json
import re
import subprocess
from urllib.parse import urlsplit

from identity import resolve

LINK=re.compile(r'https://doc\.weixin\.qq\.com/doc/([A-Za-z0-9_-]{5,180})(?=$|[?\s/),，。；;！!？）\]#>])',re.I)
READ=re.compile(r'读[一回取出下]|读取|核对|查看|打开.{0,36}文档|标题.{0,15}(?:正文|什么)|正文.{0,15}(?:什么|编号|标题)|文档.{0,8}写了什么')
WRITE=re.compile(r'不要读|无需读|不用读|别读|禁止读|假设|举例|不必读|请勿|写入|新建|追加|覆盖|替换|删除|转发|搬运|发给其他|授权|分享|翻译成|如果|能不能读|是否可以读')
def document_id(root,row,context):
    manifest=context.get('manifest',{})
    if (row.get('chattype')!='single' or row.get('chatid')!=row.get('sender')
            or not resolve(root,row.get('sender'))['is_owner']
            or manifest.get('chat_delivery')!='bridge_reports_first_and_final'):
        return None
    parts=[p.get('text','') for p in row.get('parts',[]) if not p.get('quoted') and p.get('type')=='text']
    text='\n'.join(parts)
    if (not text or len(text)>1400 or not READ.search(text) or WRITE.search(text)
            or any(x.get('quoted') for x in row.get('parts',[]))):return None
    try:
        if any(urlsplit(value).hostname!='doc.weixin.qq.com' for value in re.findall(r'https?://[^\s（），。)]+',text)):
            return None
    except ValueError:return None
    urls=LINK.findall(text)
    if not urls and re.search(r'刚才|刚创建|刚建|那份|上一个|上一份',text):
        candidates=manifest.get('recent_document_candidates',[])
        if len(candidates)==1:
            match=LINK.fullmatch(candidates[0])
            if match:urls=[match[1]]
    ids=set(urls)
    return next(iter(ids)) if len(ids)==1 else None

def read_snapshot(root,row,context,call):
    docid=document_id(root,row,context)
    if not docid:return None
    try:
        r=call(['doc','contents','get','--json',json.dumps({'docid':docid,'content_type':'text'})])
        url=r.get('url','')
        loc=urlsplit(url)
        if (loc.scheme!='https' or loc.netloc.lower()!='doc.weixin.qq.com' or loc.path.rstrip('/')!='/doc/'+docid
                or not isinstance(r.get('content'),str) or not isinstance(r.get('name'),str)
                or len(r['content'].encode('utf8'))>16384):return None
        return {'source':'official_wecom_cli_doc_contents_get','case_id':context['case_id'],'name':r['name'],
                'document_url':'https://doc.weixin.qq.com/doc/'+docid,'version':r.get('version'),'content':r['content'],
                'read_at':datetime.now(timezone.utc).isoformat()}
    except (OSError,ValueError,TypeError,RuntimeError,subprocess.SubprocessError):
        return None
