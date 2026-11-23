'use strict';
// Only the root's reviewed local jobs may call this module. Never ingest a sender-supplied job.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const put=(file,obj)=>{const t=file+'.tmp';const fd=fs.openSync(t,'w');try{fs.writeFileSync(fd,JSON.stringify(obj,null,2));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}fs.renameSync(t,file);};
function contentHash(job){
 if(!job.media)return typeof job.text==='string'?hash(job.text):null;
 const m=job.media;return hash(JSON.stringify(['media',m.type,m.relative_path,m.filename,m.sha256,m.bytes,m.title||null,m.description||null]));
}
function reviewedMedia(root,m){
 if(!['image','file','video','voice'].includes(m.type)||typeof m.relative_path!=='string'||typeof m.filename!=='string'||!m.filename||m.filename.length>160||m.filename!==path.basename(m.filename)||/[\\/:\x00-\x1f]/.test(m.filename))throw Error('invalid_media_manifest');
 if(!Number.isInteger(m.bytes)||m.bytes<=0||m.bytes>100*1024*1024||!/^([a-f0-9]{64})$/.test(m.sha256))throw Error('invalid_media_manifest');
 if((m.title!=null&&typeof m.title!=='string')||(m.description!=null&&typeof m.description!=='string')||Buffer.byteLength(m.title||'')>128||Buffer.byteLength(m.description||'')>512)throw Error('invalid_media_metadata');
 const base=fs.realpathSync(path.join(root,'outgoing-media')),file=fs.realpathSync(path.resolve(root,m.relative_path)),relative=path.relative(base,file);
 if(relative.startsWith('..')||path.isAbsolute(relative)||!fs.statSync(file).isFile())throw Error('invalid_media_path');
 const bytes=fs.readFileSync(file);
 if(bytes.length!==m.bytes||hash(bytes)!==m.sha256)throw Error('media_changed_after_review');
 return bytes;
}
async function sendOnce(root,job,verifiedSession,client,now=Date.now()){
 const dir=path.join(root,'outbox-receipts');fs.mkdirSync(dir,{recursive:true});
 if(!job?.job_id)throw Error('job_id_required');
 const file=path.join(dir,hash(job.job_id)+'.json');if(fs.existsSync(file))return JSON.parse(fs.readFileSync(file,'utf8'));
 const hold=reason=>({state:'not_sent',reason,job_id:job.job_id});
 if(!verifiedSession?.verified_by_root||!verifiedSession.evidence_ref||job.chatid!==verifiedSession.chatid||job.task_id!==verifiedSession.task_id)return hold('unverified_or_wrong_session_task');
 if(!job.approval?.root_reviewed||!job.approval.owner_authorization_ref||job.approval.audience!==job.chatid)return hold('missing_authorized_audience');
 if(!job.media&&(typeof job.text!=='string'||!job.text||Buffer.byteLength(job.text,'utf8')>20480))return hold('invalid_content_size');
 if(job.media&&job.text)return hold('separate_media_and_text_jobs_required');
 if(job.approval.content_sha256!==contentHash(job))return hold('content_changed_after_review');
 let mediaBuffer;
 if(job.media){try{mediaBuffer=reviewedMedia(root,job.media);}catch{return hold('invalid_or_changed_media');}}
 const last=Date.parse(verifiedSession.last_user_interaction_at);
 if(!Number.isFinite(last)||now-last>=24*3600000||now<last)return hold('session_window_expired_remention_required');
 if(job.cancelled||job.superseded)return hold('cancelled_or_superseded');
 const previous=fs.readdirSync(dir).filter(f=>f.endsWith('.json')).map(f=>JSON.parse(fs.readFileSync(path.join(dir,f),'utf8'))).filter(r=>r.chatid===job.chatid&&r.attempted_at);
 // Local guardrails, NOT a statement of the server's exact limits. Include attempts, not just ACKs.
 if(previous.filter(r=>now-Date.parse(r.attempted_at)<60000).length>=20||previous.filter(r=>now-Date.parse(r.attempted_at)<3600000).length>=500)return hold('local_rate_guard');
 let r={job_id:job.job_id,chatid:job.chatid,task_id:job.task_id,content_sha256:contentHash(job),state:'attempted_unknown',attempted_at:new Date(now).toISOString(),auto_retry:false};
 try{const fd=fs.openSync(file,'wx');try{fs.writeFileSync(fd,JSON.stringify(r,null,2));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}}catch(e){if(e.code==='EEXIST')return JSON.parse(fs.readFileSync(file,'utf8'));throw e;}
 try{
  let ack;
  if(job.media){
   const m=job.media;
   const uploaded=await client.uploadMedia(mediaBuffer,{type:m.type,filename:m.filename});
   if(uploaded?.type!==m.type||typeof uploaded?.media_id!=='string'||!uploaded.media_id)throw Error('media_upload_unverified');
   const options={};if(m.title)options.title=m.title;if(m.description)options.description=m.description;
   ack=await client.sendMediaMessage(job.chatid,m.type,uploaded.media_id,options);
  }else ack=await client.sendMessage(job.chatid,{msgtype:'markdown',markdown:{content:job.text}});
  if(ack?.errcode===0){r.state='server_accepted';r.receipt={req_id:ack.headers?.req_id||null,errcode:0};}
  else {r.state='rejected_or_unknown';r.error_code=typeof ack?.errcode==='number'?ack.errcode:null;}
 }catch{r.state='failed_or_unknown';}
 r.finished_at=new Date().toISOString();put(file,r);return r;
}
module.exports={sendOnce,hash,contentHash,reviewedMedia};
