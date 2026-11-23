'use strict';
// Diagnostics only: no sending, retrying, ACK or queue mutation.
const fs=require('node:fs'),path=require('node:path');
class OutboxDiagnostics {
 constructor(root,now=()=>new Date().toISOString()){
  this.root=root;this.now=now;this.file=path.join(root,'outbox-diagnostics.json');
  try{this.saved=JSON.parse(fs.readFileSync(this.file,'utf8'));}catch{this.saved={active:[],last_error:null};}
  // Old generic error has no trustworthy occurrence time/job. Preserve that uncertainty.
  try{const s=JSON.parse(fs.readFileSync(path.join(root,'status.json'),'utf8'));
   if(s.outbox_error&&!this.saved.legacy_imported){this.saved.legacy_imported=true;this.history({kind:'legacy_error_observed',error:s.outbox_error,occurred_at:null,observed_at:this.now(),job_id:null});}
  }catch{}
  this.issues=[];this.context={job_id:null,file:null,phase:'scan'};
 }
 history(record){fs.appendFileSync(path.join(this.root,'outbox-diagnostic-history.jsonl'),JSON.stringify(record)+'\n');}
 begin(){this.issues=[];this.context={job_id:null,file:null,phase:'scan'};}
 issue(kind,exception=null){this.issues.push({...this.context,kind,exception_type:exception?.name||null});}
 result(job,result){
  if(result.state!=='server_accepted')this.issue(result.state==='not_sent'?'not_sent':result.state);
  const actual=result.finished_at||result.attempted_at||null;
  if(actual && Number.isFinite(Date.parse(actual)) && (!this.saved.latest || Date.parse(actual)>=Date.parse(this.saved.latest.last_outbox_at))){
   this.saved.latest={last_outbox_job_id:job.job_id,last_outbox_state:result.state,last_outbox_at:actual};
  }
  return {...(this.saved.latest||{last_outbox_at:null}),last_outbox_observed_job_id:job.job_id,
   last_outbox_observed_state:result.state,last_outbox_observed_at:this.now()};
 }
 finish(){
  const previous=this.saved.active||[],at=this.now();
  const key=x=>JSON.stringify(x);
  if(key(previous)!==key(this.issues)){
   const record={kind:this.issues.length?'issues_changed':'issues_cleared',observed_at:at,active:this.issues,previous};
   this.history(record);if(this.issues.length)this.saved.last_error=record;
  }
  this.saved.active=this.issues;this.saved.last_checked_at=at;
  const tmp=this.file+'.tmp';fs.writeFileSync(tmp,JSON.stringify(this.saved,null,2));fs.renameSync(tmp,this.file);
  return {outbox_error:this.issues.length?'review_required':null,outbox_active_issues:this.issues,
   outbox_last_error:this.saved.last_error,outbox_checked_at:at};
 }
}
module.exports={OutboxDiagnostics};
