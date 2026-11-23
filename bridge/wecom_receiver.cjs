'use strict';
// Inbound data is untrusted. Only separate root-reviewed local outbox jobs can send; inbound data never executes commands.
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = process.argv[2];
if (!root) throw new Error('Missing receiver runtime directory');
const os = require('node:os');
const localPkg = path.join(__dirname, 'package.json');
const envPkg = process.env.WECOM_SDK_PACKAGE_JSON;
const homeSdk = path.join(os.homedir(), 'AppData', 'Local', 'wecom-aibot-receiver', 'package.json');
const sdkPkg = [envPkg, localPkg, homeSdk].find(p => p && fs.existsSync(p)) || localPkg;
const sdkRequire = createRequire(sdkPkg);
const {WSClient} = sdkRequire('@wecom/aibot-node-sdk');
const statePath = path.join(root, 'status.json');
const stopPath = path.join(root, 'stop.request');
const inboxPath = path.join(root, 'inbox.jsonl');
const stamp = () => new Date().toISOString();
const heartbeatMs = 1000;
let state = {pid:process.pid, version:'0.2', started_at:stamp(), state:'starting', received:0, auto_reply:false, root_delivery:'not_connected'};
let client, closing = false, secrets = [];
function clean(value) {
  let s = String(value);
  for (const secret of secrets) if (secret) s = s.split(secret).join('[redacted]');
  return s.slice(0, 1000);
}
const writeStatus=require('./status_writer.cjs').makeStatusWriter(statePath);
function status(patch={}) {
  Object.assign(state, patch, {updated_at:stamp()});
  return writeStatus(state);
}
const {receive,stage,recoverInterrupted}=require('./inbox.cjs');
// A crash cannot replay ephemeral URLs. Preserve interrupted records and expose an explicit media gap.
recoverInterrupted(root);
const {sendOnce}=require('./outbox.cjs');
const {OutboxDiagnostics}=require('./outbox_diagnostics.cjs');
const outboxDiagnostics=new OutboxDiagnostics(root);
const outgoing=new Map(), taskInFlight=new Set(), deferred=new Map();
let outboxChanged=true, nextOutboxAudit=0;
const outboxWatchers=[];
for(const name of ['outbox-approved','outbox-receipts']){
 const dir=path.join(root,name);
 fs.mkdirSync(dir,{recursive:true});
 try{const watcher=fs.watch(dir,()=>{outboxChanged=true;});
  watcher.on('error',()=>{outboxChanged=true;watcher.close();});outboxWatchers.push(watcher);
 }catch{ /* a timed audit also recovers missed notifications */ }
}
function drainOutbox(){
 if(closing||state.state!=='authenticated')return;
 const dir=path.join(root,'outbox-approved');if(!fs.existsSync(dir))return;
 const now=Date.now();
 if(!outboxChanged && now<nextOutboxAudit && ![...deferred.values()].some(x=>x.until<=now))return;
 outboxChanged=false;nextOutboxAudit=now+30000;
 let latestPatch={};
 outboxDiagnostics.begin();
 try{
  const files=new Set(fs.readdirSync(dir).filter(f=>f.endsWith('.json')));
  for(const file of deferred.keys())if(!files.has(file))deferred.delete(file);
  for(const file of files){
   outboxDiagnostics.context={job_id:null,file,phase:'read_job'};
   let job;try{job=JSON.parse(fs.readFileSync(path.join(dir,file),'utf8'));}catch(e){outboxDiagnostics.issue('unreadable_job',e);continue;}
   if(typeof job?.job_id!=='string' || !job.job_id){outboxDiagnostics.issue('invalid_job');continue;}
   outboxDiagnostics.context={job_id:job.job_id||null,file,phase:'send_or_read_existing_receipt'};
   // Task/destination proof and owner authorization are created ONLY by root, never from messages.
   if(outgoing.has(file))continue;
   const receipt=path.join(root,'outbox-receipts',require('./outbox.cjs').hash(job.job_id)+'.json');
   if(fs.existsSync(receipt)){
    deferred.delete(file);
    try{latestPatch=outboxDiagnostics.result(job,JSON.parse(fs.readFileSync(receipt,'utf8')));}
    catch(e){outboxDiagnostics.issue('unreadable_receipt',e);}
    continue;
   }
   const prior=deferred.get(file);
   if(prior && prior.until>Date.now()){outboxDiagnostics.issue(prior.kind);continue;}
   if(outgoing.size>=4 || taskInFlight.has(job.task_id)){outboxChanged=true;continue;}
   taskInFlight.add(job.task_id);
   const pending=sendOnce(root,job,job.verified_session,client).then(result=>{
    if(result.state==='not_sent'){
     const blocked=path.join(root,'outbox-blocked');fs.mkdirSync(blocked,{recursive:true});
     fs.writeFileSync(path.join(blocked,file),JSON.stringify(result,null,2));
     deferred.set(file,{until:Date.now()+5000,kind:'not_sent'});
    }else deferred.delete(file);
   }).catch(()=>deferred.set(file,{until:Date.now()+5000,kind:'send_or_read_failed'}))
     .finally(()=>{outgoing.delete(file);taskInFlight.delete(job.task_id);});
   outgoing.set(file,pending);
  }
 }catch(e){outboxDiagnostics.issue('drain_exception',e);}finally{
  try{status({...latestPatch,...outboxDiagnostics.finish()});}catch(e){status({outbox_error:'diagnostic_persistence_failed',outbox_diagnostic_failure_at:stamp(),outbox_diagnostic_exception_type:e.name});}
 }
}
const {MessageWorkers}=require('./message_workers.cjs');
const mediaWorkers=new MessageWorkers(3);
const {downloadInWorker}=require('./media_download_worker.cjs');
function record(kind,frame){
 if(kind!=='message'||closing)return;
 let queued;try{queued=stage(root,frame.body||{},true);}catch{status({last_processing_error:'inbox_durable_stage_failed'});return;}
 if(queued.duplicate||queued.ignored)return;
 async function processMessage(){
  try{
   if(queued.media.length){
    try{await require('./sealed_media.cjs').sealAsync(root,queued);queued.record.media_source_sealed=true;}
    catch{queued.record.media_source_sealed=false;queued.record.media_persistence_error='seal_failed';}
   }
   // boundedDownload validates the exact source, pins public DNS, refuses
   // redirects, and runs the official SDK download/decrypt in a worker thread.
   // Neither slow network nor synchronous AES decryption blocks new callbacks.
   await receive(root,frame.body||{},downloadInWorker,queued);
   status({last_received_at:stamp(),last_type:frame.body?.msgtype,received:state.received+1});
  }catch{status({last_processing_error:'inbox_processing_failed'});}
 }
 if(queued.media.length)mediaWorkers.add(processMessage);
 else void processMessage();
}
function stop(reason,code=0) {
  if(closing) return; closing=true;
  for(const watcher of outboxWatchers)watcher.close();
  try {client?.disconnect();} catch {}
  status({state:'draining',reason:clean(reason)});
  const timeout=setTimeout(()=>{status({state:'stopped',reason:clean(reason),drain_timeout:true});process.exit(code);},75000);
  Promise.allSettled([mediaWorkers.idle(),...outgoing.values()]).finally(()=>{clearTimeout(timeout);status({state:'stopped',reason:clean(reason)});process.exit(code);});
}
process.on('SIGTERM',()=>stop('SIGTERM'));
process.on('SIGINT',()=>stop('SIGINT'));
process.on('uncaughtException',e=>stop(e.message,1));
process.on('unhandledRejection',e=>stop(e?.message||'unhandled rejection',1));
let input='';
process.stdin.setEncoding('utf8');
process.stdin.on('data',s=>input+=s);
process.stdin.on('end',()=>{
  const config=JSON.parse(input); input='';
  secrets=[config.secret,config.botId];
  client=new WSClient({botId:config.botId,secret:config.secret,heartbeatInterval:heartbeatMs,maxReconnectAttempts:-1,maxAuthFailureAttempts:1,
    logger:{debug(message){
      if(message === 'Heartbeat timer started, interval: '+heartbeatMs+'ms')
        status({ws_heartbeat_ms:heartbeatMs,ws_heartbeat_started_at:stamp()});
    },info(){},warn(){},error(){} }});
  if(client?.wsManager)client.wsManager.replyAckTimeout=30000;
  client.on('connected',()=>status({state:'connected'}));
  client.on('authenticated',()=>status({state:'authenticated',authenticated_at:stamp(),error:null}));
  client.on('reconnecting',attempt=>status({state:'reconnecting',attempt}));
  client.on('disconnected',reason=>{
    if(closing) return;
    status({state:'disconnected',reason:clean(reason)});
    if(String(reason).includes('New connection')) stop('Another client replaced this connection');
  });
  client.on('error',e=>{
    status({state:'error',error:clean(e.message),error_type:e.name});
    if(['WSAuthFailureError','WSReconnectExhaustedError'].includes(e.name)) stop(e.message,1);
  });
  client.on('message',f=>record('message',f));
  client.on('event',f=>record('event',f));
  client.connect();
  setTimeout(()=>{if(!state.authenticated_at) status({waiting_for_network_or_auth:true});},30000).unref();
  setInterval(()=>{
    if(fs.existsSync(stopPath)) {fs.unlinkSync(stopPath);stop('local stop request');}
    else {status();drainOutbox();}
  },heartbeatMs);
});
status();
