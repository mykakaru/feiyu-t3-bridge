const assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {sendOnce,hash}=require('../bridge/outbox.cjs');
(async()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wecom-outbox-test-')),now=Date.now();
 const session={verified_by_root:true,evidence_ref:'fixture',chatid:'groupA',task_id:'taskA',last_user_interaction_at:new Date(now-3600000).toISOString()};
 const job={job_id:'one',chatid:'groupA',task_id:'taskA',text:'fixture result',approval:{root_reviewed:true,owner_authorization_ref:'fixture-owner',audience:'groupA',content_sha256:hash('fixture result')}};
 let calls=0;const client={sendMessage:async(c,b)=>{calls++;assert.equal(c,'groupA');assert.equal(b.chatid,undefined);return {errcode:0,headers:{req_id:'fixture'}}}};
 assert.equal((await sendOnce(root,job,session,client,now)).state,'server_accepted');await sendOnce(root,job,session,client,now);assert.equal(calls,1);
 const j={...job,job_id:'two'};assert.equal((await sendOnce(root,j,{...session,chatid:'other'},client,now)).state,'not_sent');
 assert.equal((await sendOnce(root,j,{...session,last_user_interaction_at:new Date(now-25*3600000).toISOString()},client,now)).reason,'session_window_expired_remention_required');
 assert.equal((await sendOnce(root,{...j,text:'changed'},session,client,now)).reason,'content_changed_after_review');
 const fail={sendMessage:async()=>{calls++;throw Error('timeout')}};assert.equal((await sendOnce(root,j,session,fail,now)).state,'failed_or_unknown');await sendOnce(root,j,session,client,now);assert.equal(calls,2);
 assert.equal((await sendOnce(root,{...job,job_id:'three'},session,{sendMessage:async()=>({errcode:45009})},now)).state,'rejected_or_unknown');
 console.log('PASS ACK, duplicate suppression, audience/task binding, 24h expiry, content approval hash, no retry on uncertain failure, server rejection. Fixture only; no network send.');
})();
