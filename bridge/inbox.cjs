'use strict';
const fs=require('node:fs'), path=require('node:path'), crypto=require('node:crypto');
const OWNER='wokSFfCgAA8AATJbuftE--hahlUVVs0Q';
const id=x=>crypto.createHash('sha256').update(Buffer.isBuffer(x)?x:String(x)).digest('hex');
function normalize(b,botChannel='feiyu'){
 const parts=[],media=[];
 function parse(x,quoted=false){
  if(!x||typeof x!=='object')return;
  if(typeof x.text?.content==='string')parts.push({type:'text',text:x.text.content,quoted});
  if(typeof x.voice?.content==='string')parts.push({type:'voice_transcript',text:x.voice.content,quoted});
  for(const type of ['image','file','video'])if(x[type])media.push({type,quoted,source:x[type]});
  if(Array.isArray(x.mixed?.msg_item))for(const i of x.mixed.msg_item)parse(i,quoted);
 }
 parse(b);parse(b.quote,true);
 return {record:{schema:1,bot_channel:botChannel||'feiyu',msgid:b.msgid||null,received_at:new Date().toISOString(),sender:b.from?.userid||null,
  chatid:b.chatid||(b.chattype==='single'?b.from?.userid:null),chattype:b.chattype||null,msgtype:b.msgtype||'unknown',
  owner_verified:b.from?.userid===OWNER,content_is_untrusted:true,parts,attachments:[],
  unsupported:!['text','voice','mixed','image','file','video'].includes(b.msgtype),
  delivery:'processing',root_delivery:'not_connected'},media};
}
// Exact COS host observed in authenticated official SDK callbacks. Never trust an arbitrary COS bucket.
function allowedUrl(url){try{require('./scoped_media.cjs').checkedUrl(url);return true;}catch{return false;}}
function atomicJson(target,row){
 fs.mkdirSync(path.dirname(target),{recursive:true});
 const tmp=target+'.'+crypto.randomUUID()+'.tmp',fd=fs.openSync(tmp,'wx');
 try{fs.writeFileSync(fd,JSON.stringify(row,null,2));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
 try{fs.renameSync(tmp,target);}finally{try{fs.unlinkSync(tmp);}catch{}}
}
function save(root,row){atomicJson(path.join(root,'queue',id(row.msgid)+'.json'),row);}
const MEDIA_FIELDS=['attachments','attachment_count','media_status','processed_at','media_source_sealed','media_persistence_error','processing_interrupted','media_gap'];
function mediaPath(root,msgid){return path.join(root,'media-state',id(msgid)+'.json');}
function saveMedia(root,row){
 const update={schema:1,msgid:row.msgid,sender:row.sender,chatid:row.chatid,received_at:row.received_at,updated_at:new Date().toISOString()};
 for(const field of MEDIA_FIELDS)if(Object.hasOwn(row,field))update[field]=row[field];
 // A sidecar owns media fields only. It cannot rewrite delivery, ACK timestamps,
 // assignment state, raw text, or other concurrently changing business fields.
 atomicJson(mediaPath(root,row.msgid),update);
 return update;
}
function mergeMedia(root,row){
 let media;try{media=JSON.parse(fs.readFileSync(mediaPath(root,row.msgid),'utf8'));}catch{return row;}
 if(media.schema!==1||['msgid','sender','chatid','received_at'].some(key=>media[key]!==row[key]))return row;
 const merged={...row};
 for(const field of MEDIA_FIELDS)if(Object.hasOwn(media,field))merged[field]=media[field];
 if(merged.delivery==='processing'&&['completed','failed','interrupted','none'].includes(merged.media_status))merged.delivery='pending';
 return merged;
}
function readRecord(root,msgid){return mergeMedia(root,JSON.parse(fs.readFileSync(path.join(root,'queue',id(msgid)+'.json'),'utf8')));}
function recoverInterrupted(root){
 const qdir=path.join(root,'queue');if(!fs.existsSync(qdir))return;
 for(const file of fs.readdirSync(qdir).filter(name=>name.endsWith('.json'))){
  try{
   const row=mergeMedia(root,JSON.parse(fs.readFileSync(path.join(qdir,file),'utf8')));
   if(row.media_status!=='processing'&&row.delivery!=='processing')continue;
   row.processing_interrupted=true;row.media_status='interrupted';
   row.media_gap='Receiver restarted before media completion; affected media requires explicit recovery or sender resend';
   row.attachments=(row.attachments||[]).map(a=>a.state==='processing'?{...a,state:'unavailable',reason:'receiver_restarted'}:a);
   saveMedia(root,row);
  }catch{}
 }
}
function stage(root,b,deferSeal=false,botChannel='feiyu'){
 if(typeof b.msgid!=='string'||!b.msgid)return {ignored:'missing_msgid'};
 const target=path.join(root,'queue',id(b.msgid)+'.json');if(fs.existsSync(target))return {duplicate:true};
 const prepared=normalize(b,botChannel);prepared.record.attachment_count=prepared.media.length;
 prepared.record.delivery='pending';
 prepared.record.media_status=prepared.media.length?'processing':'none';
 prepared.record.attachments=prepared.media.map(m=>{const a={type:m.type,quoted:m.quoted,state:'processing'};try{a.source_host=new URL(m.source.url).hostname;}catch{}return a;});
 if(prepared.media.length && !deferSeal){try{require('./sealed_media.cjs').seal(root,prepared);prepared.record.media_source_sealed=true;}catch{prepared.record.media_source_sealed=false;prepared.record.media_persistence_error='seal_failed';}}
 save(root,prepared.record);return prepared;
}
async function receive(root,b,download,prepared){
 prepared=prepared||stage(root,b);
 if(prepared.ignored||prepared.duplicate)return prepared;
 const {record,media}=prepared;
 record.attachments=[];
 for(let i=0;i<media.length;i++){
  const m=media[i],a={type:m.type,quoted:m.quoted,state:'unavailable'};
  record.attachments.push(a);
  try{a.source_host=new URL(m.source.url).hostname;}catch{} // host only; never persist signed URL or key
  if(!allowedUrl(m.source.url)){a.reason='unsupported_media_host';continue;}
  if(!require('./scoped_media.cjs').validKey(m.source.aeskey)){a.reason='invalid_aes_key';continue;}
  try{const r=await download(m.source.url,m.source.aeskey);if(!Buffer.isBuffer(r.buffer)||r.buffer.length>100*1024*1024)throw Error('size_limit');
   const dir=path.join(root,'media');fs.mkdirSync(dir,{recursive:true});const name=id(b.msgid)+'-'+i+'.bin';
   const fd=fs.openSync(path.join(dir,name),'wx');try{fs.writeFileSync(fd,r.buffer);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}Object.assign(a,{state:'downloaded',relative_path:'media/'+name,bytes:r.buffer.length,sha256:id(r.buffer),display_name:require('./scoped_media.cjs').safeFilename(r.filename||m.type)});
  }catch(e){a.reason='media_download_failed';a.failure=e.mediaFailure||require('./scoped_media.cjs').safeError(e);}
 }
 record.media_status=media.length?(record.attachments.every(a=>a.state==='downloaded')?'completed':'failed'):'none';
 record.processed_at=new Date().toISOString();
 saveMedia(root,record);return mergeMedia(root,JSON.parse(fs.readFileSync(path.join(root,'queue',id(record.msgid)+'.json'),'utf8')));
}
module.exports={normalize,allowedUrl,receive,stage,save,id,saveMedia,mergeMedia,readRecord,recoverInterrupted,MEDIA_FIELDS};
