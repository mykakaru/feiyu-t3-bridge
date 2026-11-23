'use strict';
// Network I/O and the official SDK's synchronous AES decrypt stay off the
// receiver event loop. No worker ever connects, sends, or acknowledges a chat.
const {Worker,isMainThread,parentPort}=require('node:worker_threads');
const scoped=require('./scoped_media.cjs');

if(!isMainThread){
 parentPort.once('message',async({url,key})=>{
  try{
   const result=await scoped.boundedDownload(url,key);
   const bytes=Uint8Array.from(result.buffer);
   parentPort.postMessage({ok:true,bytes,filename:result.filename},[bytes.buffer]);
  }catch(error){parentPort.postMessage({ok:false,failure:scoped.safeError(error)});}
 });
}

function downloadInWorker(url,key,options={}){
 return new Promise((resolve,reject)=>{
  const worker=new Worker(options.workerFile||__filename);
  let settled=false;
  const finish=(error,result)=>{
   if(settled)return;settled=true;clearTimeout(timer);
   void worker.terminate().catch(()=>{});
   error?reject(error):resolve(result);
  };
  const timer=setTimeout(()=>{const error=Error('media_download_timeout');error.code='ETIMEDOUT';finish(error);},options.timeoutMs||scoped.TIMEOUT_MS+15000);
  worker.once('message',message=>{
   if(message?.ok===true&&message.bytes instanceof Uint8Array&&message.bytes.byteLength<=scoped.MAX_BYTES){
    finish(null,{buffer:Buffer.from(message.bytes.buffer,message.bytes.byteOffset,message.bytes.byteLength),filename:scoped.safeFilename(message.filename)});
   }else{
    const error=Error('media_download_failed');
    if(message?.failure&&typeof message.failure==='object')error.mediaFailure=message.failure;
    finish(error);
   }
  });
  worker.once('error',error=>finish(error));
  worker.once('exit',code=>{if(!settled){const error=Error('media_download_worker_exited');error.code='EWORKEREXIT';finish(error);}});
  worker.postMessage({url,key});
 });
}

module.exports={downloadInWorker};
