'use strict';
// Status telemetry must not terminate the message receiver on transient Windows file locks.
const fs=require('node:fs'),path=require('node:path');
function makeStatusWriter(file,io=fs,wait=ms=>Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,ms),now=()=>new Date().toISOString()){
 let failing=false;
 const note=(kind,code)=>{try{io.appendFileSync(path.join(path.dirname(file),'status-write-events.jsonl'),JSON.stringify({kind,code:code||null,at:now()})+'\n');}catch{}};
 return value=>{
  const tmp=file+'.'+process.pid+'.tmp';
  for(let attempt=0;attempt<5;attempt++){
   try{io.writeFileSync(tmp,JSON.stringify(value,null,2));io.renameSync(tmp,file);if(failing)note('recovered');failing=false;return true;}
   catch(e){if(!['EPERM','EACCES','EBUSY'].includes(e.code)||attempt===4){if(!failing)note('write_failed',e.code||'unknown');failing=true;return false;}wait(20*(attempt+1));}
  }
  return false;
 };
}
module.exports={makeStatusWriter};
