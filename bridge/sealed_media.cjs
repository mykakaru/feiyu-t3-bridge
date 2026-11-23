'use strict';
const {spawnSync,execFile}=require('node:child_process'),path=require('node:path');
const python=process.env.FEIYU_PYTHON_EXE||'python';
function call(root,args,input){
 const r=spawnSync(python,[path.join(root,'sealed_media.py'),...args],{input,windowsHide:true,timeout:10000,maxBuffer:2*1024*1024+4096});
 if(r.status!==0||r.error)throw Error('sealed_media_operation_failed');
 return r.stdout;
}
function seal(root,prepared){call(root,['seal'],JSON.stringify(prepared));}
function sealAsync(root,prepared){
 return new Promise((resolve,reject)=>{
  const child=execFile(python,[path.join(root,'sealed_media.py'),'seal'],
   {windowsHide:true,timeout:10000,maxBuffer:2*1024*1024+4096},
   error=>error?reject(Error('sealed_media_operation_failed')):resolve());
  child.stdin.on('error',reject);
  child.stdin.end(JSON.stringify(prepared));
 });
}
function unseal(root,msgid){return JSON.parse(call(root,['unseal',msgid]).toString('utf8'));}
module.exports={seal,sealAsync,unseal};
