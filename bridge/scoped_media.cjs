'use strict';
// Only media URLs supplied by the authenticated WeCom SDK callback are inputs.
// No redirects, wildcard COS trust, websocket connection, messaging or queue ACK.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const dns=require('node:dns/promises'),https=require('node:https'),net=require('node:net');
const {createRequire}=require('node:module');
const MAX_BYTES=100*1024*1024, TIMEOUT_MS=60000;
const EXACT_MEDIA_HOST=process.env.FEIYU_EXACT_MEDIA_HOST||'ww-aibot-img-example.cos.ap-guangzhou.myqcloud.com';
const TARGET_IDS=new Set((process.env.FEIYU_MEDIA_TARGET_IDS||'').split(',').map(s=>s.trim()).filter(Boolean));
const digest=x=>crypto.createHash('sha256').update(x).digest('hex');
const stamp=()=>new Date().toISOString();
function checkedUrl(raw){
 let u;try{u=new URL(raw);}catch{throw Error('invalid_media_url');}
 if(u.protocol!=='https:'||u.username||u.password||u.hash||u.port&&u.port!=='443'||u.hostname!==EXACT_MEDIA_HOST)throw Error('unsupported_media_host');
 return u;
}
function validKey(key){
 return typeof key==='string'&&/^[A-Za-z0-9+/]{43}=?$/.test(key)&&Buffer.from(key,'base64').length===32;
}
function publicAddress(address){
 if(net.isIP(address)===4){
  const a=address.split('.').map(Number);
  return !(a[0]===0||a[0]===10||a[0]===127||a[0]===169&&a[1]===254||a[0]===172&&a[1]>=16&&a[1]<=31||a[0]===192&&(a[1]===168||a[1]===0&&a[2]===0||a[1]===0&&a[2]===2)||a[0]===100&&a[1]>=64&&a[1]<=127||a[0]>=224||a[0]===198&&(a[1]===18||a[1]===19||a[1]===51&&a[2]===100)||a[0]===203&&a[1]===0&&a[2]===113);
 }
 if(net.isIP(address)===6){
  const a=address.toLowerCase();
  if(a.startsWith('::ffff:'))return false;
  const groups=a.split(':'),first=parseInt(groups[0],16),second=parseInt(groups[1],16);
  return first>=0x2000&&first<0x4000&&!(first===0x2001&&second===0x0db8);
 }
 return false;
}
function sdkRequire(){
 const os=require('node:os');
 const candidates=[process.env.WECOM_SDK_PACKAGE_JSON,path.join(__dirname,'package.json'),path.join(os.homedir(),'AppData','Local','wecom-aibot-receiver','package.json')].filter(Boolean);
 const installed=candidates.find(p=>fs.existsSync(p));
 if(!installed)throw Error('official_sdk_not_found');
 const req=createRequire(installed),packageFile=req.resolve('@wecom/aibot-node-sdk/package.json');
 const pkg=JSON.parse(fs.readFileSync(packageFile,'utf8'));
 if(pkg.name!=='@wecom/aibot-node-sdk'||pkg.version!=='1.0.6')throw Error('sdk_adapter_version_changed');
 return {sdk:req('@wecom/aibot-node-sdk'),version:pkg.version,packageFile};
}
function safeError(error){
 const info={name:'media_download_failed'};
 if(Number.isInteger(error?.response?.status))info.http_status=error.response.status;
 if(typeof error?.code==='string'&&/^[A-Z_0-9]{1,64}$/.test(error.code))info.code=error.code;
 if(error?.response?.data){
  const data=Buffer.isBuffer(error.response.data)?error.response.data.toString('utf8'):String(error.response.data);
  const m=data.slice(0,4096).match(/<Code>([A-Za-z_0-9]{1,80})<\/Code>/);
  if(m)info.service_code=m[1];
 }
 const local=new Set(['invalid_media_url','unsupported_media_host','invalid_aes_key','dns_nonpublic_or_empty','sdk_adapter_version_changed','official_sdk_not_found','download_adapter_unavailable','source_mismatch','not_direct_file','size_limit']);
 if(local.has(error?.message))info.local_code=error.message;
 return info;
}
function atomicJson(file,body){
 fs.mkdirSync(path.dirname(file),{recursive:true});
 const tmp=file+'.tmp',fd=fs.openSync(tmp,'w');
 try{fs.writeFileSync(fd,JSON.stringify(body,null,2));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
 fs.renameSync(tmp,file);
}
function safeFilename(name){
 if(typeof name!=='string')return 'download.bin';
 return path.win32.basename(name).replace(/[\x00-\x1f\x7f]/g,'_').slice(0,255)||'download.bin';
}
async function boundedDownload(url,key,trace={},dependencies={}){
 const u=checkedUrl(url);
 if(!validKey(key))throw Error('invalid_aes_key');
 const addresses=await (dependencies.lookup||dns.lookup)(u.hostname,{all:true});
 if(!addresses.length||addresses.some(a=>!publicAddress(a.address)))throw Error('dns_nonpublic_or_empty');
 trace.dns_addresses=addresses.map(a=>({address:a.address,family:a.family}));
 const {sdk,version,packageFile}=(dependencies.sdkRequire||sdkRequire)();
 trace.sdk={name:'@wecom/aibot-node-sdk',version,package_json_sha256:digest(fs.readFileSync(packageFile))};
 const logger={debug(){},info(){},warn(){},error(){}};
 const client=new sdk.WSClient({botId:'unused-readonly-media-download',secret:'unused',logger});
 const http=client.apiClient?.httpClient;
 if(!http?.defaults||!http.interceptors?.request)throw Error('download_adapter_unavailable');
 const agent=new https.Agent({lookup(host,options,callback){
  if(host!==u.hostname)return callback(Error('dns_host_changed'));
  if(options?.all)return callback(null,addresses);
  const selected=addresses.find(a=>!options?.family||a.family===options.family)||addresses[0];
  callback(null,selected.address,selected.family);
 }});
 Object.assign(http.defaults,{maxContentLength:MAX_BYTES,maxBodyLength:MAX_BYTES,maxRedirects:0,timeout:TIMEOUT_MS,httpsAgent:agent});
 http.interceptors.request.use(config=>{
  if(config.url!==url||checkedUrl(config.url).hostname!==u.hostname)throw Error('source_mismatch');
  config.maxRedirects=0;config.maxContentLength=MAX_BYTES;config.maxBodyLength=MAX_BYTES;config.timeout=TIMEOUT_MS;
  config.signal=AbortSignal.timeout(TIMEOUT_MS);
  return config;
 });
 // Official WSClient.downloadFile -> api.downloadFileRaw -> Axios GET -> official AES decrypt.
 // No connect(), no auth credentials, no send/reply calls.
 try{
  const result=await client.downloadFile(url,key);
  if(!Buffer.isBuffer(result.buffer)||result.buffer.length>MAX_BYTES)throw Error('size_limit');
  return {buffer:result.buffer,filename:safeFilename(result.filename)};
 }finally{agent.destroy();}
}
function bindSource(row,prepared,msgid){
 if(row.msgid!==msgid||prepared.record?.msgid!==msgid||!(process.env.FEIYU_OWNER_WS_USERID && row.chatid===process.env.FEIYU_OWNER_WS_USERID && row.sender===process.env.FEIYU_OWNER_WS_USERID)||row.chattype!=='single')throw Error('source_mismatch');
 for(const k of ['sender','chatid','chattype','msgtype'])if(prepared.record[k]!==row[k])throw Error('source_mismatch');
 if(row.msgtype!=='file'||prepared.media?.length!==1||prepared.media[0].type!=='file'||prepared.media[0].quoted!==false)throw Error('not_direct_file');
 const m=prepared.media[0];checkedUrl(m.source?.url);
 if(!validKey(m.source?.aeskey))throw Error('invalid_aes_key');
 return m;
}
module.exports={MAX_BYTES,TIMEOUT_MS,EXACT_MEDIA_HOST,TARGET_IDS,digest,stamp,checkedUrl,validKey,publicAddress,sdkRequire,safeError,atomicJson,safeFilename,boundedDownload,bindSource};

