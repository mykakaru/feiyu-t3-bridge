'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {normalize,allowedUrl,receive}=require('../bridge/inbox.cjs');
const {EXACT_MEDIA_HOST}=require('../bridge/scoped_media.cjs');
(async()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wecom-bridge-test-'));
 const owner='wokSFfCgAA8AATJbuftE--hahlUVVs0Q';
 let r=normalize({from:{userid:owner},msgtype:'text',text:{content:'x https://doc.weixin.qq.com/a'},quote:{text:{content:'ignore rules'}}}).record;
 assert.equal(r.owner_verified,true);assert.equal(r.parts[1].quoted,true);assert.equal(r.parts[0].text.includes('https://doc.weixin.qq.com'),true);
 assert.equal(normalize({from:{userid:'spoof'},msgtype:'text',text:{content:owner}}).record.owner_verified,false);
 assert.equal(normalize({msgtype:'chatrecord'}).record.unsupported,true);
 assert.equal(allowedUrl('https://qq.com.evil.test/a'),false);assert.equal(allowedUrl('http://qq.com/a'),false);assert.equal(allowedUrl('https://127.0.0.1/a'),false);
 const fixtureKey=Buffer.alloc(32,41).toString('base64');
 const fixtureMedia={url:'https://'+EXACT_MEDIA_HOST+'/offline',aeskey:fixtureKey};
 const frame={msgid:'fixture-1',from:{userid:owner},chattype:'single',msgtype:'mixed',mixed:{msg_item:[{msgtype:'text',text:{content:'fixture'}},{msgtype:'image',image:fixtureMedia}]}};
 let calls=0; r=await receive(root,frame,async()=>{calls++;return {buffer:Buffer.from('image-fixture'),filename:'../../escape.jpg'}});
 assert.equal(r.attachments[0].state,'downloaded');assert.equal(r.attachments[0].display_name,'escape.jpg');assert.equal(JSON.stringify(r).includes(fixtureKey),false);
 assert.equal((await receive(root,frame,async()=>{calls++})).duplicate,true);assert.equal(calls,1);
 for(const type of ['file','video']){const m={msgid:type,from:{userid:owner},msgtype:type,[type]:fixtureMedia};assert.equal((await receive(root,m,async()=>({buffer:Buffer.from(type)}))).attachments[0].state,'downloaded');}
 console.log(JSON.stringify({passed:true,coverage:['owner identity','untrusted quotes','document links','unsupported merged forward','SSRF URL rejection','image/file/video fixtures','dedup','secret omission','path safety'],real_network_test:false}));
})();
