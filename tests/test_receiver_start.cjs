'use strict';
// The child loads a substituted callback-only SDK. It has no sockets, credentials or approved outbox jobs.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'fish-rx-fixture-'));
const hook = path.join(root, 'fixture.cjs');
try {
  fs.writeFileSync(hook, [
    "const mod = require('node:module'), {EventEmitter} = require('node:events');",
    "const fs = require('node:fs'), path = require('node:path');",
    "const assert = require('node:assert/strict'), root = process.argv[2];",
    "require('node:net').Socket.prototype.connect = () => { throw Error('network_disabled'); };",
    "class Client extends EventEmitter {",
    " constructor(config) { super(); assert.equal(config.heartbeatInterval,1000); assert.equal(config.maxReconnectAttempts,-1); assert.equal(config.maxAuthFailureAttempts,1); }",
    " connect() { this.emit('authenticated'); const f={body:{msgid:'rx-fixture',from:{userid:'fixture'},chattype:'single',msgtype:'text',text:{content:'fixture'}}}; this.emit('message',f); this.emit('message',f); setTimeout(()=>fs.writeFileSync(path.join(root,'stop.request'),'fixture'),50); }",
    " disconnect() {}",
    " sendMessage() { throw Error('no_outbound_permitted'); }",
    "}",
    "const orig = mod.createRequire; mod.createRequire = (p) => { const req=orig(p); if(!String(p).includes('wecom-aibot-receiver'))return req; return Object.assign((name)=>name==='@wecom/aibot-node-sdk'?{WSClient:Client}:req(name),req); };"
  ].join('\n'));
  const child = spawnSync(process.execPath, ['--require',hook,path.join(__dirname,'wecom_receiver.cjs'),root], {
    input:JSON.stringify({botId:'fixture',secret:'fixture'}), encoding:'utf8',timeout:12000,windowsHide:true
  });
  assert.equal(child.status,0,child.error?.message || child.stderr);
  const status=JSON.parse(fs.readFileSync(path.join(root,'status.json')));
  assert.ok(status.authenticated_at);
  assert.equal(status.state,'stopped');assert.equal(status.received,1);
  assert.equal(fs.readdirSync(path.join(root,'queue')).length,1);
  for (const dir of ['outbox-approved','outbox-receipts']) assert.deepEqual(fs.readdirSync(path.join(root,dir)),[]);
  console.log('PASS simulated SDK cold start, durable deduplication, original reconnect settings, graceful drain, zero outgoing jobs.');
} finally { fs.rmSync(root,{recursive:true,force:true}); }
