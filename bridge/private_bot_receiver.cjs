'use strict';
// Dedicated API-mode WebSocket receiver + bidirectional outbound sender for the owner's private remote-control Bot.
// Inbound content is untrusted data. Outbound sends are driven exclusively by local root-written JSON requests.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {createRequire} = require('node:module');

const root = process.argv[2];
if (!root) throw new Error('Missing receiver runtime directory');
const botChannel = process.argv[3] || 'private_bot';

const os = require('node:os');
const localPkg = path.join(__dirname, 'package.json');
const envPkg = process.env.WECOM_SDK_PACKAGE_JSON;
const homeSdk = path.join(os.homedir(), 'AppData', 'Local', 'wecom-aibot-receiver', 'package.json');
const sdkPkg = [envPkg, localPkg, homeSdk].find(p => p && fs.existsSync(p)) || localPkg;
const sdkRequire = createRequire(sdkPkg);
const {WSClient} = sdkRequire('@wecom/aibot-node-sdk');
const {makeStatusWriter} = require('./status_writer.cjs');
const {receive, stage, recoverInterrupted} = require('./inbox.cjs');
const {MessageWorkers} = require('./message_workers.cjs');
const {downloadInWorker} = require('./media_download_worker.cjs');

// Recover any media staging interrupted by a previous crash or force-restart.
try { recoverInterrupted(root); } catch {}

const statePath = path.join(root, 'private-bot-status.json');
const stopPath = path.join(root, 'private-bot.stop');
const outboxDir = path.join(root, 'private-bot-outbox');
const resultsDir = path.join(root, 'private-bot-results');
fs.mkdirSync(outboxDir, {recursive: true});
fs.mkdirSync(resultsDir, {recursive: true});

const stamp = () => new Date().toISOString();
const heartbeatMs = 1000;
let state = {
  pid: process.pid,
  bot_channel: botChannel,
  version: '1.0',
  started_at: stamp(),
  state: 'starting',
  received: 0,
  sent: 0,
};
let client;
let closing = false;
let secrets = [];

function clean(value) {
  let s = String(value == null ? '' : value);
  for (const secret of secrets) {
    if (secret) s = s.split(secret).join('[redacted]');
  }
  return s.slice(0, 1000);
}

const writeStatus = makeStatusWriter(statePath);
function status(patch = {}) {
  Object.assign(state, patch, {updated_at: stamp()});
  return writeStatus(state);
}

function atomicWriteJson(target, obj) {
  fs.mkdirSync(path.dirname(target), {recursive: true});
  const tmp = target + '.' + crypto.randomUUID() + '.tmp';
  const fd = fs.openSync(tmp, 'wx');
  try {
    fs.writeFileSync(fd, JSON.stringify(obj, null, 2));
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  try {
    fs.renameSync(tmp, target);
  } finally {
    try { fs.unlinkSync(tmp); } catch {}
  }
}

const mediaWorkers = new MessageWorkers(3);
const inflightOutbox = new Set();
let outboxDirty = true;

try {
  const watcher = fs.watch(outboxDir, () => { outboxDirty = true; });
  watcher.on('error', () => { outboxDirty = true; });
} catch {}

async function handleOutboxFile(file) {
  const reqPath = path.join(outboxDir, file);
  let req;
  try {
    req = JSON.parse(fs.readFileSync(reqPath, 'utf8'));
  } catch {
    return;
  }
  if (!req || typeof req.req_id !== 'string' || !/^[a-zA-Z0-9_-]{4,80}$/.test(req.req_id)) {
    try { fs.unlinkSync(reqPath); } catch {}
    return;
  }
  const resPath = path.join(resultsDir, req.req_id + '.json');
  if (fs.existsSync(resPath)) {
    try { fs.unlinkSync(reqPath); } catch {}
    return;
  }
  const result = {
    req_id: req.req_id,
    chatid: req.chatid || '',
    kind: req.kind || 'markdown',
    success: false,
    errcode: -1,
    errmsg: '',
    started_at: stamp(),
  };
  try {
    let ack;
    if (req.kind === 'media') {
      const filePath = String(req.file_path || '');
      const mediaType = String(req.media_type || 'file');
      const filename = String(req.filename || path.basename(filePath));
      result.stage = 'read';
      if (!filePath || !fs.existsSync(filePath)) {
        const err = new Error('media_file_missing');
        err.errcode = -4;
        throw err;
      }
      const buf = fs.readFileSync(filePath);
      if (!buf.length) {
        const err = new Error('media_file_empty');
        err.errcode = -5;
        throw err;
      }
      result.stage = 'upload';
      let uploaded;
      let uploadErr;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          uploaded = await client.uploadMedia(buf, {type: mediaType, filename});
          uploadErr = null;
          break;
        } catch (err) {
          uploadErr = err;
          const code = typeof err?.errcode === 'number'
            ? err.errcode
            : (typeof err?.body?.errcode === 'number' ? err.body.errcode : -1);
          const msg = String(err?.errmsg || err?.message || '');
          const transient = [-1, 429, 45009, 45011].includes(code) || /timeout|not connected|system error/i.test(msg);
          if (!transient || attempt === 2 || closing || state.state !== 'authenticated') {
            throw err;
          }
          await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        }
      }
      if (uploadErr) throw uploadErr;
      if (!uploaded || typeof uploaded.media_id !== 'string' || !uploaded.media_id) {
        throw new Error('media_upload_missing_id');
      }
      const opts = {};
      if (req.title) opts.title = String(req.title);
      if (req.description) opts.description = String(req.description);
      result.stage = 'send';
      ack = await client.sendMediaMessage(String(req.chatid), mediaType, uploaded.media_id, opts);
    } else {
      result.stage = 'send';
      ack = await client.sendMessage(String(req.chatid), {
        msgtype: 'markdown',
        markdown: {content: String(req.text || '')},
      });
    }
    const errcode = typeof ack?.errcode === 'number' ? ack.errcode : (ack?.body?.errcode ?? -1);
    const errmsg = String(ack?.errmsg || ack?.body?.errmsg || '');
    result.errcode = errcode;
    result.errmsg = clean(errmsg);
    result.success = errcode === 0;
    result.ws_req_id = ack?.headers?.req_id || null;
    if (result.success) {
      status({sent: state.sent + 1, last_sent_at: stamp()});
    }
  } catch (err) {
    result.errcode = typeof err?.errcode === 'number'
      ? err.errcode
      : (typeof err?.body?.errcode === 'number' ? err.body.errcode : -1);
    const rawMsg = err?.errmsg || err?.body?.errmsg || err?.message || (typeof err === 'object' ? JSON.stringify(err) : String(err));
    result.errmsg = clean(rawMsg);
  }
  result.finished_at = stamp();
  try {
    atomicWriteJson(resPath, result);
  } catch {}
  try {
    fs.unlinkSync(reqPath);
  } catch {}
}

function drainOutbox() {
  if (closing || state.state !== 'authenticated' || !client) return;
  if (!outboxDirty && inflightOutbox.size === 0) {
    // Still sweep periodically
  }
  outboxDirty = false;
  let files = [];
  try {
    files = fs.readdirSync(outboxDir).filter(f => f.endsWith('.json'));
  } catch {
    return;
  }
  for (const file of files) {
    if (inflightOutbox.has(file) || inflightOutbox.size >= 4) continue;
    inflightOutbox.add(file);
    handleOutboxFile(file).finally(() => {
      inflightOutbox.delete(file);
    });
  }
}

function record(kind, frame) {
  if (closing) return;
  const body = frame?.body || {};
  if (kind === 'event') {
    const etype = body?.event?.eventtype || body?.eventtype || 'event';
    const uid = body?.from?.userid || null;
    status({last_event_at: stamp(), last_event_type: String(etype), last_event_userid: uid});
    return;
  }
  if (kind !== 'message') return;
  let queued;
  try {
    queued = stage(root, body, true, botChannel);
  } catch {
    status({last_processing_error: 'inbox_durable_stage_failed'});
    return;
  }
  if (queued.duplicate || queued.ignored) return;
  async function processMessage() {
    try {
      if (queued.media.length) {
        try {
          await require('./sealed_media.cjs').sealAsync(root, queued);
          queued.record.media_source_sealed = true;
        } catch {
          queued.record.media_source_sealed = false;
          queued.record.media_persistence_error = 'seal_failed';
        }
      }
      await receive(root, body, downloadInWorker, queued);
      status({
        last_received_at: stamp(),
        last_type: body?.msgtype,
        last_sender: body?.from?.userid || null,
        received: state.received + 1,
      });
    } catch {
      status({last_processing_error: 'inbox_processing_failed'});
    }
  }
  if (queued.media.length) mediaWorkers.add(processMessage);
  else void processMessage();
}

function stop(reason, code = 0) {
  if (closing) return;
  closing = true;
  try { client?.disconnect(); } catch {}
  status({state: 'draining', reason: clean(reason)});
  const timeout = setTimeout(() => {
    status({state: 'stopped', reason: clean(reason), drain_timeout: true});
    process.exit(code);
  }, 15000);
  Promise.allSettled([mediaWorkers.idle()]).finally(() => {
    clearTimeout(timeout);
    status({state: 'stopped', reason: clean(reason)});
    process.exit(code);
  });
}

process.on('SIGTERM', () => stop('SIGTERM'));
process.on('SIGINT', () => stop('SIGINT'));
process.on('uncaughtException', e => stop(e.message, 1));
process.on('unhandledRejection', e => stop(e?.message || 'unhandled rejection', 1));

let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', s => { input += s; });
process.stdin.on('end', () => {
  const config = JSON.parse(input);
  input = '';
  secrets = [config.secret, config.botId];
  client = new WSClient({
    botId: config.botId,
    secret: config.secret,
    heartbeatInterval: heartbeatMs,
    maxReconnectAttempts: -1,
    maxAuthFailureAttempts: 1,
    logger: {debug(){}, info(){}, warn(){}, error(){}},
  });
  if (client?.wsManager) {
    client.wsManager.replyAckTimeout = 30000;
  }
  client.on('connected', () => status({state: 'connected'}));
  client.on('authenticated', () => {
    status({state: 'authenticated', authenticated_at: stamp(), error: null});
    outboxDirty = true;
    drainOutbox();
  });
  client.on('reconnecting', attempt => status({state: 'reconnecting', attempt}));
  client.on('disconnected', reason => {
    if (closing) return;
    status({state: 'disconnected', reason: clean(reason)});
    if (String(reason).includes('New connection')) stop('Another client replaced this connection');
  });
  client.on('error', e => {
    status({state: 'error', error: clean(e.message), error_type: e.name});
    if (['WSAuthFailureError', 'WSReconnectExhaustedError'].includes(e.name)) stop(e.message, 1);
  });
  client.on('message', f => record('message', f));
  client.on('event', f => record('event', f));
  client.connect();

  setInterval(() => {
    if (fs.existsSync(stopPath)) {
      try { fs.unlinkSync(stopPath); } catch {}
      stop('local stop request');
    } else {
      status();
      drainOutbox();
    }
  }, 250);
});
status();
