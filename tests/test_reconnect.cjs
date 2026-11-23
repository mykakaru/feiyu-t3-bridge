const assert=require('node:assert/strict');
const {sdk}=require('../bridge/scoped_media.cjs').sdkRequire();
const logger={debug(){},info(){},warn(){},error(){}};
(async()=>{
const c=new sdk.WSClient({botId:'fixture',secret:'fixture',maxReconnectAttempts:-1,maxAuthFailureAttempts:1,logger});
const m=c.wsManager;m.reconnectBaseDelay=1;m.reconnectMaxDelay=1;m.reconnectAttempts=11;
let calls=0;m.connect=()=>calls++;m.scheduleReconnect();await new Promise(r=>setTimeout(r,20));assert.equal(calls,1);assert.equal(m.reconnectAttempts,12);
m.isManualClose=true;m.scheduleReconnect();await new Promise(r=>setTimeout(r,20));assert.equal(calls,1);
const d=new sdk.WSClient({botId:'fixture',secret:'fixture',maxReconnectAttempts:10,logger});let error;d.wsManager.reconnectAttempts=10;d.wsManager.onError=e=>error=e;d.wsManager.scheduleReconnect();assert.equal(error.name,'WSReconnectExhaustedError');
console.log(JSON.stringify({sdk_offline_test:true,infinite_retry_past_10:true,manual_stop_honored:true,default_10_exhaustion_reproduced:true,network_disconnected:false}));
})();
