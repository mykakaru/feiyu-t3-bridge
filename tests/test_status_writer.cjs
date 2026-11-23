const assert=require('node:assert/strict');const {makeStatusWriter}=require('../bridge/status_writer.cjs');
let calls=0,logs=[],delay=0;
const io={writeFileSync(){},renameSync(){if(++calls<=2)throw Object.assign(Error('fixture'),{code:'EPERM'});},appendFileSync(p,x){logs.push(JSON.parse(x));}};
let w=makeStatusWriter('C:/fixture/status.json',io,ms=>delay+=ms);assert(w({state:'authenticated'}));assert.equal(calls,3);assert.equal(delay,60);assert.equal(logs.length,0);
io.renameSync=()=>{throw Object.assign(Error('fixture'),{code:'EPERM'});};assert.equal(w({}),false);assert.equal(w({}),false);assert.equal(logs.length,1);assert.equal(logs[0].kind,'write_failed');
io.renameSync=()=>{};assert(w({}));assert.equal(logs[1].kind,'recovered');
io.writeFileSync=()=>{throw Object.assign(Error('fixture'),{code:'ENOSPC'});};assert.equal(w({}),false);assert.equal(logs[2].code,'ENOSPC');
console.log('PASS transient-lock retry, persistent-lock nonthrow+dedup, recovery event, disk-full nonthrow. Fixture only.');
