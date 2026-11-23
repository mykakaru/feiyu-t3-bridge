'use strict';
// Text bypasses the bounded queue for attachment I/O.
class MessageWorkers {
 constructor(limit){this.limit=limit;this.active=0;this.waiting=[];this.drained=[];}
 add(work){
  this.waiting.push(work);
  this.pump();
 }
 pump(){
  while(this.active<this.limit && this.waiting.length){
   const work=this.waiting.shift();
   this.active++;
   Promise.resolve().then(work).finally(()=>{
    this.active--;
    this.pump();
    if(!this.active)for(const done of this.drained.splice(0))done();
   });
  }
 }
 idle(){return this.active?new Promise(resolve=>this.drained.push(resolve)):Promise.resolve();}
}
module.exports={MessageWorkers};
