import test from 'node:test';
import assert from 'node:assert/strict';
import {TelemetryOutbox, retryDelay} from './cab_watch/assets/telemetry.mjs';

test('pending detour survives refresh and is acknowledged before the return reading',()=>{
  const storage={data:new Map(),getItem(key){return this.data.get(key);},setItem(key,value){this.data.set(key,value);}};
  const queue=new TelemetryOutbox(storage,'manager-a');
  queue.append([{trip_id:'x',sequence:11,lat:13,lon:80}]);
  queue.append([{trip_id:'x',sequence:12,lat:13.01,lon:80}]);
  assert.throws(()=>queue.append([{sequence:13}]));
  const restored=new TelemetryOutbox(storage,'manager-a');
  assert.equal(restored.first()[0].sequence,11);
  assert.equal(new TelemetryOutbox(storage,'manager-b').length,0);
  restored.acknowledge();
  assert.equal(new TelemetryOutbox(storage,'manager-a').first()[0].sequence,12);
  restored.acknowledge();
  assert.equal(new TelemetryOutbox(storage,'manager-a').length,0);
});

test('retry respects server delay and stops on client errors',()=>{
  assert.equal(retryDelay({status:429,retryAfter:5000},0),5000);
  assert.equal(retryDelay({},9),30000);
  assert.equal(retryDelay({status:422},0),null);
});

test('large fleets upload bounded chunks without losing readings',()=>{
  const storage={value:null,getItem(){return this.value;},setItem(key,value){this.value=value;}};
  const queue=new TelemetryOutbox(storage,'manager');
  queue.append(Array.from({length:60},(_,id)=>({trip_id:String(id),sequence:1})));
  const received=[];
  while(queue.length){
    assert.ok(queue.first().length<=25);
    received.push(...queue.first());
    queue.acknowledge();
  }
  assert.equal(received.length,60);
  assert.equal(new Set(received.map(r=>r.trip_id)).size,60);
});

test('simulator checkpoint and original uploads survive refresh atomically',()=>{
  const storage={value:null,getItem(){return this.value;},setItem(k,v){this.value=v;}};
  const queue=new TelemetryOutbox(storage,'manager');
  const point={trip_id:'t',sequence:20,captured_at:'2020-01-01T00:00:20Z',lat:13,lon:80};
  const simulation={point:[300,400],sequence:20,motion:{mode:'detour',trail:['G','H'],target:'A'}};
  queue.append([point],{t:simulation});
  const refreshed=new TelemetryOutbox(storage,'manager');
  assert.deepEqual(refreshed.first(),[point]);assert.deepEqual(refreshed.simulations.t,simulation);
  refreshed.acknowledge();
  assert.equal(new TelemetryOutbox(storage,'manager').simulations.t.sequence,20);
  assert.equal(new TelemetryOutbox(storage,'manager').length,0);
  storage.setItem=()=>{throw Error('quota');};
  assert.throws(()=>refreshed.append([{...point,sequence:21}],{t:{...simulation,sequence:21}}));
  assert.equal(refreshed.simulations.t.sequence,20);assert.equal(refreshed.length,0);
});
test('a lost local session has no resumable simulations',()=>{
  const queue=new TelemetryOutbox({getItem(){return null;}},'manager');
  assert.deepEqual(queue.simulations,{});assert.equal(queue.length,0);
});
