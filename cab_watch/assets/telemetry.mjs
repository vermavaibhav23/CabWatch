// One atomic sessionStorage value owns simulator checkpoints AND pending GPS.
const BATCH_LIMIT=100;

export class TelemetryOutbox {
  constructor(storage, owner) {
    this.storage=storage;this.key='cabwatch.telemetry.'+owner;
    const saved=JSON.parse(storage.getItem(this.key)||'null');
    this.batches=(Array.isArray(saved)?saved:saved?.batches||[]).map(batch=>batch.map(({trip_id,sequence,captured_at,lat,lon})=>({trip_id,sequence,captured_at,lat,lon})));
    this.simulations=Array.isArray(saved)?{}:saved?.simulations||{};
  }
  get length(){return this.batches.length;}
  first(){return this.batches[0]?.slice(0,BATCH_LIMIT);}
  save(batches,simulations){
    // Assign memory only after durable browser storage succeeds.
    this.storage.setItem(this.key,JSON.stringify({version:2,batches,simulations}));
    this.batches=batches;this.simulations=simulations;
  }
  checkpoint(simulations){this.save(this.batches,{...this.simulations,...simulations});}
  append(batch,simulations={}){
    if(this.length>=2)throw Error('Telemetry buffer is full.');
    this.save([...this.batches,batch],{...this.simulations,...simulations});
  }
  acknowledge(){
    const remaining=this.batches[0].slice(BATCH_LIMIT);
    this.save(remaining.length?[remaining,...this.batches.slice(1)]:this.batches.slice(1),this.simulations);
  }
}

export function retryDelay(error, attempts) {
  if (error.status && ![408, 429, 500, 502, 503, 504].includes(error.status)) return null;
  return Math.max(error.retryAfter || 0, Math.min(30000, 1000 * 2 ** Math.min(attempts, 5)));
}
