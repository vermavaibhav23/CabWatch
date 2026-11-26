import {TelemetryOutbox,retryDelay} from './telemetry.mjs?v=batch100';
import {makeCab,motionSnapshot,divert,rejoin,advance} from './motion.mjs?v=retrace1';
import {cityMap,makeLayers,drawSharedRoads,selectMap,showPoint,animate} from './map.mjs?v=status1';

const $=id=>document.getElementById(id);
const colors=[
    "#2164cb", "#a53685", "#00796b", "#be5422", "#664bac", "#a62844",
    "#11759c", "#637521", "#ab400f", "#594ba6", "#08805a", "#ad365e",
    "#38578c", "#885a0b", "#426d57", "#804991", "#2d7782", "#8c5436",
    "#536aaf", "#923b32",
];
const stage=c=>c.liveCompleted?'Completed':!c.resumable?'Display only · local session unavailable':c.mode==='arrived'?'Arrival queued for processing':'Locally resumable';
let fleet,selected,running=false,frame,timer,changing=false,reviewGeneration=0,network,dispatchKey;
const msg=text=>{$('message').textContent=text;};
const element=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};

async function api(path,method='GET',body,extraHeaders={}){
  const token=sessionStorage.getItem('cabwatch.token');
  const headers={'Content-Type':'application/json',...extraHeaders};
  if(token && !path.startsWith('auth/'))headers.Authorization='Bearer '+token;
  const response=await fetch('/cab-watch/'+path,{method,cache:'no-store',headers,
    body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(35000)});
  if(!response.ok){
    if(response.status===401 && !path.startsWith('auth/'))signOut('Please sign in again.');
    const detail=await response.json().catch(()=>({}));
    const message=typeof detail.detail==='string'?detail.detail:Array.isArray(detail.detail)?detail.detail[0].msg:'Request failed ('+response.status+'). Try again.';
    const error=Error(message);error.status=response.status;
    error.retryAfter=Number(response.headers.get('Retry-After')||0)*1000;
    throw error;
  }
  return response.json();
}
function chosen(){return fleet?.cabs.find(c=>c.id===selected);}
function select(id){selected=id;selectMap(fleet.cabs,id);render();loadAlerts(fleet,id);}
function alertFor(c){if(fleet.socketStates)return fleet.socketStates[c.id]?.alert;return fleet.alerts.find(a=>a.trip_id===c.id&&a.closed_sequence===null&&!a.tracking_delayed&&!c.delayed);}


function render(){
  if(!fleet){
    $('create').disabled=true;$('play').disabled=true;
    $('total').textContent='—';$('open').textContent='—';$('mode').textContent='Loading trips';
    return;
  }
  $('clock').textContent=[Math.floor(fleet.seconds/60),fleet.seconds%60].map(s=>String(s).padStart(2,'0')).join(':');
  $('total').textContent=fleet.cabs.filter(c=>!c.liveCompleted).length;
  $('open').textContent=fleet.cabs.filter(c=>alertFor(c)).length;
  const moving=fleet.cabs.some(c=>c.resumable&&c.mode!=='arrived');
  $('mode').textContent=running?'● Running':moving?'Paused':'Ready';
  $('play').textContent=running?'Ⅱ Pause':'▶ Start';
  $('play').disabled=changing||(!moving&&!fleet.outbox?.length);
  $('create').disabled=changing||!$('planner').hidden;
  for(const c of fleet.cabs){
    c.row.setAttribute('aria-pressed',String(c.id===selected));
    const alarm=alertFor(c);
    c.row.classList.toggle('alarm',Boolean(alarm));
    c.map.pin.classList.toggle('offroute',Boolean(alarm));
    c.offRoute=Boolean(alarm);
    c.badge.textContent=alarm?'OFF ROUTE':c.mode==='turn requested'?'Turn queued':
      c.mode==='detour'?'Route changed':stage(c);
    c.desc.textContent=alarm?'Confirmed incident · peak '+Math.round(alarm.peak_metres)+' m · opened GPS #'+alarm.opened_sequence:c.origin+' → '+c.destination;
  }
  const c=chosen();
  if(!c){
    $('selected').textContent='Select a cab';$('tripline').textContent='Choose pickup and drop-off to start a trip.';
    $('distance').classList.remove('offroute-details');
    $('distance').textContent='';$('wrong').disabled=true;$('return').disabled=true;
  }else{
    $('selected').textContent=c.vehicle_id+' · '+c.origin+' → '+c.destination;
    $('tripline').textContent=c.origin+' → '+c.destination+' · '+(c.metres/1000).toFixed(2)+' km · '+stage(c);
    const alarm=alertFor(c);
    const captured=c.localSimulation?c.capturedAt:c.displayCapturedAt;
    $('distance').classList.toggle('offroute-details',Boolean(alarm));
    $('distance').textContent=(c.localSimulation?'Local simulation GPS #'+c.sequence:'Last loaded GPS #'+c.confirmed)+
      ' · Captured '+(captured?new Date(captured).toLocaleString():'not yet')+
      (alarm?' · OFF ROUTE confirmed at GPS #'+alarm.opened_sequence+' · '+new Date(alarm.opened_at).toLocaleString()+' · incident peak '+Math.round(alarm.peak_metres)+' m':' · No backend-confirmed open incident');
    $('wrong').disabled=changing||!c.resumable||c.mode!=='route'||c.index+1>=c.route.length-1;
    $('return').disabled=changing||!c.resumable||!['detour','turn requested'].includes(c.mode);
  }
  const selectedAlerts=fleet.incidents?.[selected]||fleet.alerts.filter(a=>a.trip_id===selected);
  const logKey=JSON.stringify([selected,selectedAlerts.map(a=>[a.id,a.closed_sequence,a.tracking_delayed,Math.round(a.peak_metres)])]);
  if(logKey!==fleet.logKey){
    fleet.logKey=logKey;const log=$('alerts');log.replaceChildren();
    if(!selectedAlerts.length)log.append(element('p','No recorded deviations for this trip.','muted'));
    for(const a of selectedAlerts){
      const row=element('div',undefined,'logrow'),info=element('div');
      info.append(element('strong',a.vehicle_id+' · '+(a.closed_sequence!==null?'Resolved':'Open at last processed reading')),
        element('small','Captured '+new Date(a.opened_at).toLocaleString()+' · GPS #'+a.opened_sequence+' · peak '+Math.round(a.peak_metres)+' m'));
      const b=element('button','Explain alert');b.onclick=()=>explain(a);row.append(info,b);log.append(row);
    }
  }
}
async function explain(a){
  const f=fleet,generation=++reviewGeneration,pane=$('review');
  pane.hidden=false;pane.textContent='Checking the recorded GPS evidence…';
  try{
    const r=await api('me/alerts/'+a.id+'/explain','POST');
    if(f!==fleet||generation!==reviewGeneration)return;
    pane.replaceChildren(element('strong',r.source+' · '+a.vehicle_id+' · '+r.facts.status+' at review'),
      element('p',r.summary),
      element('p','Opened at GPS #'+r.facts.opened_sequence+' · peak '+r.facts.peak_m+' m'));
    if(r.notice)pane.append(element('small',r.notice));
  }catch(e){if(f===fleet&&generation===reviewGeneration)pane.textContent=e.message;}
}
function draw(now){if(fleet&&animate(fleet.cabs,now))frame=requestAnimationFrame(draw);}
function gps(f,c){
  return {trip_id:c.id,sequence:c.sequence,captured_at:c.capturedAt,
    lat:f.origin[0]-c.point[1]*f.metres/f.lat_scale,
    lon:f.origin[1]+c.point[0]*f.metres/f.lon_scale};
}
function applyReceipt(f,result){
  const cab=f.cabs.find(c=>c.id===result.trip_id);
  if(!cab || result.sequence<cab.confirmed)return;
  cab.confirmed=result.sequence;cab.offset=result.distance_m;
  if(result.alert){
    const alert={...result.alert,vehicle_id:cab.vehicle_id};
    f.alerts=[alert,...f.alerts.filter(a=>a.id!==alert.id)];
  }
  if(result.completed)cab.mode='arrived';
}
async function sendPositions(f){
  if(f.working || !f.outbox.length || f.retryTimer)return;
  f.working=true;
  try{
    while(f===fleet && f.outbox.length){
      const response=await api('me/gps','POST',{readings:f.outbox.first()});
      if(f!==fleet)return;
      for(const result of response.results)if(!result.queued)applyReceipt(f,result);
      f.outbox.acknowledge();f.retryAttempts=0;
      $('connection').textContent=response.queued?'GPS queued for processing':'● Connected';render();
    }
  }catch(e){
    if(f===fleet){
      const delay=retryDelay(e,f.retryAttempts||0);
      if(delay!==null){
        f.retryAttempts=(f.retryAttempts||0)+1;
        $('connection').textContent='Retrying GPS upload';
        f.retryTimer=setTimeout(()=>{f.retryTimer=null;sendPositions(f);},delay+Math.random()*250);
      }else{
        running=false;f.failed=true;$('connection').textContent='Connection interrupted';
        msg(e.message+' Press Start to retry these positions.');
      }
    }
  }finally{f.working=false;if(f===fleet)render();}
}
function schedule(f,delay){clearTimeout(timer);timer=setTimeout(()=>tick(f),delay);}
function tick(f){
  if(f!==fleet || f.failed)return;
  // Allow movement while a request is in flight, but bound unsent work on slow networks.
  if(running && f.outbox.length<2){
    const before=f.cabs.map(c=>({...c,recovery:c.recovery?[...c.recovery]:undefined}));
    const moving=f.cabs.filter(c=>c.resumable&&advance(c));
    if(moving.length){
      const time=new Date().toISOString();
      for(const cab of moving)cab.capturedAt=time;
      const batch=moving.map(c=>gps(f,c));
      try{f.outbox.append(batch,simulationSnapshots(f));}
      catch(e){
        f.cabs.forEach((cab,index)=>Object.assign(cab,before[index]));
        running=false;f.failed=true;msg('Unable to save pending GPS locally. '+e.message);render();return;
      }
      // Animate persisted local movement immediately, without waiting for Kafka.
      for(const cab of moving)showPoint(cab,cab.point);
      cancelAnimationFrame(frame);draw(performance.now());
      f.seconds+=5;
    }else running=false;
  }
  sendPositions(f);
  render();
  if(running)schedule(f,1000);
}
function closePlanner(){
  $('planner').hidden=true;$('map').classList.remove('picking');
  for(const node of document.querySelectorAll('.junction'))node.setAttribute('tabindex','-1');
}
function updatePlanner(){
  const a=$('pickup').value,b=$('dropoff').value;
  $('launch').disabled=changing||!a||!b||a===b;
  $('pickHint').textContent=!a?'Choose a pickup on the map.':!b?'Pickup '+a+' selected. Click a drop-off to start.':
    a===b?'Choose a different drop-off.':'Ready to dispatch to '+a+' → '+b+'.';
}
function pick(name){
  if($('planner').hidden||changing)return;
  if(!$('pickup').value){$('pickup').value=name;updatePlanner();return;}
  $('dropoff').value=name;updatePlanner();
  if(!$('launch').disabled)launch();
}
function openPlanner(){
  dispatchKey=crypto.randomUUID();
  $('pickup').value='';$('dropoff').value='';$('planner').hidden=false;
  $('map').classList.add('picking');
  for(const node of document.querySelectorAll('.junction'))node.setAttribute('tabindex','0');
  updatePlanner();render();
}
function showCabInRoster(v){
  if(v.row)return;
  if(!$('roster').querySelector('.cabrow'))$('roster').replaceChildren();
  v.row=element('button',undefined,'cabrow');
  v.row.style.setProperty('--cab',v.color);
  const top=element('span',undefined,'rowtop'),id=element('span',undefined,'cabid');
  id.append(element('i',undefined,'swatch'),element('strong',v.vehicle_id));
  v.badge=element('span','With customer','badge');top.append(id,v.badge);
  v.desc=element('span',undefined,'desc');v.row.append(top,v.desc);
  v.row.onclick=()=>select(v.id);
  $('roster').append(v.row);
}
async function launch(){
  if(changing||$('launch').disabled)return;
  const f=fleet;changing=true;
  for(const id of ['launch','cancel','pickup','dropoff'])$(id).disabled=true;
  msg('Planning the route…');
  try{
    const trip=await api('me/trips','POST',
      {pickup:$('pickup').value,dropoff:$('dropoff').value},{'Idempotency-Key':dispatchKey});
    if(f!==fleet)return;
    let c=f.cabs.find(c=>c.id===trip.id);
    if(!c){
      const fresh=makeCab(trip,network);
      f.outbox.checkpoint({[trip.id]:{point:fresh.point,sequence:0,motion:motionSnapshot(fresh),capturedAt:null}});
      c=restoreTrip(trip);
    }
    drawSharedRoads(f.cabs,network);
    closePlanner();running=true;f.failed=false;select(c.id);schedule(f,1000);
    msg(c.vehicle_id+' started at '+c.origin+' for '+c.destination+'.');
  }catch(e){msg(e.message);}
  finally{
    changing=false;for(const id of ['cancel','pickup','dropoff'])$(id).disabled=false;
    updatePlanner();render();
  }
}
function restoreTrip(trip){
  const c=makeCab(trip,network);
  const plate=trip.vehicle_id;
  c.cab=Number(plate.startsWith('CB#') ? plate.slice(3) : plate.slice(-4));
  c.color=colors[(c.cab+colors.length-1)%colors.length];
  c.capturedAt=trip.captured_at;
  c.confirmed=c.sequence=trip.last_sequence;c.offset=trip.distance_m;
  c.liveCompleted=trip.completed;c.displayCapturedAt=trip.captured_at;
  if(trip.lat!==null)c.point=[(trip.lon-fleet.origin[1])*fleet.lon_scale/fleet.metres,
    (fleet.origin[0]-trip.lat)*fleet.lat_scale/fleet.metres];
  if(trip.completed)c.mode='arrived';
  const local=fleet.outbox.simulations[trip.id];
  c.localSimulation=Boolean(local&&local.sequence>=trip.last_sequence);
  c.resumable=c.localSimulation&&!trip.completed;
  if(c.localSimulation){
    const resumed=makeCab({...trip,motion:local.motion},network);
    for(const key of ['mode','index','trail','to','recovery'])c[key]=resumed[key];
    c.sequence=local.sequence;c.capturedAt=local.capturedAt;c.point=[...local.point];
    if(trip.completed)c.mode='arrived';
  }
  fleet.cabs.push(c);makeLayers([c],select);showPoint(c,c.point);
  showCabInRoster(c);
  return c;
}

let incidentGeneration=0;
async function loadAlerts(f,id){
  if(!id)return;
  const generation=++incidentGeneration,version=f.incidentVersion||0;
  try{
    const view=await api('me/trips/'+id+'/alerts');
    if(f!==fleet||id!==selected||generation!==incidentGeneration)return;
    const merged=new Map(view.alerts.map(a=>[a.id,a]));
    // A snapshot received during this request takes precedence over its older log.
    if(version!==(f.incidentVersion||0))for(const a of f.alerts)if(a.trip_id===id)merged.set(a.id,a);
    f.incidents ||= {};f.incidents[id]=[...merged.values()].sort((a,b)=>a.opened_sequence-b.opened_sequence);render();
  }catch(e){if(f===fleet)msg(e.message);}
}
function simulationSnapshots(f){
  return Object.fromEntries(f.cabs.filter(c=>c.resumable).map(c=>[c.id,{
    point:[...c.point],sequence:c.sequence,capturedAt:c.capturedAt,motion:motionSnapshot(c)
  }]));
}
function changeMotion(action){
  const c=chosen();if(!c?.resumable)return;
  const before={...c,trail:[...c.trail],recovery:c.recovery?[...c.recovery]:undefined};
  try{action(c);fleet.outbox.checkpoint(simulationSnapshots(fleet));render();}
  catch(e){Object.assign(c,before);msg('Unable to save simulator state: '+e.message);}
}
// One recovery read per socket reconnect, never a recurring browser poll.
async function recoverTrips(f){
  try{
    const view=await api('me/trips');
    if(f!==fleet)return;
    for(const trip of view.trips){
      let c=f.cabs.find(c=>c.id===trip.id);
      if(!c){c=restoreTrip(trip);drawSharedRoads(f.cabs,network);}
      if(!c.localSimulation){
        c.confirmed=trip.last_sequence;c.offset=trip.distance_m;c.displayCapturedAt=trip.captured_at;
        if(trip.lat!==null)showPoint(c,[(trip.lon-f.origin[1])*f.lon_scale/f.metres,(f.origin[0]-trip.lat)*f.lat_scale/f.metres]);
      }
    }
    loadAlerts(f,selected);
    cancelAnimationFrame(frame);draw(performance.now());render();
  }catch(e){if(f===fleet)msg('Fleet recovery failed. Refresh to retry. '+e.message);}
}
function applyStatus(f,message){
  f.incidentVersion=(f.incidentVersion||0)+1;
  f.socketStates=Object.fromEntries(message.vehicles.map(v=>[v.trip_id,v]));
  f.alerts=message.alerts;
  for(const [id,incidents] of Object.entries(f.incidents||{})){
    const merged=new Map(incidents.map(a=>[a.id,a]));
    for(const alert of message.alerts)if(alert.trip_id===id)merged.set(alert.id,alert);
    f.incidents[id]=[...merged.values()].sort((a,b)=>a.opened_sequence-b.opened_sequence);
  }
  for(const c of f.cabs){
    const state=f.socketStates[c.id];
    if(!state)continue;
    c.liveCompleted=state.completed;
    if(state.completed){c.mode='arrived';c.resumable=false;}
    // Status changes must never overwrite c.point or the map's local target.
  }
}
function stopAlertSocket(f){
  if(!f)return;
  clearTimeout(f.socketRetry);f.socketStopped=true;f.socket?.close();
}
function connectAlerts(f){
  if(f!==fleet||f.socketStopped)return;
  const socket=f.socket=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/cab-watch/me/alerts/ws');
  socket.onopen=()=>socket.send(JSON.stringify({token:sessionStorage.getItem('cabwatch.token')}));
  let firstSnapshot=true;
  socket.onmessage=event=>{
    if(f!==fleet||f.socket!==socket)return;
    const message=JSON.parse(event.data);
    if(message.type!=='off_route_snapshot')return;
    applyStatus(f,message);
    if(firstSnapshot){
      firstSnapshot=false;
      if(f.socketConnected)recoverTrips(f);
      f.socketConnected=true;
    }
    $('alertConnection').textContent='● Alert updates connected · backend-confirmed incidents';
    render();
  };
  socket.onclose=event=>{
    if(f!==fleet||f.socketStopped)return;
    $('alertConnection').textContent='Alert connection lost · showing last confirmed status · reconnecting';
    render();
    if(event.code===4401){signOut('Please sign in again.');return;}
    f.socketRetry=setTimeout(()=>connectAlerts(f),1500);
  };
  socket.onerror=()=>socket.close();
}

async function setup(){
  if(changing)return;
  changing=true;
  try{
    const token=sessionStorage.getItem('cabwatch.token');
    const subject=JSON.parse(atob(token.split('.')[1].replace(/-/g,'+').replace(/_/g,'/'))).sub;
    const outbox=new TelemetryOutbox(sessionStorage,subject);
    while(outbox.length){
      await api('me/gps','POST',{readings:outbox.first()});
      outbox.acknowledge();
    }
    network=network||await api('network');
    const created=await api('me/trips');
    running=false;clearTimeout(timer);cancelAnimationFrame(frame);reviewGeneration++;selected=null;closePlanner();
    clearTimeout(fleet?.retryTimer);
    stopAlertSocket(fleet);
    fleet={outbox,...network,...created,savedAlerts:created.alerts,cabs:[],seconds:0,working:false};
    cityMap(network,pick);closePlanner();
    for(const id of ['routes','shared','pins'])$(id).replaceChildren();
    for(const id of ['pickup','dropoff']){
      $(id).replaceChildren(new Option('Select node',''));
      for(const n of Object.keys(network.nodes))$(id).add(new Option(n,n));
    }
    $('roster').replaceChildren(element('p','Add a trip to see its cab.','inner muted'));
    for(const trip of created.trips)restoreTrip(trip);
    drawSharedRoads(fleet.cabs,network);
    selected=(fleet.cabs.find(c=>c.mode!=='arrived')||fleet.cabs.at(-1))?.id;
    if(selected){selectMap(fleet.cabs,selected);draw(performance.now());}
    $('alerts').textContent='No recorded deviations for this trip.';$('review').hidden=true;
    $('connection').textContent='● Ready';
    msg(fleet.cabs.some(c=>c.resumable)?'Press Start to resume this tab’s saved simulation.':fleet.cabs.length?'Existing trips are display only. Add a trip to simulate here.':'Add a trip to choose a pickup and drop-off.');
    connectAlerts(fleet);loadAlerts(fleet,selected);
  }catch(e){
    msg(e.message+' Try again.');$('connection').textContent='Offline';
    const delay=retryDelay(e,0);
    if(delay!==null && sessionStorage.getItem('cabwatch.token')){
      clearTimeout(timer);timer=setTimeout(()=>setup(),delay+Math.random()*250);
    }
  }
  finally{changing=false;render();}
}

$('pickup').onchange=$('dropoff').onchange=updatePlanner;
$('launch').onclick=launch;$('cancel').onclick=()=>{closePlanner();render();};
$('create').onclick=openPlanner;
$('play').onclick=()=>{if(!fleet)return;running=!running;fleet.failed=false;render();
  msg(running?'Simulation running · each update advances five simulated seconds.':'Simulation paused.');schedule(fleet,0);};
$('wrong').onclick=()=>{changeMotion(divert);
  msg(running?'Turn queued for the next junction.':'Turn queued for the next junction. Press Start to drive.');};
$('return').onclick=()=>{changeMotion(rejoin);
  msg(running?'Cab is retracing its detour.':'Return requested. Press Start to drive.');};
let registering=false;
function signOut(message=''){
  stopAlertSocket(fleet);
  sessionStorage.removeItem('cabwatch.token');sessionStorage.removeItem('cabwatch.name');
  clearTimeout(fleet?.retryTimer);fleet=null;running=false;selected=null;clearTimeout(timer);cancelAnimationFrame(frame);
  $('dashboard').hidden=true;$('account').hidden=false;$('authMessage').textContent=message;
  $('connection').textContent='Signed out';$('password').value='';
}
async function openDashboard(){
  $('signedName').textContent=sessionStorage.getItem('cabwatch.name')||'My fleet';
  $('account').hidden=true;$('dashboard').hidden=false;await setup();
}
$('switchAuth').onclick=()=>{
  registering=!registering;
  $('authTitle').textContent=$('authSubmit').textContent=registering?'Create account':'Sign in';
  $('switchAuth').textContent=registering?'Use an existing account':'Create an account';
  $('nameLabel').hidden=!registering;$('accountName').required=registering;
  $('password').autocomplete=registering?'new-password':'current-password';
  $('authMessage').textContent='';
};
$('authForm').onsubmit=async event=>{
  event.preventDefault();$('authSubmit').disabled=true;
  try{
    const body={email:$('email').value,password:$('password').value};
    if(registering)body.name=$('accountName').value;
    const account=await api(registering?'auth/register':'auth/login','POST',body);
    sessionStorage.setItem('cabwatch.token',account.access_token);
    sessionStorage.setItem('cabwatch.name',account.name);$('password').value='';
    await openDashboard();
  }catch(e){$('authMessage').textContent=e.message;}
  finally{$('authSubmit').disabled=false;}
};
$('logout').onclick=()=>{signOut();location.reload();};
if(sessionStorage.getItem('cabwatch.token'))openDashboard();
else signOut();

