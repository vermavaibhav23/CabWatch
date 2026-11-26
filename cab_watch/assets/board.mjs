import {TelemetryOutbox,retryDelay} from './telemetry.mjs?v=route2';
import {makeCab,motionSnapshot,divert,rejoin,advance} from './motion.mjs?v=retrace1';
import {cityMap,makeLayers,drawSharedRoads,selectMap,showPoint,animate} from './map.mjs?v=alerts1';

const $=id=>document.getElementById(id);
const colors=[
    "#2164cb", "#a53685", "#00796b", "#be5422", "#664bac", "#a62844",
    "#11759c", "#637521", "#ab400f", "#594ba6", "#08805a", "#ad365e",
    "#38578c", "#885a0b", "#426d57", "#804991", "#2d7782", "#8c5436",
    "#536aaf", "#923b32",
];
const stage=c=>c.mode==='arrived'?(fleet?.telemetry_mode==='kafka'&&!c.liveCompleted?'Awaiting history processing':'Completed'):'With customer';
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
function select(id){selected=id;selectMap(fleet.cabs,id);render();loadHistory(fleet,id);}
function alertFor(c){if(fleet.socketStates)return fleet.socketStates[c.id]?.alert;return fleet.alerts.find(a=>a.trip_id===c.id&&a.closed_sequence===null&&!a.tracking_delayed&&!c.delayed);}


function render(){
  if(!fleet){
    $('create').disabled=true;$('play').disabled=true;
    $('total').textContent='—';$('open').textContent='—';$('mode').textContent='Loading trips';
    return;
  }
  $('clock').textContent=[Math.floor(fleet.seconds/60),fleet.seconds%60].map(s=>String(s).padStart(2,'0')).join(':');
  $('total').textContent=fleet.cabs.filter(c=>c.mode!=='arrived').length;
  $('open').textContent=fleet.cabs.filter(c=>alertFor(c)).length;
  const moving=fleet.cabs.some(c=>c.mode!=='arrived');
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
    c.badge.textContent=alarm?'OFF ROUTE':c.confirmed<c.sequence?'History processing':c.mode==='turn requested'?'Turn queued':
      c.mode==='detour'?'Route changed':stage(c);
    const evidence=fleet.socketStates?.[c.id];
    c.desc.textContent=alarm?Math.round(evidence?.distance_m??c.offset)+' m from route · GPS #'+(evidence?.sequence??c.confirmed)+' · Captured '+new Date(evidence?.captured_at??c.displayCapturedAt).toLocaleTimeString():c.origin+' → '+c.destination;
  }
  const c=chosen();
  if(!c){
    $('selected').textContent='Select a cab';$('tripline').textContent='Choose pickup and drop-off to start a trip.';
    $('distance').classList.remove('offroute-details');
    $('distance').textContent='';$('wrong').disabled=true;$('return').disabled=true;
  }else{
    $('selected').textContent=c.vehicle_id+' · '+c.origin+' → '+c.destination;
    $('tripline').textContent=c.origin+' → '+c.destination+' · '+(c.metres/1000).toFixed(2)+' km · '+stage(c);
    const evidence=fleet.socketStates?.[c.id];
    const captured=evidence?.captured_at??c.displayCapturedAt;
    $('distance').classList.toggle('offroute-details',Boolean(alertFor(c)));
    $('distance').textContent=(alertFor(c)?'OFF ROUTE · ':'')+Math.round(fleet.socketStates?.[c.id]?.distance_m??c.offset)+' m from planned route · GPS #'+(evidence?.sequence??c.confirmed)+
      ' · Recorded '+(captured?new Date(captured).toLocaleString():'not yet')+' · Historical confirmation: 3 consecutive readings';
    $('wrong').disabled=changing||c.mode!=='route'||c.index+1>=c.route.length-1;
    $('return').disabled=changing||!['detour','turn requested'].includes(c.mode);
  }
  const selectedAlerts=fleet.historyAlerts?.[selected]||fleet.alerts.filter(a=>a.trip_id===selected);
  const logKey=JSON.stringify([selected,selectedAlerts.map(a=>[a.id,a.closed_sequence,a.tracking_delayed,Math.round(a.peak_metres)])]);
  if(logKey!==fleet.logKey){
    fleet.logKey=logKey;const log=$('alerts');log.replaceChildren();
    if(!selectedAlerts.length)log.append(element('p','No recorded deviations for this trip.','muted'));
    for(const a of selectedAlerts){
      const row=element('div',undefined,'logrow'),info=element('div');
      info.append(element('strong',a.vehicle_id+(a.historical===false?' · Legacy record':'')+' · '+(a.tracking_delayed?'Tracking delayed':a.closed_sequence?(a.close_reason||'Resolved'):'Open at last recorded sample')),
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
    motion:motionSnapshot(c),
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
    const moving=f.cabs.filter(c=>advance(c));
    if(moving.length){
      const time=new Date().toISOString();
      for(const cab of moving)cab.capturedAt=time;
      const batch=moving.map(c=>gps(f,c));
      try{f.outbox.append(batch);}
      catch(e){
        f.cabs.forEach((cab,index)=>Object.assign(cab,before[index]));
        running=false;f.failed=true;msg('Unable to save pending GPS locally. '+e.message);render();return;
      }
      // Map positions come from persisted journey history.
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
    const c=f.cabs.find(c=>c.id===trip.id)||restoreTrip(trip);
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
  const recordedPoint=[...c.point];
  if(trip.accepted_point && trip.accepted_point.sequence>c.sequence){
    const a=trip.accepted_point,resumed=makeCab({...trip,last_sequence:a.sequence,motion:a.motion},network);
    for(const key of ['mode','index','trail','to','recovery'])c[key]=resumed[key];
    c.sequence=a.sequence;c.capturedAt=a.captured_at;
    c.point=[(a.lon-fleet.origin[1])*fleet.lon_scale/fleet.metres,(fleet.origin[0]-a.lat)*fleet.lat_scale/fleet.metres];
  }
  fleet.cabs.push(c);makeLayers([c],select);showPoint(c,recordedPoint);
  showCabInRoster(c);
  return c;
}

let historyGeneration=0,historyCursor=0;
async function loadHistory(f,id,append=false){
  if(!id)return;
  const generation=++historyGeneration;
  if(!append){historyCursor=0;$('historyRows').replaceChildren();$('historyStatus').textContent='Loading recorded journey…';}
  try{
    const view=await api('me/trips/'+id+'/history?after='+(append?historyCursor:0));
    if(f!==fleet||id!==selected||generation!==historyGeneration)return;
    for(const p of view.readings){
      const row=element('tr');
      for(const value of [p.sequence,new Date(p.captured_at).toLocaleString(),
          p.lat.toFixed(5)+', '+p.lon.toFixed(5),Math.round(p.distance_m||0)+' m',
          p.timestamp_reversed?'Timestamp reversed':p.sequence_gap?'Sequence gap':p.off_route_sample?'Off route':'On route'])row.append(element('td',value));
      $('historyRows').append(row);
    }
    f.historyAlerts ||= {};
    f.historyAlerts[id]=view.alerts.map(a=>({...a,id:a.event_key||a.id,vehicle_id:f.cabs.find(c=>c.id===id)?.vehicle_id}));
    render();
    historyCursor=view.next_after;
    $('historyMore').hidden=!view.has_more;
    $('historyStatus').textContent=view.total+' recorded readings · processed through #'+view.processed_sequence+
      ' · accepted through #'+view.accepted_sequence+' · '+(view.accepted_sequence>view.processed_sequence?'Processing pending':'History caught up')+
      '. Gaps or reversed timestamps interrupt incident confirmation.';
  }catch(e){if(f===fleet&&id===selected)$('historyStatus').textContent=e.message;}
}
$('historyMore').onclick=()=>loadHistory(fleet,selected,true);
$('historyRefresh').onclick=()=>loadHistory(fleet,selected);
async function pollHistory(f){
  if(f!==fleet||f.polling)return;
  f.polling=true;
  try{
    const view=await api('me/trips');
    if(f!==fleet)return;
    f.alerts=view.alerts;
    if(selected && f.cabs.find(c=>c.id===selected)?.confirmed !== view.trips.find(t=>t.id===selected)?.last_sequence)loadHistory(f,selected);
    for(const trip of view.trips){
      let c=f.cabs.find(c=>c.id===trip.id);
      if(!c){c=restoreTrip(trip);drawSharedRoads(f.cabs,network);}
      c.confirmed=trip.last_sequence;c.offset=trip.distance_m;c.displayCapturedAt=trip.captured_at;
      c.liveCompleted=trip.completed;c.delayed=false;
      if(trip.lat!==null){
        const point=[(trip.lon-f.origin[1])*f.lon_scale/f.metres,(f.origin[0]-trip.lat)*f.lat_scale/f.metres];
        showPoint(c,point);
      }
      if(trip.completed)c.mode='arrived';
    }
    $('connection').textContent=view.trips.some(t=>t.processing_pending)?'Journey history processing':'Recorded history updated';
    cancelAnimationFrame(frame);draw(performance.now());render();
  }catch(e){if(f===fleet)$('connection').textContent='History unavailable — retrying';}
  finally{f.polling=false;if(f===fleet)f.pollTimer=setTimeout(()=>pollHistory(f),2000);}
}
function stopAlertSocket(f){
  if(!f)return;
  clearTimeout(f.socketRetry);f.socketStopped=true;f.socket?.close();
}
function connectAlerts(f){
  if(f!==fleet||f.socketStopped)return;
  const socket=f.socket=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/cab-watch/me/alerts/ws');
  socket.onopen=()=>socket.send(JSON.stringify({token:sessionStorage.getItem('cabwatch.token')}));
  socket.onmessage=event=>{
    if(f!==fleet||f.socket!==socket)return;
    const message=JSON.parse(event.data);
    if(message.type!=='off_route_snapshot')return;
    f.socketStates=Object.fromEntries(message.vehicles.map(v=>[v.trip_id,v]));
    $('alertConnection').textContent='● Alert updates connected · status at captured reading';
    render();
  };
  socket.onclose=event=>{
    if(f!==fleet||f.socketStopped)return;
    f.socketStates=null;
    $('alertConnection').textContent='Alert connection lost · using recorded history · reconnecting';
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
    if(outbox.hasLegacyReadings()){
      const saved=await api('me/trips');
      outbox.upgrade(saved.trips);
    }
    while(outbox.length){
      await api('me/gps','POST',{readings:outbox.first()});
      outbox.acknowledge();
    }
    network=network||await api('network');
    const created=await api('me/trips');
    running=false;clearTimeout(timer);cancelAnimationFrame(frame);reviewGeneration++;selected=null;closePlanner();
    clearTimeout(fleet?.retryTimer);clearTimeout(fleet?.pollTimer);
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
    msg(fleet.cabs.length?'Add a trip or press Start to resume.':'Add a trip to choose a pickup and drop-off.');
    connectAlerts(fleet);pollHistory(fleet);loadHistory(fleet,selected);
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
$('wrong').onclick=()=>{divert(chosen());render();
  msg(running?'Turn queued for the next junction.':'Turn queued for the next junction. Press Start to drive.');};
$('return').onclick=()=>{rejoin(chosen());render();
  msg(running?'Cab is retracing its detour.':'Return requested. Press Start to drive.');};
let registering=false;
function signOut(message=''){
  stopAlertSocket(fleet);
  sessionStorage.removeItem('cabwatch.token');sessionStorage.removeItem('cabwatch.name');
  clearTimeout(fleet?.retryTimer);clearTimeout(fleet?.pollTimer);fleet=null;running=false;selected=null;clearTimeout(timer);cancelAnimationFrame(frame);
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

