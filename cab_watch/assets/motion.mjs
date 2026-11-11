const distance = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);

export function makeCab(trip, network) {
  const nodes = trip.route.map(node => network.nodes[node]);
  const point = [...nodes[0]];
  const motion = trip.motion;
  return {...trip, nodes, point, mode:motion?.mode ?? 'route', index:motion?.index ?? 0,
    from:motion?.source, to:motion?.target, recovery:motion?.recovery?.slice(), sequence:0,
    offset:0, confirmed:0, network};
}

export function motionSnapshot(cab) {
  switch (cab.mode) {
    case 'route':
    case 'turn requested':
      return {mode:cab.mode, index:cab.index};
    case 'detour':
      return {mode:cab.mode, source:cab.from, target:cab.to};
    case 'returning':
      return {mode:cab.mode, recovery:[...cab.recovery]};
    case 'arrived':
      return {mode:cab.mode};
  }
  throw new Error('Unknown cab movement: ' + cab.mode);
}

function neighbours(network, node) {
  return network.roads.filter(road => road.includes(node)).map(road => road.replace(node, ''));
}

function pick(choices) {
  return choices[Math.floor(Math.random() * choices.length)];
}

function resumeAt(cab, node) {
  cab.index = cab.route.indexOf(node);
  cab.mode = cab.index === cab.route.length - 1 ? 'arrived' : 'route';
}

export function divert(cab) {
  if (cab.mode !== 'route') return;
  if (cab.index + 1 < cab.route.length - 1) cab.mode = 'turn requested';
}

function chooseDetourRoad(cab, node) {
  const index = cab.route.indexOf(node);
  const plannedNext = index < 0 ? undefined : cab.route[index + 1];
  const roads = neighbours(cab.network, node);
  const alternatives = roads.filter(next => next !== plannedNext);
  cab.from = node;
  cab.to = pick(alternatives.length ? alternatives : roads);
  cab.mode = 'detour';
}

// From a junction, minimize the road distance to rejoin plus the route left to drive.
function recoveryPath(cab, start) {
  const {nodes} = cab.network;
  const remaining = new Map();
  let suffix = 0;
  for (let i=cab.route.length-1;i>=0;i--) {
    if (i<cab.route.length-1) suffix += distance(nodes[cab.route[i]],nodes[cab.route[i+1]]);
    remaining.set(cab.route[i],suffix);
  }
  const costs = new Map([[start, 0]]);
  const parent = new Map();
  const open = new Set([start]);
  const settled = new Set();
  let best, bestTotal = Infinity, bestConnector = Infinity;
  const epsilon = 1e-8;
  while (open.size) {
    let node;
    for (const candidate of open) {
      if (node === undefined || costs.get(candidate) < costs.get(node)) node = candidate;
    }
    const connector = costs.get(node);
    if (connector>bestTotal+epsilon) break;
    if (remaining.has(node)) {
      const total = connector+remaining.get(node);
      // For equal full journeys, rejoin sooner rather than staying off-route.
      if (total<bestTotal-epsilon || (Math.abs(total-bestTotal)<=epsilon && connector<bestConnector)) {
        best=node;bestTotal=total;bestConnector=connector;
      }
    }
    open.delete(node);
    settled.add(node);
    for (const next of neighbours(cab.network, node)) {
      if (settled.has(next)) continue;
      const cost = costs.get(node) + distance(nodes[node], nodes[next]);
      if (cost >= (costs.get(next) ?? Infinity)) continue;
      costs.set(next, cost);
      parent.set(next, node);
      open.add(next);
    }
  }
  if (best!==undefined) {
    const path = [best];
    while (parent.has(best)) {
      best=parent.get(best);
      path.unshift(best);
    }
    return path;
  }
  throw new Error('No road leads back to the planned route.');
}

export function rejoin(cab) {
  if (cab.mode === 'turn requested') {
    cab.mode = 'route';
  } else if (cab.mode === 'detour') {
    // At a junction we can choose now. Once on a road, finish it first.
    const atSource = distance(cab.point, cab.network.nodes[cab.from]) < 1e-8;
    cab.recovery = recoveryPath(cab, atSource ? cab.from : cab.to);
    cab.mode = 'returning';
  }
}

export function advance(cab) {
  if (cab.mode === 'arrived') return false;
  const onRoute = cab.mode === 'route' || cab.mode === 'turn requested';
  const next = onRoute ? cab.route[cab.index + 1] :
    cab.mode === 'returning' ? cab.recovery[0] : cab.to;
  const target = cab.network.nodes[next];
  const remaining = distance(cab.point, target);
  if (remaining > 14) {
    cab.point = cab.point.map((v, i) => v + (target[i] - v) * 14 / remaining);
  } else {
    cab.point = [...target];
    if (onRoute) {
      const queued = cab.mode === 'turn requested';
      resumeAt(cab, next);
      if (queued && cab.mode === 'route') chooseDetourRoad(cab, next);
    } else if (cab.mode === 'returning' && cab.recovery.length === 1) {
      resumeAt(cab, next);
    } else if (cab.mode === 'returning') {
      cab.recovery.shift();
    } else {
      chooseDetourRoad(cab, next);
    }
  }
  cab.sequence++;
  return true;
}

