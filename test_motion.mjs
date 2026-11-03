import assert from 'node:assert/strict';
import test from 'node:test';
import {makeCab, divert, advance, rejoin} from './cab_watch/assets/motion.mjs';

// A---B---C---D is the plan. A---E---F---C is a longer side road.
const nodes = {P:[-80,0], A:[0,0], B:[100,0], C:[200,0], D:[300,0], E:[0,100], F:[200,100]};
const network = {nodes, roads:['PA','AB','BC','CD','AE','EF','FC']};
const stops = ['P','A','B','C','D'];
function trip() {
  return makeCab({stops, path:stops.map(n=>nodes[n])}, network);
}
function until(cab, condition) {
  for (let i=0;i<100 && !condition();i++) advance(cab);
  assert.ok(condition());
}

test('a wandering cab passes successive side-road junctions and rejoins at C',()=>{
  const cab=trip();
  advance(cab);
  divert(cab);
  assert.equal(cab.mode,'turn requested');
  const x=cab.point[0]; advance(cab);
  assert.ok(cab.point[0]>x);
  until(cab,()=>cab.mode==='detour');
  assert.deepEqual(cab.point,nodes.A);
  assert.equal(cab.to,'E');
  until(cab,()=>cab.from==='C');
  assert.equal(cab.mode,'detour');
  for(let i=0;i<200;i++)advance(cab);
  assert.equal(cab.mode,'detour');
  rejoin(cab);
  until(cab,()=>cab.mode==='arrived');
});

test('recovery from midway along EF follows roads to the cheaper endpoint',()=>{
  for(const [steps,junction] of [[2,'A'],[12,'C']]){
    const cab=trip();
    divert(cab);
    until(cab,()=>cab.from==='E');
    for(let i=0;i<steps;i++) advance(cab);
    const before=[...cab.point];
    rejoin(cab);
    assert.deepEqual(cab.point,before);
    assert.deepEqual(cab.recovery,junction==='A'?['E','A']:['F','C']);
    until(cab,()=>cab.mode==='route');
    assert.deepEqual(cab.point,nodes[junction]);
  }
});

test('a dispatched car visits pickup, drop-off, and returns to its depot',()=>{
  const legs=[
    {stops:['A','B'],path:[nodes.A,nodes.B]},
    {stops:['B','C'],path:[nodes.B,nodes.C]},
    {stops:['C','B','A'],path:[nodes.C,nodes.B,nodes.A]},
  ];
  const cab=makeCab({legs,stops:['A','B','C','B','A'],path:[nodes.A,nodes.B,nodes.C,nodes.B,nodes.A]},network);
  assert.equal(cab.leg,0);
  until(cab,()=>cab.leg===1);
  assert.deepEqual(cab.point,nodes.B);
  until(cab,()=>cab.leg===2);
  assert.deepEqual(cab.point,nodes.C);
  until(cab,()=>cab.mode==='arrived');
  assert.equal(cab.leg,2);
  assert.deepEqual(cab.point,nodes.A);
});

test('pickup at the depot skips the empty first leg',()=>{
  const legs=[
    {stops:['A'],path:[nodes.A]},
    {stops:['A','B'],path:[nodes.A,nodes.B]},
    {stops:['B','A'],path:[nodes.B,nodes.A]},
  ];
  const cab=makeCab({legs,stops:['A','B','A'],path:[nodes.A,nodes.B,nodes.A]},network);
  assert.equal(cab.leg,1);
  until(cab,()=>cab.mode==='arrived');
  assert.equal(cab.leg,2);
});

test('drop-off at the home depot completes the empty return leg',()=>{
  const legs=[
    {stops:['A','B'],path:[nodes.A,nodes.B]},
    {stops:['B','A'],path:[nodes.B,nodes.A]},
    {stops:['A'],path:[nodes.A]},
  ];
  const cab=makeCab({legs,stops:['A','B','A'],path:[nodes.A,nodes.B,nodes.A]},network);
  until(cab,()=>cab.mode==='arrived');
  assert.equal(cab.leg,2);
  assert.deepEqual(cab.point,nodes.A);
});

