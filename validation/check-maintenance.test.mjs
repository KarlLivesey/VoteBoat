// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {validateMaintenance} from './check-maintenance.mjs';
const header='wave,kind,group,replica,intended_ns,submitted_ns,completed_ns,status,previous_base,requested_index,observed_base,snapshot_generation,ticket_sequence,before_bytes,after_bytes,reason';
const rows=[
 '1,checkpoint,1,1,1000000000,1100000000,1200000000,completed,5,8,9,1,0,0,0,',
 ...[1,2,3].map(r=>`1,reclaim,1,${r},1000000000,1200000000,1700000000,completed,0,0,0,0,1,100,50,`),
 '2,skip,1,1,2000000000,3000000000,3000000000,skipped,0,0,0,0,0,0,0,late',
];
const metadata={horizon_ns:3e9,elapsed_ns:3.1e9,maintenance_period_ns:1e9,groups:1,checkpoints:1,reclaims:3,maintenance_skips:1,reclaimed_bytes:150,pause_start_ns:0,pause_end_ns:0,actual_pause_ns:0,actual_resume_ns:0,forced_index:0};
const originalBases='stage,replica,group,base_index\nbefore_close,1,1,9\nbefore_close,2,1,0\nbefore_close,3,1,0\nafter_recover,1,1,9\nafter_recover,2,1,0\nafter_recover,3,1,0\n';
function check(rr=rows,summary=metadata,bases=originalBases) {
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'voteboat-maintenance-check-'));
 try {
  fs.writeFileSync(path.join(root,'maintenance.csv'),[header,...rr].join('\n')+'\n');fs.writeFileSync(path.join(root,'bases.csv'),bases);
  validateMaintenance('fixture',summary,path.join(root,'maintenance.csv'),path.join(root,'bases.csv'));
 } finally {fs.rmSync(root,{recursive:true});}
}
test('durable boundary, three reclaimed files and explicit missed opportunity reconcile',()=>check());
test('reject fabricated completed checkpoint below requested boundary or missing reclamation',()=>{
 assert.throws(()=>check([rows[0].replace('5,8,9','5,8,7'),...rows.slice(1)]));
 assert.throws(()=>check(rows.filter((_,i)=>i!==2)));
});
test('reject unfinished work and regressed recovered snapshot base',()=>{
 assert.throws(()=>check([rows[0].replace('completed','pending'),...rows.slice(1)]));
 assert.throws(()=>check(rows,metadata,originalBases.replace('after_recover,1,1,9','after_recover,1,1,8')));
});
test('catch-up needs a post-pause boundary, new snapshot install and recovered follower base',()=>{
 const rr=[
  '0,pause,1,3,1000000000,1000100000,1000100000,completed,0,0,0,0,0,0,0,',
  ...rows.slice(0,4),
  '0,resume,1,3,2000000000,2000100000,2000100000,completed,0,0,0,0,0,0,0,',rows[4],
 ];
 const s={...metadata,pause_start_ns:1e9,pause_end_ns:2e9,actual_pause_ns:1000100000,actual_resume_ns:2000100000,forced_group:1,forced_index:9,forced_prior_last:7,forced_leader:1,forced_term:1,follower_snapshot_installs:1,installs_at_resume:0};
 const bases=originalBases.replace('before_close,3,1,0','before_close,3,1,9').replace('after_recover,3,1,0','after_recover,3,1,9');
 check(rr,s,bases);
 assert.throws(()=>check(rr,{...s,installs_at_resume:1},bases));
 assert.throws(()=>check(rr.map(r=>r.replaceAll('2000100000','2000000000')),{...s,actual_resume_ns:2000000000},bases));
 assert.throws(()=>check(rr,{...s,forced_prior_last:9},bases));
 assert.throws(()=>check(rr,s,originalBases));
});
