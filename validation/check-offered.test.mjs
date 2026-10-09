// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { validateOffered } from './check-offered.mjs';
const header='operation,group,intended_ns,dispatched_ns,completed_ns,replica,sequence,status,reason,applied_index,value';
const original=[
    '65,1,0,0,750000000,1,1,applied,,70,65',
    '66,1,500000000,500000000,500000000,0,0,window_refused,,0,0',
];
const metadata={
    offers:2,groups:1,window:1,warmup:64,offered_rate:2,horizon_ns:1e9,elapsed_ns:1e9,drain_ns:0,
    assembly:'shared',retry_verified:'true',workers_joined:'true',replicas:3,payload_bytes:8,
    wal_workers_per_replica:1,snapshot_workers_per_replica:1,peer_endpoints_per_replica:1,
    heartbeat_ms:50,election_min_ms:10000,election_spread_ms:10000,unknown:0,pending:0,
    applied:1,not_proposed:0,window_refused:1,no_leader:0,admission_refused:0,admitted:1,
    recovered_value:65,applied_during:1,applied_drain:0,admitted_during:1,late_decisions:0,
    backlog_at_horizon:0,max_inflight:1,p99_us:750000,service_p99_us:750000,dispatch_p99_us:0,
    offered_ops_s:2,admitted_ops_s:1,applied_during_ops_s:1,applied_total_ops_s:1,
    max_host_poll_ms:0,host_poll_ms:0,
};
function check(rows=original,summary=metadata) {
    const root=fs.mkdtempSync(path.join(os.tmpdir(),'voteboat-offered-check-'));
    try {
        const file=path.join(root,'offers.csv');fs.writeFileSync(file,[header,...rows].join('\n')+'\n');
        validateOffered('fixture',summary,file);
    } finally {fs.rmSync(root,{recursive:true});}
}
test('one actual retained request explains a refused scheduled offer',()=>check());
test('reject coordinated omission from shifted intended start',()=>{
    const rows=[original[0],original[1].replace('500000000,500000000,500000000','600000000,600000000,600000000')];
    assert.throws(()=>check(rows));
});
test('reject phantom window refusal after earlier completion',()=>{
    const rows=[original[0].replace('750000000','250000000'),original[1]];
    assert.throws(()=>check(rows),/window refusal/);
});
test('reject falsely certified unknown outcome',()=>{
    assert.throws(()=>check([original[0].replace('applied','unknown'),original[1]]));
});
test('reject unaccounted applied value and altered pending maximum',()=>{
    assert.throws(()=>check([original[0].replace('70,65','70,66'),original[1]]));
    assert.throws(()=>check(original,{...metadata,max_inflight:2}));
});
