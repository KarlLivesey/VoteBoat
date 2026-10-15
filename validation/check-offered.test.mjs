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

const phase='absolute_operation_minus_one';
function weightedFixture() {
    // Absolute operations65..70 start at cycle slots4,5,0,1,2,3 for weights4,1,1.
    const groups=[2,3,1,1,1,1], values=[22,22,23,24,25,26];
    const rows=groups.map((group,i)=>{
        const intended=Math.floor(i*1e9/6);
        return `${65+i},${group},${intended},${intended},${intended+1},1,${i+1},applied,,${100+i},${values[i]}`;
    });
    const summary={...metadata,offers:6,groups:3,window:3,offered_rate:6,
        workload_group_weights:'4,1,1',workload_group_phase:phase,
        applied:6,window_refused:0,admitted:6,recovered_value:70,
        applied_during:6,admitted_during:6,p99_us:0.001,service_p99_us:0.001,
        offered_ops_s:6,admitted_ops_s:6,applied_during_ops_s:6,applied_total_ops_s:6};
    return {rows,summary};
}
test('explicit all-one weights retain historical uniform phase and validation',()=>{
    check(original,{...metadata,workload_group_weights:'1',workload_group_phase:phase});
});
test('weighted rows retain absolute phase and uniform warmup application history',()=>{
    const {rows,summary}=weightedFixture();check(rows,summary);
});
test('reject altered weighted phase, mapping and applied history',()=>{
    const {rows,summary}=weightedFixture();
    assert.throws(()=>check(rows,{...summary,workload_group_phase:'measured_offset'}));
    assert.throws(()=>check(rows,{...summary,workload_group_weights:'1,1,1'}));
    assert.throws(()=>check([rows[0].replace('65,2,','65,1,'),...rows.slice(1)],summary));
    assert.throws(()=>check([rows[0].replace('100,22','100,23'),...rows.slice(1)],summary));
});
test('reject invalid or undisclosed workload schedule metadata',()=>{
    const {rows,summary}=weightedFixture();
    for(const weights of ['0,1,1','1,1','255,1,1','04,1,1','4,-1,1','4,1.5,1']) {
        assert.throws(()=>check(rows,{...summary,workload_group_weights:weights}));
    }
    assert.throws(()=>check(original,{...metadata,workload_group_phase:phase}));
});
test('reject a hot-group retained bound that the old uniform ceiling would admit',()=>{
    assert.ok(Math.ceil((20000+64)/2)+64<16384);
    assert.throws(()=>check(original,{...metadata,offers:20000,groups:2,
        workload_group_weights:'255,1',workload_group_phase:phase}),/weighted workload exceeds/);
});

const capacity4096={offered_capacity_requested:4096,offered_capacity_effective:4096,
    client_requests:4096,client_group_requests:4096,client_bytes:33554432,client_receipt_bytes:1048576,
    shard_max_items:8192,shard_control_items:1024,shard_group_items:4112,
    shard_group_control_items:16,shard_group_bytes:4194304};
test('wide windows require the complete explicit profile without changing outcome conservation',()=>{
    const {rows,summary}=weightedFixture();
    for(const window of [32,256,4096]) check(rows,{...summary,...capacity4096,window});
    assert.throws(()=>check(rows,{...summary,window:256}));
    assert.throws(()=>check(rows,{...summary,...capacity4096,window:4097}));
    for(const key of Object.keys(capacity4096)) {
        const bad={...summary,...capacity4096,window:256};delete bad[key];
        assert.throws(()=>check(rows,bad),key);
        assert.throws(()=>check(rows,{...summary,...capacity4096,window:256,[key]:1}),key);
    }
    assert.throws(()=>check(rows,{...summary,...capacity4096,window:256,recovered_value:71}));
    assert.throws(()=>check(rows,{...summary,...capacity4096,window:256,max_inflight:4096}));
});
