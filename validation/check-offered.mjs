// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Scheduled benchmark arithmetic only; no consensus or sustainable-capacity proof.
import assert from 'node:assert/strict';
import fs from 'node:fs';

export function validateOffered(label, summary, source) {
    const n = key => Number(summary[key]);
    const count = n('offers'), groups = n('groups'), window = n('window'), warmup = n('warmup');
    const rate = n('offered_rate'), horizon = n('horizon_ns'), elapsed = n('elapsed_ns');
    assert.ok(Number.isSafeInteger(count) && count >= 1 && count <= 50000);
    assert.ok(Number.isSafeInteger(rate) && rate >= 1 && rate <= 100000);
    assert.ok(Number.isSafeInteger(groups) && groups >= 1 && groups <= 32);
    assert.ok(Number.isSafeInteger(window) && window >= 1 && window <= 32);
    assert.equal(warmup, 64);
    assert.equal(horizon, Math.floor(count * 1e9 / rate));
    assert.ok(horizon > 0 && horizon <= 300e9 && Number.isSafeInteger(elapsed) && elapsed >= horizon);
    assert.equal(n('drain_ns'), elapsed - horizon);
    assert.equal(summary.assembly, 'shared');
    assert.equal(summary.retry_verified, 'true'); assert.equal(summary.workers_joined, 'true');
    assert.equal(n('replicas'), 3); assert.equal(n('payload_bytes'), 8);
    for (const key of ['wal_workers_per_replica','snapshot_workers_per_replica','peer_endpoints_per_replica']) assert.equal(n(key), 1);
    assert.equal(n('heartbeat_ms'),50); assert.equal(n('election_min_ms'),10000); assert.equal(n('election_spread_ms'),10000);
    assert.equal(n('unknown'),0); assert.equal(n('pending'),0);
    const lines = fs.readFileSync(source,'utf8').trim().split('\n');
    const fields = 'operation,group,intended_ns,dispatched_ns,completed_ns,replica,sequence,status,reason,applied_index,value'.split(',');
    assert.equal(lines.shift(),fields.join(',')); assert.equal(lines.length,count);
    const statuses = new Map(['applied','not_proposed','window_refused','no_leader','admission_refused'].map(s=>[s,0]));
    const tickets = new Set();
    const rows = lines.map((line,i)=>{
        const cells=line.split(','); assert.equal(cells.length,fields.length);
        const r=Object.fromEntries(fields.map((key,j)=>[key,cells[j]]));
        for(const key of fields.filter(k=>!['status','reason'].includes(k))) {
            r[key]=Number(r[key]); assert.ok(Number.isSafeInteger(r[key]) && r[key]>=0,key);
        }
        assert.equal(r.operation,warmup+i+1); assert.equal(r.group,(r.operation-1)%groups+1);
        assert.equal(r.intended_ns,Math.floor(i*1e9/rate));
        assert.ok(r.dispatched_ns>=r.intended_ns && r.completed_ns>=r.dispatched_ns && r.completed_ns<=elapsed);
        assert.ok(statuses.has(r.status)); statuses.set(r.status,statuses.get(r.status)+1);
        if(['applied','not_proposed'].includes(r.status)) {
            assert.ok(r.replica>=1 && r.replica<=3 && r.sequence>0);
            const key=`${r.replica}:${r.sequence}`;assert.ok(!tickets.has(key));tickets.add(key);
        } else { assert.equal(r.replica,0);assert.equal(r.sequence,0); }
        if(r.status==='applied') { assert.ok(r.applied_index>0 && r.value>0); }
        else { assert.equal(r.applied_index,0);assert.equal(r.value,0); }
        return r;
    });
    for(let i=1;i<rows.length;i++) assert.ok(rows[i].dispatched_ns>=rows[i-1].dispatched_ns);
    for(const [status,total] of statuses) assert.equal(n(status),total,status);
    const applied=rows.filter(r=>r.status==='applied'), admitted=rows.filter(r=>r.sequence>0);
    assert.equal(n('admitted'),admitted.length);
    assert.equal(n('admitted'),n('applied')+n('not_proposed'));
    assert.equal(n('recovered_value'),warmup+applied.length);
    const during=applied.filter(r=>r.completed_ns<horizon).length;
    assert.equal(n('applied_during'),during);assert.equal(n('applied_drain'),applied.length-during);
    assert.equal(n('admitted_during'),admitted.filter(r=>r.dispatched_ns<horizon).length);
    assert.equal(n('late_decisions'),rows.filter(r=>r.dispatched_ns>=horizon).length);
    assert.equal(n('backlog_at_horizon'),admitted.filter(r=>r.dispatched_ns<horizon && r.completed_ns>=horizon).length);
    let live=0,maxLive=0;const perGroup=new Map();
    const events=rows.flatMap((r,i)=>[
        [r.dispatched_ns,0,i,r],...(r.sequence>0?[[r.completed_ns,-1,i,r]]:[])
    ]).sort((a,b)=>a[0]-b[0]||a[1]-b[1]||a[2]-b[2]);
    for(const [,kind,,r] of events) {
        const before=perGroup.get(r.group)??0;
        if(kind===0 && r.status==='window_refused') {
            assert.ok(live>=window || before>=Math.ceil(window/groups),'window refusal must reflect actual retained load');
            continue;
        }
        if(kind===0 && r.sequence===0) continue;
        const change=kind===-1?-1:1;
        live+=change;const local=before+change;perGroup.set(r.group,local);
        assert.ok(live>=0 && live<=window);assert.ok(local>=0 && local<=Math.ceil(window/groups));maxLive=Math.max(maxLive,live);
    }
    assert.equal(live,0);assert.equal(n('max_inflight'),maxLive);
    for(let g=1;g<=groups;g++) {
        let value=Math.floor((warmup-g)/groups)+1, index=0;
        for(const r of applied.filter(r=>r.group===g).sort((a,b)=>a.applied_index-b.applied_index)) {
            assert.ok(r.applied_index>index);index=r.applied_index;assert.equal(r.value,++value);
        }
    }
    const percentile=(values,key)=>{
        if(!values.length){assert.equal(summary[key],'NA');return;}
        values.sort((a,b)=>a-b);const actual=values[Math.ceil(values.length*0.99)-1]/1000;
        assert.ok(Math.abs(actual-n(key))<=0.00051,key);
    };
    percentile(applied.map(r=>r.completed_ns-r.intended_ns),'p99_us');
    percentile(applied.map(r=>r.completed_ns-r.dispatched_ns),'service_p99_us');
    percentile(rows.map(r=>r.dispatched_ns-r.intended_ns),'dispatch_p99_us');
    for(const [key,actual] of [
        ['offered_ops_s',count*1e9/horizon],['admitted_ops_s',admitted.length*1e9/elapsed],
        ['applied_during_ops_s',during*1e9/horizon],['applied_total_ops_s',applied.length*1e9/elapsed],
    ]) assert.ok(Math.abs(n(key)-actual)<=0.00051,key);
    assert.ok(n('max_host_poll_ms')<=n('host_poll_ms') && n('host_poll_ms')<=elapsed/1e6);
    console.log(`${label}: ${count} scheduled offers, ${admitted.length} admissions, ${applied.length} applied; outcome/history/window/horizon/drain/latency/rate arithmetic pass`);
    const drainRate=n('drain_ns')>0?(n('applied_drain')*1e9/n('drain_ns')).toFixed(3):'NA';
    console.log(`load rates: offered=${summary.offered_ops_s} admitted_during=${(n('admitted_during')*1e9/horizon).toFixed(3)} applied_during=${summary.applied_during_ops_s} applied_drain=${drainRate} applied_total=${summary.applied_total_ops_s} ops/s; window_refused=${summary.window_refused} backlog_at_horizon=${summary.backlog_at_horizon}`);
}
