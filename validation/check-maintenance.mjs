// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Independent maintenance arithmetic/structure only; not protocol authority.
import assert from 'node:assert/strict';
import fs from 'node:fs';
export function validateMaintenance(label,summary,maintenanceSource,basesSource) {
    const n=k=>Number(summary[k]), horizon=n('horizon_ns'), elapsed=n('elapsed_ns'), period=n('maintenance_period_ns');
    assert.ok(Number.isSafeInteger(period) && period>=1e9 && period<=60e9 && period<horizon);
    const opportunities=Math.floor((horizon-1)/period);
    const fields='wave,kind,group,replica,intended_ns,submitted_ns,completed_ns,status,previous_base,requested_index,observed_base,snapshot_generation,ticket_sequence,before_bytes,after_bytes,reason'.split(',');
    const lines=fs.readFileSync(maintenanceSource,'utf8').trim().split('\n');assert.equal(lines.shift(),fields.join(','));
    assert.ok(lines.length<=opportunities*4+2);
    const rows=lines.map(line=>{
        const cells=line.split(',');assert.equal(cells.length,fields.length);
        const r=Object.fromEntries(fields.map((k,i)=>[k,cells[i]]));
        for(const k of fields.filter(k=>!['kind','status','reason'].includes(k))) {r[k]=Number(r[k]);assert.ok(Number.isSafeInteger(r[k]) && r[k]>=0,k);}
        assert.ok(r.replica>=1 && r.replica<=3 && r.group>=1 && r.group<=n('groups'));
        assert.ok(r.intended_ns<=r.submitted_ns && r.submitted_ns<=r.completed_ns && r.completed_ns<=elapsed);
        assert.ok(['checkpoint','reclaim','skip','pause','resume'].includes(r.kind));
        assert.equal(r.status,r.kind==='skip'?'skipped':'completed');
        if(['pause','resume'].includes(r.kind)) {assert.equal(r.wave,0);assert.equal(r.replica,3);}
        else {assert.ok(r.wave>=1 && r.wave<=opportunities);assert.equal(r.intended_ns,r.wave*period);}
        return r;
    });
    const checkpoints=rows.filter(r=>r.kind==='checkpoint'),reclaims=rows.filter(r=>r.kind==='reclaim'),skips=rows.filter(r=>r.kind==='skip');
    assert.ok(checkpoints.length>0);assert.equal(n('checkpoints'),checkpoints.length);assert.equal(n('reclaims'),reclaims.length);assert.equal(n('maintenance_skips'),skips.length);
    assert.equal(reclaims.length,3*checkpoints.length);assert.equal(n('reclaimed_bytes'),reclaims.reduce((sum,r)=>sum+r.before_bytes-r.after_bytes,0));
    const sequences=new Set();let previousEnd=0;
    for(let wave=1;wave<=opportunities;wave++) {
        const items=rows.filter(r=>r.wave===wave),c=items.filter(r=>r.kind==='checkpoint'),s=items.filter(r=>r.kind==='skip');
        assert.equal(c.length+s.length,1,'one maintenance decision per opportunity');
        if(s.length) {
            assert.equal(items.length,1);
            assert.ok(['busy','late','awaiting_new_prefix','no_new_applied_prefix'].includes(s[0].reason));
            if(s[0].reason==='busy') assert.ok(s[0].submitted_ns<previousEnd);
            if(s[0].reason==='late') assert.ok(s[0].submitted_ns>=horizon);
            continue;
        }
        const cp=c[0];assert.equal(items.length,4);assert.ok(cp.submitted_ns<horizon && cp.submitted_ns>=previousEnd);
        assert.ok(cp.requested_index>cp.previous_base && cp.observed_base>=cp.requested_index && cp.snapshot_generation>0);
        assert.equal(cp.ticket_sequence,0);assert.equal(cp.before_bytes,0);assert.equal(cp.after_bytes,0);
        const rr=items.filter(r=>r.kind==='reclaim');assert.deepEqual(rr.map(r=>r.replica).sort(),[1,2,3]);
        for(const r of rr) {
            assert.equal(r.group,cp.group);assert.ok(r.submitted_ns>=cp.completed_ns && r.ticket_sequence>0);
            const key=`${r.replica}:${r.ticket_sequence}`;assert.ok(!sequences.has(key));sequences.add(key);
        }
        previousEnd=Math.max(...rr.map(r=>r.completed_ns));
    }
    const baseLines=fs.readFileSync(basesSource,'utf8').trim().split('\n');assert.equal(baseLines.shift(),'stage,replica,group,base_index');
    assert.equal(baseLines.length,n('groups')*3*2);const bases=new Map();
    for(const line of baseLines) {
        const [stage,replica,group,base]=line.split(','),r=Number(replica),g=Number(group),b=Number(base);
        assert.ok(['before_close','after_recover'].includes(stage));assert.ok([1,2,3].includes(r));assert.ok(Number.isSafeInteger(g)&&g>=1&&g<=n('groups'));assert.ok(Number.isSafeInteger(b)&&b>=0);
        const key=`${stage}:${r}:${g}`;assert.ok(!bases.has(key));bases.set(key,b);
    }
    for(let r=1;r<=3;r++)for(let g=1;g<=n('groups');g++)assert.ok(bases.get(`after_recover:${r}:${g}`)>=bases.get(`before_close:${r}:${g}`));
    for(const cp of checkpoints)assert.ok(bases.get(`before_close:${cp.replica}:${cp.group}`)>=cp.observed_base);
    const pauses=rows.filter(r=>r.kind==='pause'),resumes=rows.filter(r=>r.kind==='resume');
    if(n('pause_start_ns')>0) {
        assert.equal(pauses.length,1);assert.equal(resumes.length,1);
        assert.equal(pauses[0].intended_ns,n('pause_start_ns'));assert.equal(resumes[0].intended_ns,n('pause_end_ns'));
        assert.equal(pauses[0].submitted_ns,n('actual_pause_ns'));assert.equal(resumes[0].submitted_ns,n('actual_resume_ns'));
        assert.ok(n('actual_pause_ns')<n('pause_end_ns') && n('actual_resume_ns')-n('actual_pause_ns')>=n('pause_end_ns')-n('pause_start_ns') && n('actual_resume_ns')<horizon);
        assert.ok(n('follower_snapshot_installs')>n('installs_at_resume') && n('forced_term')>0 && n('forced_leader')>=1 && n('forced_leader')<=2);
        const candidates=checkpoints.filter(cp=>cp.group===n('forced_group') && cp.replica===n('forced_leader') && cp.observed_base===n('forced_index') && cp.requested_index>n('forced_prior_last') && cp.submitted_ns>=n('actual_pause_ns') && cp.completed_ns<n('actual_resume_ns'));
        assert.equal(candidates.length,1,'forced checkpoint must exceed pre-pause accepted prefix while follower is paused');
        assert.ok(bases.get(`before_close:3:${n('forced_group')}`)>=n('forced_index'));
    } else {
        assert.equal(pauses.length,0);assert.equal(resumes.length,0);assert.equal(n('actual_pause_ns'),0);assert.equal(n('actual_resume_ns'),0);assert.equal(n('forced_index'),0);
    }
    console.log(`${label}: ${checkpoints.length} durable checkpoint boundaries, ${reclaims.length} exact-scope reclamation rows, ${skips.length} explicit skips; maintenance schedule/bytes/recovery-base and optional catch-up arithmetic pass`);
    const latency=items=>items.reduce((sum,r)=>sum+r.completed_ns-r.submitted_ns,0)/1e6;
    const pendingAtHorizon=rows.filter(r=>['checkpoint','reclaim'].includes(r.kind)&&r.submitted_ns<horizon&&r.completed_ns>=horizon).length;
    console.log(`maintenance observed latency sums: checkpoint_ms=${latency(checkpoints).toFixed(3)} reclaim_ms=${latency(reclaims).toFixed(3)} reclaimed_bytes=${summary.reclaimed_bytes} pending_at_horizon=${pendingAtHorizon}; concurrent/queued durations are not a critical path`);
}
