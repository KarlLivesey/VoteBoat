// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import test from 'node:test';
import { classifySerial, verifyContext, P99_BUDGET_US } from './check-serial-gate.mjs';
const reference = {
    protocol: 'TcpTls', assembly: 'startup', leader_placement: '1:1',
    replicas: '3', groups: '1', payload_bytes: '8', warmup: '64', operations: '256',
    window: '1', max_inflight: '1', recovered_value: '320',
    wal_workers_per_replica: '1', snapshot_workers_per_replica: '1', peer_endpoints_per_replica: '1',
    heartbeat_ms: '50', election_min_ms: '1000', election_spread_ms: '1000',
    retry_verified: 'true', workers_joined: 'true',
    elapsed_s: '32', applied_ops_s: '8', p50_us: '100000', p95_us: '200000', p99_us: '250000', max_us: '300000',
};
test('fixed p99 boundary distinguishes equality and actual numeric refusal', () => {
    assert.equal(classifySerial(reference).passed, true);
    assert.equal(classifySerial({ ...reference, p99_us: String(P99_BUDGET_US + 0.001) }).passed, false);
    assert.equal(classifySerial({ ...reference, p99_us: '249999' }).passed, true);
});
test('fast results from a different topology/window/assembly cannot satisfy the reference gate', () => {
    for (const [key, value] of [['protocol','Quic'],['assembly','shared'],['groups','8'],['window','8'],['operations','128'],['warmup','0'],['leader_placement','1:2'],['mode','offered'],['heartbeat_ms','100']]) {
        assert.throws(() => classifySerial({ ...reference, [key]:value }), key);
    }
});
test('incomplete recovery or worker ownership cannot be advertised as performance success', () => {
    for (const [key,value] of [['retry_verified','false'],['workers_joined','false'],['recovered_value','319'],['replicas','1'],['snapshot_workers_per_replica','0']]) {
        assert.throws(() => classifySerial({ ...reference, [key]:value }), key);
    }
});
test('missing, nonfinite, unordered and inconsistent timings are refused', () => {
    for (const [key,value] of [['p99_us','NaN'],['p99_us','Infinity'],['p99_us',''],['p99_us','-1'],['p99_us','199999'],['max_us','249999'],['elapsed_s','0'],['applied_ops_s','100']]) {
        assert.throws(() => classifySerial({ ...reference, [key]:value }), key);
    }
    const absent={...reference};delete absent.p99_us;assert.throws(() => classifySerial(absent));
});
test('volatile/missing/mismatched reference provenance cannot release a numeric success', () => {
    const hash = 'a'.repeat(64);
    const context = { schema:1, mode:'disk_reference', filesystem:'btrfs', root:'/captured/reference',
        source_revision:'b'.repeat(40), executable_sha256:hash, summary_sha256:hash, samples_sha256:hash };
    verifyContext(context, hash, hash);
    for (const [key,value] of [['filesystem','tmpfs'],['filesystem','ramfs'],['filesystem',''],['mode','smoke'],['root','relative'],['source_revision','unknown'],['executable_sha256',''],['summary_sha256','b'.repeat(64)],['samples_sha256','b'.repeat(64)]]) {
        assert.throws(() => verifyContext({...context,[key]:value},hash,hash),key);
    }
});

test('public context preserves workspace-relative root without local path disclosure', () => {
    const hash = 'a'.repeat(64);
    const context = {schema:1,mode:'disk_reference',filesystem:'btrfs',
        root_path_kind:'workspace_relative',root:'target/bench-slice136-tcp-serial-b',
        source_revision:'b'.repeat(40),executable_sha256:hash,summary_sha256:hash,samples_sha256:hash};
    verifyContext(context,hash,hash);
    for(const root of ['../target/bench-a','target/bench-../secret','/target/bench-a','target/other']) {
        assert.throws(()=>verifyContext({...context,root},hash,hash));
    }
    assert.throws(()=>verifyContext({...context,root_path_kind:'unknown'},hash,hash));
});
