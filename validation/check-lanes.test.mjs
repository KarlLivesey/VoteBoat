// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { validateLanes } from './check-lanes.mjs';

function fixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'voteboat-lane-check-'));
    fs.writeFileSync(path.join(root, 'plan.csv'), 'lane,first_group,groups,warmup,operations,window\n1,1,2,32,2,2\n2,3,2,32,2,2\n');
    fs.writeFileSync(path.join(root, 'samples.csv'), 'lane,group,operation,submitted_ns,completed_ns,latency_ns,applied_index,value\n1,1,33,10,20,10,30,17\n1,2,34,11,21,10,30,17\n2,3,33,11,23,12,30,17\n2,4,34,12,24,12,30,17\n');
    const summary = { assembly: 'static_lanes', protocol: 'TcpTls', retry_verified: 'true', workers_joined: 'true', host_threads_joined: 'true', operations: 4, lanes: 2, groups: 4, replicas: 3, payload_bytes: 8, warmup: 64, recovered_value: 68, host_owner_threads: 2, wal_workers: 6, snapshot_workers: 6, peer_endpoints: 6, tcp_dial_workers: 6, window: 4, elapsed_ns: 24, elapsed_s: 0.000000024, applied_ops_s: 4e9 / 24, p99_ns: 12, p99_us: .012 };
    return {root, summary, validate: () => validateLanes(root, summary, path.join(root, 'samples.csv'))};
}
function check(mutator, succeeds = false) {
    const fixtureData = fixture();
    try {
        mutator(fixtureData);
        if (succeeds) assert.equal(fixtureData.validate().count, 4);
        else assert.throws(fixtureData.validate);
    } finally { fs.rmSync(fixtureData.root, { recursive: true }); }
}
test('independent lane totals, assignment, windows and timing', () => check(() => {}, true));
test('overlapping groups rejected', () => check(({root}) => {
    const file = path.join(root, 'plan.csv'); fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('2,3,2,', '2,2,2,'));
}));
test('dropped and duplicate operations rejected', () => check(({root}) => {
    const file = path.join(root, 'samples.csv'); fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('2,4,34,', '2,4,33,'));
}));
test('inflated throughput rejected', () => check(({summary}) => { summary.applied_ops_s *= 2; }));
test('hidden global window multiplication rejected', () => check(({summary, root}) => {
    summary.window = 2;
    const file = path.join(root, 'plan.csv'); fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replaceAll(',32,2,2', ',32,2,1'));
}));
test('false recovery evidence rejected', () => check(({summary}) => { summary.recovered_value++; }));
test('incorrect p99 rejected', () => check(({summary}) => { summary.p99_ns++; }));
