// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Independent raw-result arithmetic and workload checks; not a Raft proof.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

assert.ok(process.argv.length > 2, 'pass one or more successful run directories');
for (const directory of process.argv.slice(2)) {
    const runDirectory = fs.existsSync(path.join(directory, 'summary.txt'));
    const summaryPath = runDirectory ? path.join(directory, 'summary.txt') : `${directory}.txt`;
    const samplesPath = runDirectory ? path.join(directory, 'samples.csv') : `${directory}.csv`;
    const summary = Object.fromEntries(fs.readFileSync(summaryPath, 'utf8')
        .trim().split(/\s+/).map(token => token.split('=')));
    const count = Number(summary.operations);
    const groups = Number(summary.groups);
    const window = Number(summary.window);
    const warmup = Number(summary.warmup);
    assert.ok(Number.isSafeInteger(count) && count > 0);
    assert.ok(Number.isSafeInteger(groups) && groups > 0 && groups <= 32);
    assert.ok(Number.isSafeInteger(window) && window > 0 && window <= 32);
    assert.equal(summary.retry_verified, 'true');
    assert.equal(summary.workers_joined, 'true');
    assert.equal(Number(summary.recovered_value), warmup + count);
    assert.equal(Number(summary.replicas), 3);
    assert.equal(Number(summary.payload_bytes), 8);
    for (const key of ['wal_workers_per_replica', 'snapshot_workers_per_replica', 'peer_endpoints_per_replica']) {
        assert.equal(Number(summary[key]), 1);
    }
    const lines = fs.readFileSync(samplesPath, 'utf8').trim().split('\n');
    assert.equal(lines.shift(), 'operation,submitted_ns,completed_ns,latency_ns,applied_index,value,group');
    assert.equal(lines.length, count);
    const rows = lines.map(line => {
        const row = line.split(',').map(Number);
        assert.equal(row.length, 7);
        assert.ok(row.every(Number.isSafeInteger));
        const [op, start, end, latency, index, value, group] = row;
        assert.ok(start >= 0 && end >= start && index > 0);
        assert.equal(latency, end - start);
        assert.equal(group, (op - 1) % groups + 1);
        assert.equal(value, Math.floor((op - 1) / groups) + 1);
        assert.ok(end <= Number(summary.elapsed_s) * 1e9 + 1000);
        return row;
    });
    rows.sort((a, b) => a[0] - b[0]);
    const previous = new Map();
    for (const [i, row] of rows.entries()) {
        assert.equal(row[0], warmup + i + 1);
        assert.ok(row[4] > (previous.get(row[6]) ?? 0), 'per-group log positions must increase');
        previous.set(row[6], row[4]);
    }
    const events = rows.flatMap(row => [[row[1], 1, row[6]], [row[2], -1, row[6]]]);
    events.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    let inflight = 0;
    let maxInflight = 0;
    const perGroup = new Map();
    for (const [, change, group] of events) {
        inflight += change;
        const local = (perGroup.get(group) ?? 0) + change;
        perGroup.set(group, local);
        assert.ok(inflight >= 0 && inflight <= window);
        assert.ok(local >= 0 && local <= Math.ceil(window / groups));
        maxInflight = Math.max(maxInflight, inflight);
    }
    assert.equal(inflight, 0);
    assert.equal(maxInflight, Number(summary.max_inflight));
    const latencies = rows.map(row => row[3]).sort((a, b) => a - b);
    for (const percentile of [50, 95, 99]) {
        const actual = latencies[Math.ceil(count * percentile / 100) - 1] / 1000;
        assert.ok(Math.abs(actual - Number(summary[`p${percentile}_us`])) <= 0.00051);
    }
    assert.ok(Math.abs(latencies.at(-1) / 1000 - Number(summary.max_us)) <= 0.00051);
    assert.ok(Math.abs(count / Number(summary.elapsed_s) - Number(summary.applied_ops_s)) < 0.001);
    assert.ok(Number(summary.max_host_poll_ms) <= Number(summary.host_poll_ms));
    assert.ok(Number(summary.host_poll_ms) <= Number(summary.elapsed_s) * 1000);
    console.log(`${directory}: ${count} receipts, group routing/history, windows and latency/rate arithmetic pass`);
}
