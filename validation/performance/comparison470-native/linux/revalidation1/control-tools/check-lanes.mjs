// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Independent arithmetic and assignment checks, not a consensus proof.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

function csv(file, header) {
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
    assert.equal(lines.shift(), header);
    return lines.map(line => {
        const row = line.split(',').map(Number);
        assert.ok(row.every(n => Number.isSafeInteger(n) && n >= 0));
        return row;
    });
}
export function validateLanes(directory, summary, samplesPath) {
    assert.equal(summary.assembly, 'static_lanes');
    assert.ok(['TcpTls', 'Quic'].includes(summary.protocol));
    for (const key of ['retry_verified', 'workers_joined', 'host_threads_joined']) {
        assert.equal(summary[key], 'true');
    }
    const count = Number(summary.operations), lanes = Number(summary.lanes), groups = Number(summary.groups);
    assert.ok(Number.isInteger(lanes) && lanes >= 1 && lanes <= 4);
    assert.ok(Number.isInteger(groups) && groups >= lanes && groups <= 32);
    assert.ok(Number.isInteger(count) && count >= lanes && count <= 50000);
    assert.equal(Number(summary.replicas), 3);
    assert.equal(Number(summary.payload_bytes), 8);
    assert.equal(Number(summary.warmup), 64);
    assert.equal(Number(summary.recovered_value), 64 + count);
    assert.equal(Number(summary.host_owner_threads), lanes);
    for (const key of ['wal_workers', 'snapshot_workers', 'peer_endpoints']) assert.equal(Number(summary[key]), 3 * lanes);
    assert.equal(Number(summary.tcp_dial_workers), summary.protocol === 'TcpTls' ? 3 * lanes : 0);
    const planFile = path.join(directory, 'plan.csv');
    const plans = csv(planFile, 'lane,first_group,groups,warmup,operations,window');
    assert.equal(plans.length, lanes);
    let nextGroup = 1;
    for (let i = 0; i < plans.length; i++) {
        const [lane, first, size, warmup, operations, window] = plans[i];
        assert.equal(plans[i].length, 6);
        assert.equal(lane, i + 1);
        assert.equal(first, nextGroup);
        assert.ok(size >= 1 && warmup >= size && operations >= 1 && window >= 1);
        nextGroup += size;
    }
    assert.equal(nextGroup, groups + 1);
    for (const [column, total] of [[3, 64], [4, count], [5, Number(summary.window)]]) {
        assert.equal(plans.reduce((sum, plan) => sum + plan[column], 0), total);
    }
    const rows = csv(samplesPath, 'lane,group,operation,submitted_ns,completed_ns,latency_ns,applied_index,value');
    assert.equal(rows.length, count);
    const events = [], latencies = [];
    for (const [lane, first, size, warmup, operations, window] of plans) {
        const selected = rows.filter(row => row[0] === lane).sort((a, b) => a[2] - b[2]);
        assert.equal(selected.length, operations);
        const previous = new Map();
        const localEvents = [];
        for (const [i, row] of selected.entries()) {
            assert.equal(row.length, 8);
            const [, group, op, start, end, latency, index, value] = row;
            assert.equal(op, warmup + i + 1);
            assert.equal(group, first + (op - 1) % size);
            assert.equal(value, Math.floor((op - 1) / size) + 1);
            assert.ok(end >= start && index > (previous.get(group) ?? 0));
            assert.equal(latency, end - start);
            previous.set(group, index);
            if (end > start) localEvents.push([start, 1], [end, -1]);
            latencies.push(latency);
        }
        checkWindow(localEvents, window);
        events.push(...localEvents);
    }
    checkWindow(events, Number(summary.window));
    const elapsed = Math.max(...rows.map(row => row[4]));
    assert.equal(Number(summary.elapsed_ns), elapsed);
    assert.ok(Math.abs(Number(summary.elapsed_s) - elapsed / 1e9) <= 0.000000501);
    assert.ok(Math.abs(Number(summary.applied_ops_s) - count * 1e9 / elapsed) <= 0.000501);
    latencies.sort((a, b) => a - b);
    const p99 = latencies[Math.ceil(count * .99) - 1];
    assert.equal(Number(summary.p99_ns), p99);
    assert.ok(Math.abs(Number(summary.p99_us) - p99 / 1000) <= .000501);
    return { lanes, groups, count, elapsed_ns: elapsed, p99_ns: p99, partitions: plans.map(p => p.slice(0, 3)) };
}
function checkWindow(events, bound) {
    assert.ok(Number.isInteger(bound) && bound >= 1 && bound <= 32);
    // A completion at the same instant releases its credit before a new request.
    events.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    let live = 0;
    for (const [, delta] of events) {
        live += delta;
        assert.ok(live >= 0 && live <= bound, 'window exceeded');
    }
    assert.equal(live, 0);
}
