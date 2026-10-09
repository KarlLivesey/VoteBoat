// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Independent raw-result arithmetic and workload checks; not a Raft proof.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { validateOffered } from './check-offered.mjs';

const requireStorage = process.argv.includes('--storage');
const directories = process.argv.slice(2).filter(arg => arg !== '--storage');
assert.ok(directories.length > 0, 'pass one or more successful run directories or archived prefixes');
for (const directory of directories) {
    const runDirectory = fs.existsSync(path.join(directory, 'summary.txt'));
    const summaryPath = runDirectory ? path.join(directory, 'summary.txt') : `${directory}.txt`;
    const samplesPath = runDirectory ? path.join(directory, 'samples.csv') : `${directory}.csv`;
    const summary = Object.fromEntries(fs.readFileSync(summaryPath, 'utf8')
        .trim().split(/\s+/).map(token => token.split('=')));
    const groups = Number(summary.groups);
    if (summary.mode === 'offered') {
        const offers = runDirectory ? path.join(directory, 'offers.csv') : `${directory}.offers.csv`;
        validateOffered(directory, summary, offers);
    } else {
        const count = Number(summary.operations);
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
    const storagePath = runDirectory ? path.join(directory, 'storage.csv') : `${directory}.storage.csv`;
    if (requireStorage || fs.existsSync(storagePath)) {
        const storageLines = fs.readFileSync(storagePath, 'utf8').trim().split('\n');
        const fields = storageLines.shift().split(',');
        const expectedFields = 'stage,replica,append_calls,append_units,append_commands,append_ns,append_max_ns,barrier_calls,barrier_tickets,barrier_ns,barrier_max_ns,errors,encoded_bytes,io_append_calls,io_append_ns,sync_calls,sync_ns,publish_calls,publish_ns,batch_histogram,group_units'.split(',');
        assert.deepEqual(fields, expectedFields);
        assert.equal(storageLines.length, 12);
        const stages = ['before_measurement', 'after_measurement', 'after_create_join', 'after_recover_join'];
        const snapshots = new Map();
        const numericFields = fields.slice(2, -2);
        const parseCounts = value => new Map(value.split('|').filter(Boolean).map(pair => {
            const [key, count] = pair.split(':').map(Number);
            assert.ok(Number.isSafeInteger(key) && Number.isSafeInteger(count) && count > 0);
            return [key, count];
        }));
        for (const line of storageLines) {
            const values = line.split(',');
            assert.equal(values.length, fields.length);
            const row = Object.fromEntries(fields.map((key, i) => [key, values[i]]));
            assert.ok(stages.includes(row.stage));
            const replica = Number(row.replica);
            assert.ok([1, 2, 3].includes(replica));
            const key = `${row.stage}:${replica}`;
            assert.ok(!snapshots.has(key));
            for (const field of numericFields) {
                row[field] = Number(row[field]);
                assert.ok(Number.isSafeInteger(row[field]) && row[field] >= 0, field);
            }
            assert.equal(row.errors, 0);
            const histogram = parseCounts(row.batch_histogram);
            assert.ok(histogram.size <= 256);
            assert.equal([...histogram.values()].reduce((a, b) => a + b, 0), row.append_calls);
            assert.equal([...histogram].reduce((sum, [units, calls]) => {
                assert.ok(units >= 1 && units <= 256);
                return sum + units * calls;
            }, 0), row.append_units);
            const groupUnits = parseCounts(row.group_units);
            assert.ok(groupUnits.size <= groups);
            for (const group of groupUnits.keys()) assert.ok(group >= 1 && group <= groups);
            assert.equal([...groupUnits.values()].reduce((a, b) => a + b, 0), row.append_units);
            assert.ok(row.append_calls <= row.io_append_calls && row.io_append_calls <= row.append_calls + 1);
            assert.ok(row.barrier_tickets <= row.append_units);
            assert.ok(row.barrier_calls <= row.append_calls);
            assert.ok(row.sync_calls >= row.publish_calls && row.sync_calls <= row.publish_calls + 1);
            assert.ok(row.sync_calls >= row.barrier_calls + 1 && row.sync_calls <= row.barrier_calls + 2);
            assert.ok(row.publish_calls >= row.barrier_calls + 1 && row.publish_calls <= row.barrier_calls + 2);
            assert.ok(row.append_max_ns <= row.append_ns && row.barrier_max_ns <= row.barrier_ns);
            if (row.stage.endsWith('_join')) {
                assert.equal(row.append_calls, row.io_append_calls);
                // Independent original appends may share a verified barrier.
                assert.ok(row.barrier_calls > 0 && row.barrier_calls <= row.append_calls);
                assert.equal(row.append_units, row.barrier_tickets);
                assert.equal(row.sync_calls, row.barrier_calls + 1);
                assert.equal(row.publish_calls, row.barrier_calls + 1);
            }
            snapshots.set(key, row);
        }
        const totals = Object.fromEntries(numericFields.map(key => [key, 0]));
        const histogram = new Map();
        for (const replica of [1, 2, 3]) {
            const sequence = stages.map(stage => snapshots.get(`${stage}:${replica}`));
            assert.ok(sequence.every(Boolean));
            for (let i = 1; i <= 2; i++) {
                for (const field of numericFields) assert.ok(sequence[i][field] >= sequence[i - 1][field]);
            }
            for (const field of numericFields) totals[field] += sequence[1][field] - sequence[0][field];
            const before = parseCounts(sequence[0].batch_histogram);
            for (const [units, calls] of parseCounts(sequence[1].batch_histogram)) {
                const delta = calls - (before.get(units) ?? 0);
                assert.ok(delta >= 0);
                histogram.set(units, (histogram.get(units) ?? 0) + delta);
            }
        }
        console.log(`storage measured deltas: append_calls=${totals.append_calls} units=${totals.append_units} commands=${totals.append_commands} mean_units=${(totals.append_units / totals.append_calls).toFixed(3)} append_ms=${(totals.append_ns / 1e6).toFixed(3)} barriers=${totals.barrier_calls} mean_appends_per_barrier=${(totals.append_calls / totals.barrier_calls).toFixed(3)} mean_units_per_barrier=${(totals.barrier_tickets / totals.barrier_calls).toFixed(3)} barrier_ms=${(totals.barrier_ns / 1e6).toFixed(3)} sync_ms=${(totals.sync_ns / 1e6).toFixed(3)} publish_ms=${(totals.publish_ns / 1e6).toFixed(3)} histogram=${[...histogram].sort((a,b)=>a[0]-b[0]).map(([n,c])=>n+':'+c).join('|')}`);
    }
}
