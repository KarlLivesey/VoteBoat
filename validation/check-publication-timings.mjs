// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// File operation diagnostics; not quorum, durability or latency evidence.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const steps = ['open', 'write', 'file_sync', 'rename', 'directory_sync'];
const stages = ['before_measurement', 'after_measurement', 'after_create_join', 'after_recover_join'];
const fields = ['stage', 'replica', 'step', 'calls', 'errors', 'elapsed_ns', 'max_ns', 'manifest_calls', 'manifest_errors', 'manifest_ns'];

export function validatePublication(csv) {
    const lines = csv.trimEnd().split('\n');
    assert.deepEqual(lines.shift().split(','), fields);
    assert.equal(lines.length, 60, 'need all replica/stage/step snapshots');
    const snapshots = new Map();
    for (const line of lines) {
        const values = line.split(',');
        assert.equal(values.length, fields.length);
        const row = Object.fromEntries(fields.map((f, i) => [f, values[i]]));
        assert.ok(stages.includes(row.stage));
        assert.ok(['1', '2', '3'].includes(row.replica));
        assert.ok(steps.includes(row.step));
        for (const f of fields.slice(3)) {
            assert.match(row[f], /^\d+$/);
            row[f] = BigInt(row[f]);
        }
        assert.equal(row.errors, 0n);
        assert.equal(row.manifest_errors, 0n);
        const key = `${row.stage}:${row.replica}`;
        const snapshot = snapshots.get(key) ?? new Map();
        assert.ok(!snapshot.has(row.step), 'duplicate step');
        snapshot.set(row.step, row);
        snapshots.set(key, snapshot);
    }
    assert.equal(snapshots.size, 12);
    for (const [key, snapshot] of snapshots) {
        assert.equal(snapshot.size, 5);
        const first = snapshot.get('open');
        let elapsed = 0n;
        for (const row of snapshot.values()) {
            assert.equal(row.manifest_calls, first.manifest_calls);
            assert.equal(row.manifest_ns, first.manifest_ns);
            if (key.includes('_join:')) {
                assert.equal(row.calls, row.manifest_calls);
                assert.ok(row.max_ns <= row.elapsed_ns);
                assert.ok(row.calls > 0n);
            }
            elapsed += row.elapsed_ns;
        }
        if (key.includes('_join:')) assert.ok(elapsed <= first.manifest_ns, 'steps exceed enclosing duration');
    }
    const deltas = [];
    for (const replica of ['1', '2', '3']) {
        for (const step of steps) {
            const rows = stages.map(stage => snapshots.get(`${stage}:${replica}`).get(step));
            for (let i = 1; i <= 2; i++) {
                for (const f of ['calls', 'elapsed_ns', 'max_ns', 'manifest_calls', 'manifest_ns']) {
                    assert.ok(rows[i][f] >= rows[i - 1][f], `${f} regressed`);
                }
            }
            const calls = rows[1].calls - rows[0].calls;
            const elapsed = rows[1].elapsed_ns - rows[0].elapsed_ns;
            assert.ok(calls > 0n);
            deltas.push({ replica, step, calls: calls.toString(), elapsed_ns: elapsed.toString() });
        }
    }
    return { scope: 'finite diagnostic; live snapshots may cut calls; joined totals stable; parallel times are not client critical path', measurement_deltas: deltas };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    assert.equal(process.argv.length, 3, 'usage: node check-publication-timings.mjs PUBLICATION_CSV');
    process.stdout.write(JSON.stringify(validatePublication(fs.readFileSync(process.argv[2], 'utf8')), null, 2) + '\n');
}
