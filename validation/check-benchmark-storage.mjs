// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
const legacy = 'stage,replica,append_calls,append_units,append_commands,append_ns,append_max_ns,barrier_calls,barrier_tickets,barrier_ns,barrier_max_ns,errors,encoded_bytes,io_append_calls,io_append_ns,sync_calls,sync_ns,publish_calls,publish_ns,batch_histogram,group_units'.split(',');
const extra = 'successful_barriers,barrier_known_originals,barrier_missing_tickets,barrier_dropped_tickets,barrier_pending_tickets,barrier_correlation_capacity_bytes,barrier_unit_histogram,barrier_original_histogram,barrier_same_group_unit_histogram,synchronized_barrier_calls,no_io_barrier_calls,ambiguous_io_barrier_calls,synchronized_same_group_multi_unit_calls,synchronized_same_group_multi_unit_known_originals'.split(',');
const histograms = ['batch_histogram', 'group_units', 'barrier_unit_histogram', 'barrier_original_histogram', 'barrier_same_group_unit_histogram'];
// These describe each observation, not cumulative work. Capacity belongs to
// Trace::snapshot's cloned pending Vec, not the live observer's retained backing.
const gaugeFields = ['barrier_pending_tickets', 'barrier_correlation_capacity_bytes'];
const sum = map => [...map.values()].reduce((a, b) => a + b, 0);
const weight = map => [...map].reduce((a, [key, count]) => a + key * count, 0);
function counts(value) {
    const result = new Map();
    for (const pair of value.split('|').filter(Boolean)) {
        assert.match(pair, /^\d+:\d+$/);
        const [key, count] = pair.split(':').map(Number);
        assert.ok(Number.isSafeInteger(key) && Number.isSafeInteger(count) && count > 0);
        assert.ok(!result.has(key), 'duplicate histogram bin');
        result.set(key, count);
    }
    return result;
}
function bounded(value, minimum) {
    const map = counts(value);
    assert.ok(map.size <= 66);
    for (const bin of map.keys()) assert.ok(bin >= minimum && bin <= 65, 'bounded histogram bin');
    return map;
}
function checkDependencies(row, joined) {
    const units = bounded(row.barrier_unit_histogram, 1);
    const originals = bounded(row.barrier_original_histogram, 0);
    const same = bounded(row.barrier_same_group_unit_histogram, 1);
    const successes = row.successful_barriers;
    assert.equal(successes, row.barrier_calls, 'errors=0 requires every barrier to succeed');
    assert.equal(row.synchronized_barrier_calls + row.no_io_barrier_calls + row.ambiguous_io_barrier_calls, successes, 'physical classifications partition successes');
    assert.equal(sum(units), successes);
    assert.equal(sum(same), successes);
    assert.ok(sum(originals) <= successes);
    assert.ok(successes - sum(originals) <= row.barrier_missing_tickets);
    if (row.barrier_missing_tickets === 0) assert.equal(sum(originals), successes);
    assert.ok(row.barrier_missing_tickets <= row.barrier_tickets);
    assert.ok(row.barrier_dropped_tickets <= row.append_units);
    assert.equal(row.barrier_pending_tickets, row.append_units - row.barrier_dropped_tickets - row.barrier_tickets + row.barrier_missing_tickets, 'exact observed ticket conservation');
    assert.ok(row.barrier_correlation_capacity_bytes >= row.barrier_pending_tickets);
    assert.ok(row.barrier_known_originals <= row.append_commands, 'commands are suffix originals, not client successes');
    if (row.barrier_pending_tickets === 0 && row.barrier_missing_tickets === 0 && row.barrier_dropped_tickets === 0) {
        assert.equal(row.barrier_known_originals, row.append_commands, 'every observed command belongs to a consumed exact ticket');
    }
    assert.ok(weight(units) <= row.barrier_tickets);
    if (!units.has(65)) assert.equal(weight(units), row.barrier_tickets);
    assert.ok(weight(originals) <= row.barrier_known_originals);
    if (!originals.has(65) && row.barrier_missing_tickets === 0) assert.equal(weight(originals), row.barrier_known_originals);
    for (let threshold = 1; threshold <= 65; threshold++) {
        const above = map => [...map].filter(([bin]) => bin >= threshold).reduce((n, [, count]) => n + count, 0);
        assert.ok(above(same) <= above(units), 'same-group maximum cannot exceed total units');
    }
    const grouped = successes - (same.get(1) ?? 0);
    const synchronized = row.synchronized_barrier_calls;
    assert.ok(row.synchronized_same_group_multi_unit_calls <= Math.min(grouped, synchronized));
    assert.ok(row.synchronized_same_group_multi_unit_calls >= Math.max(0, grouped - row.no_io_barrier_calls - row.ambiguous_io_barrier_calls - row.barrier_missing_tickets));
    assert.ok(row.synchronized_same_group_multi_unit_known_originals <= row.barrier_known_originals);
    if (row.synchronized_same_group_multi_unit_calls === 0) assert.equal(row.synchronized_same_group_multi_unit_known_originals, 0);
    if (synchronized === successes && row.barrier_missing_tickets === 0 && grouped === successes) {
        assert.equal(row.synchronized_same_group_multi_unit_known_originals, row.barrier_known_originals);
    }
    // One startup sync/publication is outside ObservedLog. A snapshot can catch
    // one operation in flight. Ambiguous barriers do not expose their exact pair.
    assert.ok(row.sync_calls >= synchronized + 1 && row.publish_calls >= synchronized + 1);
    if (row.ambiguous_io_barrier_calls === 0) {
        assert.ok(row.sync_calls <= synchronized + 2 && row.publish_calls <= synchronized + 2);
        if (joined) {
            assert.equal(row.sync_calls, synchronized + 1);
            assert.equal(row.publish_calls, synchronized + 1);
        }
    } else if (joined) {
        assert.ok(row.sync_calls + row.publish_calls - 2 * (synchronized + 1) >= row.ambiguous_io_barrier_calls, 'each ambiguous pair contains at least one I/O call');
        if (row.ambiguous_io_barrier_calls === 1) {
            assert.ok(row.sync_calls !== synchronized + 2 || row.publish_calls !== synchronized + 2, 'a sole remaining 1/1 pair is synchronized, not ambiguous');
        }
    }
}
export function checkStorage(storagePath, firstGroup, groups) {
    const storageLines = fs.readFileSync(storagePath, 'utf8').trim().split(/\r?\n/);
    const fields = storageLines.shift().split(',');
    const current = fields.length === legacy.length + extra.length;
    assert.deepEqual(fields, current ? [...legacy, ...extra] : legacy);
    assert.equal(storageLines.length, 12);
    const stages = ['before_measurement', 'after_measurement', 'after_create_join', 'after_recover_join'];
    const snapshots = new Map();
    const numericFields = fields.slice(2).filter(field => !histograms.includes(field));
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
            assert.match(row[field], /^\d+$/, field);
            row[field] = Number(row[field]);
            assert.ok(Number.isSafeInteger(row[field]) && row[field] >= 0, field);
        }
        assert.equal(row.errors, 0);
        const histogram = counts(row.batch_histogram);
        assert.ok(histogram.size <= 256);
        assert.equal(sum(histogram), row.append_calls);
        for (const units of histogram.keys()) assert.ok(units >= 1 && units <= 256);
        assert.equal(weight(histogram), row.append_units);
        const groupUnits = counts(row.group_units);
        assert.ok(groupUnits.size <= groups);
        for (const group of groupUnits.keys()) assert.ok(group >= firstGroup && group < firstGroup + groups);
        assert.equal(sum(groupUnits), row.append_units);
        assert.ok(row.append_calls <= row.io_append_calls && row.io_append_calls <= row.append_calls + 1);
        assert.ok(row.barrier_tickets <= row.append_units);
        assert.ok(row.barrier_calls <= row.append_calls);
        assert.ok(row.sync_calls >= row.publish_calls && row.sync_calls <= row.publish_calls + 1);
        assert.ok(row.append_max_ns <= row.append_ns && row.barrier_max_ns <= row.barrier_ns);
        const joined = row.stage.endsWith('_join');
        if (current) checkDependencies(row, joined);
        else {
            assert.ok(row.sync_calls >= row.barrier_calls + 1 && row.sync_calls <= row.barrier_calls + 2);
            assert.ok(row.publish_calls >= row.barrier_calls + 1 && row.publish_calls <= row.barrier_calls + 2);
        }
        if (joined) {
            assert.equal(row.append_calls, row.io_append_calls);
            assert.ok(row.barrier_calls > 0 && row.barrier_calls <= row.append_calls);
            assert.equal(row.append_units, row.barrier_tickets);
            if (!current) {
                assert.equal(row.sync_calls, row.barrier_calls + 1);
                assert.equal(row.publish_calls, row.barrier_calls + 1);
            }
        }
        snapshots.set(key, row);
    }
    const monotonic = numericFields.filter(field => !gaugeFields.includes(field));
    const totals = Object.fromEntries(monotonic.map(key => [key, 0]));
    const gauges = [];
    const histogram = new Map();
    for (const replica of [1, 2, 3]) {
        const sequence = stages.map(stage => snapshots.get(`${stage}:${replica}`));
        assert.ok(sequence.every(Boolean));
        for (let i = 1; i <= 2; i++) {
            for (const field of monotonic) assert.ok(sequence[i][field] >= sequence[i - 1][field], field);
            for (const field of histograms.filter(field => fields.includes(field))) {
                const before = counts(sequence[i - 1][field]), after = counts(sequence[i][field]);
                for (const [bin, count] of before) assert.ok((after.get(bin) ?? 0) >= count, field);
            }
        }
        for (const field of monotonic) totals[field] += sequence[1][field] - sequence[0][field];
        if (current) for (const row of sequence) gauges.push(Object.fromEntries(
            ['stage', 'replica', ...gaugeFields].map(field => [field, row[field]])));
        const before = counts(sequence[0].batch_histogram);
        for (const [units, calls] of counts(sequence[1].batch_histogram)) histogram.set(units, (histogram.get(units) ?? 0) + calls - (before.get(units) ?? 0));
    }
    // Elapsed scopes overlap and are wall time, never summed as CPU. Originals
    // count suffix commands, including replicas/retries, not successful clients.
    const correlationComplete = current && [...snapshots.values()].every(row => row.barrier_missing_tickets === 0 && row.barrier_dropped_tickets === 0);
    console.log(`storage measured deltas: append_calls=${totals.append_calls} units=${totals.append_units} commands=${totals.append_commands} mean_units=${(totals.append_units / totals.append_calls).toFixed(3)} append_ms=${(totals.append_ns / 1e6).toFixed(3)} barriers=${totals.barrier_calls} mean_appends_per_barrier=${(totals.append_calls / totals.barrier_calls).toFixed(3)} mean_units_per_barrier=${(totals.barrier_tickets / totals.barrier_calls).toFixed(3)} barrier_ms=${(totals.barrier_ns / 1e6).toFixed(3)} sync_ms=${(totals.sync_ns / 1e6).toFixed(3)} publish_ms=${(totals.publish_ns / 1e6).toFixed(3)} histogram=${[...histogram].sort((a,b)=>a[0]-b[0]).map(([n,c])=>n+':'+c).join('|')}${current ? ` synchronized=${totals.synchronized_barrier_calls} no_io=${totals.no_io_barrier_calls} ambiguous_io=${totals.ambiguous_io_barrier_calls} dependency_correlation_complete=${correlationComplete}` : ''}`);
    return { schema: current ? 35 : 21, totals, gauges, correlationComplete };
}
