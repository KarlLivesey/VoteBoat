// Private offered-mode adaptation of the completed native470 release readback control.
// The committed raw validator separately checks outcomes, maintenance and storage arithmetic.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
const [root, variant] = process.argv.slice(2);
assert.ok(['off', 'on'].includes(variant));
const tokens = text => Object.fromEntries(text.trim().split(/\s+/).map(pair => pair.split('=')));
const selected = variant === 'on';
const expected = selected ? {
    group: '1', worker_units: '4', append_window_enabled: 'true', append_window_units: '4',
    append_window_originals: '64', append_window_core_bytes: '1048576',
} : {group: '1', worker_units: '1', append_window_enabled: 'false', append_window_units: '1'};
for (const mode of ['create', 'recover']) {
    const rows = fs.readFileSync(path.join(root, `append-window-${mode}.txt`), 'utf8').trim().split('\n').map(tokens);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map(row => row.replica).sort(), ['1', '2', '3']);
    for (const row of rows) {
        assert.deepEqual(Object.keys(row).sort(), [...Object.keys(expected), 'replica'].sort());
        for (const [key, value] of Object.entries(expected)) assert.equal(row[key], value, `${mode}:${row.replica}:${key}`);
    }
}
const summary = tokens(fs.readFileSync(path.join(root, 'summary.txt'), 'utf8'));
for (const [key, value] of Object.entries(expected).filter(([key]) => key.startsWith('append_window'))) assert.equal(summary[key], value, key);
if (!selected) for (const key of ['append_window_originals', 'append_window_core_bytes']) assert.equal(summary[key], undefined);
for (const [key, value] of Object.entries({mode: 'offered', assembly: 'shared', replicas: '3', groups: '1', payload_bytes: '8', offers: '15000', offered_rate: '500', window: '64', warmup: '64', ready_visit_items_effective: '16'})) assert.equal(summary[key], value, key);
const lines = fs.readFileSync(path.join(root, 'storage.csv'), 'utf8').trim().split('\n');
const header = lines.shift().split(',');
assert.equal(header.length, 35);
const rows = lines.map(line => Object.fromEntries(line.split(',').map((value, index) => [header[index], value])));
assert.equal(rows.length, 12);
for (const row of rows) for (const key of ['barrier_missing_tickets', 'barrier_dropped_tickets', 'errors']) assert.equal(row[key], '0', `${row.stage}:${row.replica}:${key}`);
const deltas = [];
for (const replica of ['1', '2', '3']) {
    const before = rows.filter(row => row.stage === 'before_measurement' && row.replica === replica);
    const after = rows.filter(row => row.stage === 'after_measurement' && row.replica === replica);
    assert.equal(before.length, 1); assert.equal(after.length, 1);
    const delta = {replica};
    for (const key of ['synchronized_same_group_multi_unit_calls', 'synchronized_same_group_multi_unit_known_originals']) {
        for (const row of [before[0], after[0]]) assert.ok(/^\d+$/.test(row[key]) && Number.isSafeInteger(Number(row[key])));
        delta[key] = Number(after[0][key]) - Number(before[0][key]);
        assert.ok(delta[key] >= 0);
        if (!selected) assert.equal(delta[key], 0, `OFF:${replica}:${key}`);
    }
    deltas.push(delta);
}
// This fixture places its sole leader on replica1. Positive counts prove activation,
// not useful client completions, exact proposal width, or an observer-free speedup.
if (selected) {
    assert.ok(deltas[0].synchronized_same_group_multi_unit_calls > 0, 'ON measurement sharing inactive');
    assert.ok(deltas[0].synchronized_same_group_multi_unit_known_originals > 0, 'ON measurement sharing has no known originals');
}
console.log(JSON.stringify({root, variant, selected: expected, measurement_physical_sharing: deltas, scope: 'Instrumented finite offered diagnostic; raw validator is authoritative for exact outcome/storage arithmetic.'}));
