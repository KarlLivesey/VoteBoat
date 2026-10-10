// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import test from 'node:test';
import { steps, validatePublication } from './check-publication-timings.mjs';

function fixture() {
    const rows = ['stage,replica,step,calls,errors,elapsed_ns,max_ns,manifest_calls,manifest_errors,manifest_ns'];
    for (const [i, stage] of ['before_measurement', 'after_measurement', 'after_create_join', 'after_recover_join'].entries()) {
        const calls = i === 3 ? 2 : 10 * (i + 1);
        for (let replica = 1; replica <= 3; replica++) {
            for (const step of steps) rows.push(`${stage},${replica},${step},${calls},0,${calls * 100},100,${calls},0,${calls * 600}`);
        }
    }
    return rows.join('\n') + '\n';
}
function changed(field, value, row = 6) {
    const lines = fixture().trimEnd().split('\n');
    const index = lines[0].split(',').indexOf(field);
    const values = lines[row].split(',');
    values[index] = value;
    lines[row] = values.join(',');
    return lines.join('\n') + '\n';
}
test('complete diagnostic has fifteen independent measured step deltas', () => {
    const result = validatePublication(fixture());
    assert.equal(result.measurement_deltas.length, 15);
    assert.equal(result.measurement_deltas[0].calls, '10');
    assert.equal(result.measurement_deltas[0].elapsed_ns, '1000');
});
test('partial, duplicate, unknown and error snapshots are refused', () => {
    assert.throws(() => validatePublication(fixture().trimEnd().split('\n').slice(0, -1).join('\n')));
    for (const [field, value] of [['step','open'], ['replica','4'], ['stage','measurement_failure'], ['calls','-1'], ['errors','1'], ['manifest_errors','1']]) {
        assert.throws(() => validatePublication(changed(field, value, field === 'step' ? 7 : 6)), field);
    }
});
test('regression and inconsistent joined operation totals are refused', () => {
    for (const [field, value, row] of [['calls','1',16], ['elapsed_ns','1',16], ['max_ns','1',16], ['calls','29',31], ['manifest_ns','1',31], ['max_ns','999999',31]]) {
        assert.throws(() => validatePublication(changed(field, value, row)), field);
    }
});
test('live snapshot can end inside a call without inventing an atomic observation', () => {
    assert.doesNotThrow(() => validatePublication(changed('calls', '21', 16)));
    const rows = changed('max_ns', '2001', 16).trimEnd().split('\n');
    const joined = rows[31].split(',');
    joined[6] = '2001';
    rows[31] = joined.join(',');
    assert.doesNotThrow(() => validatePublication(rows.join('\n') + '\n'));
});
