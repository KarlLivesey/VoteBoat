// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Original-step counter arithmetic, not eligibility/hit-rate or quorum evidence.
import assert from 'node:assert/strict';
export function validateJoined(summary, csv) {
    const integer = value => {
        assert.match(String(value), /^(0|[1-9]\d*)$/);
        const result = Number(value);
        assert.ok(Number.isSafeInteger(result));
        return result;
    };
    const bound = integer(summary.ready_reply_inputs_effective);
    assert.ok([0, 2].includes(bound));
    assert.equal(integer(summary.ready_reply_inputs_requested), bound);
    assert.equal(summary.ready_commit_batch_capable, 'true');
    const lines = csv.trim().split('\n');
    assert.equal(lines.shift(), 'phase,replica,joined_transitions,joined_reply_inputs,joined_originals');
    assert.equal(lines.length, 6);
    const seen = new Set();
    const totals = {warmup: [0, 0, 0], measurement: [0, 0, 0]};
    for (const line of lines) {
        const fields = line.split(',');
        assert.equal(fields.length, 5);
        const [phase, ...values] = fields;
        assert.ok(Object.hasOwn(totals, phase));
        const [replica, transitions, replies, originals] = values.map(integer);
        assert.ok([1, 2, 3].includes(replica));
        const key = `${phase}:${replica}`;
        assert.ok(!seen.has(key));
        seen.add(key);
        assert.ok(transitions <= replies && replies <= transitions * bound);
        assert.ok(transitions <= originals && originals <= transitions * 64);
        if (!transitions) assert.equal(originals, 0);
        for (const [index, value] of [transitions, replies, originals].entries()) {
            totals[phase][index] += value;
            assert.ok(Number.isSafeInteger(totals[phase][index]));
        }
    }
    assert.ok(totals.warmup[2] <= integer(summary.warmup));
    assert.ok(totals.measurement[2] <= integer(summary.offers ?? summary.operations));
    assert.deepEqual(totals.measurement, ['joined_transitions', 'joined_reply_inputs', 'joined_originals'].map(key => integer(summary[key])));
    return totals;
}
