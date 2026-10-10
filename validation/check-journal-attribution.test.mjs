// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Mutate the actual retained native corpus, rather than fabricate a passing WAL.
import fs from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyAttribution } from './check-journal-attribution.mjs';

const read = relative => fs.readFileSync(new URL(relative, import.meta.url), 'utf8');
const receipt = read('performance/slice222/reference.csv');
const source = [1, 2, 3].map(i => read(`performance/slice228/reference-replica${i}.csv`));
function alter(text, change) {
    const [header, ...lines] = text.trimEnd().split('\n');
    const fields = header.split(',');
    const changed = lines.flatMap(line => {
        const values = line.split(',');
        const row = Object.fromEntries(fields.map((field, i) => [field, values[i]]));
        return change(row) === false ? [] : [fields.map(field => row[field]).join(',')];
    });
    return `${header}\n${changed.join('\n')}\n`;
}
function reject(changed, message) {
    assert.throws(() => verifyAttribution(receipt, [changed, ...source.slice(1)]), message);
}

test('all original receipt positions map in both actual three-replica runs', () => {
    for (const mode of ['reference', 'diagnostic']) {
        const result = verifyAttribution(read(`performance/slice222/${mode}.csv`),
            [1, 2, 3].map(i => read(`performance/slice228/${mode}-replica${i}.csv`)));
        assert.equal(result.receipts, 256);
        assert.equal(result.synchronization_measured, false);
        for (const replica of result.replicas) {
            assert.equal(replica.original_receipts_matched, 256);
            assert.deepEqual(replica.records_per_original_receipt, { 2: 256 });
            assert.equal(replica.selected_physical_batches, 512);
        }
    }
});

test('later duplicate operation IDs cannot substitute for the original committed position', () => {
    let originalBatch;
    alter(source[0], row => {
        if (row.operation === '320' && row.index === '321' && row.event === 'commit') originalBatch = row.batch;
    });
    assert.ok(originalBatch);
    assert.match(source[0], /,commit,320,324,2,1,/); // Actual later retry remains.
    const changed = alter(source[0], row => {
        if (row.batch !== originalBatch) return;
        if (row.event === 'commit') return false;
        row.committed_commands = '0'; row.class = 'control';
    });
    reject(changed, /original committed position missing/);
});

test('a command commit without its exact earlier append cannot be attributed', () => {
    const changed = alter(source[0], row => {
        if (row.operation === '65' && row.event === 'commit') row.term = '2';
    });
    reject(changed, /original append missing/);
});

test('a measured receipt cannot move to an unrelated log position', () => {
    const changed = alter(receipt, row => { if (row.operation === '65') row.applied_index = '1'; });
    assert.throws(() => verifyAttribution(changed, source), /original committed position missing/);
});

test('physical offset holes and mismatched event ownership are refused', () => {
    reject(alter(source[0], row => { if (row.event === 'batch' && row.batch === '2') row.offset = '0'; }),
        /physical offset discontinuity/);
    reject(alter(source[0], row => { if (row.event === 'commit' && row.operation === '65') row.store = '2'; }),
        /wrong replica/);
});

test('a command append is not reported as a combined append and commit', () => {
    reject(alter(source[0], row => {
        if (row.class === 'append_only') row.class = 'combined';
    }), /physical classification/);
});
