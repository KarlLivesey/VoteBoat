// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { checkStorage } from './check-benchmark-storage.mjs';
const legacy = 'stage,replica,append_calls,append_units,append_commands,append_ns,append_max_ns,barrier_calls,barrier_tickets,barrier_ns,barrier_max_ns,errors,encoded_bytes,io_append_calls,io_append_ns,sync_calls,sync_ns,publish_calls,publish_ns,batch_histogram,group_units'.split(',');
const extra = 'successful_barriers,barrier_known_originals,barrier_missing_tickets,barrier_dropped_tickets,barrier_pending_tickets,barrier_correlation_capacity_bytes,barrier_unit_histogram,barrier_original_histogram,barrier_same_group_unit_histogram,synchronized_barrier_calls,no_io_barrier_calls,ambiguous_io_barrier_calls,synchronized_same_group_multi_unit_calls,synchronized_same_group_multi_unit_known_originals'.split(',');
function row(n) {
    return { append_calls:4*n, append_units:8*n, append_commands:12*n, append_ns:100*n, append_max_ns:10*n,
        barrier_calls:4*n, barrier_tickets:8*n, barrier_ns:100*n, barrier_max_ns:10*n, errors:0,
        encoded_bytes:100*n, io_append_calls:4*n, io_append_ns:50*n, sync_calls:2*n+1,
        sync_ns:20*n, publish_calls:2*n+1, publish_ns:30*n, batch_histogram:`2:${4*n}`, group_units:`1:${8*n}`,
        successful_barriers:4*n, barrier_known_originals:12*n, barrier_missing_tickets:0,
        barrier_dropped_tickets:0, barrier_pending_tickets:0, barrier_correlation_capacity_bytes:1024,
        barrier_unit_histogram:`2:${4*n}`, barrier_original_histogram:`3:${4*n}`,
        barrier_same_group_unit_histogram:`2:${4*n}`, synchronized_barrier_calls:2*n,
        no_io_barrier_calls:2*n, ambiguous_io_barrier_calls:0,
        synchronized_same_group_multi_unit_calls:2*n, synchronized_same_group_multi_unit_known_originals:6*n };
}
function fixture(current = true, change = () => {}, alterFields = fields => fields) {
    const fields = alterFields(current ? [...legacy, ...extra] : [...legacy]);
    const rows = [];
    for (const [i, stage] of ['before_measurement','after_measurement','after_create_join','after_recover_join'].entries()) {
        for (const replica of [1,2,3]) {
            const n = i === 3 ? 1 : i + 1;
            const value = {stage, replica, ...row(n)};
            if (!current) value.sync_calls = value.publish_calls = value.barrier_calls + 1;
            change(value, n);
            rows.push(fields.map(key => value[key]).join(','));
        }
    }
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'voteboat-storage-check-'));
    const file = path.join(directory, 'storage.csv');
    fs.writeFileSync(file, [fields.join(','), ...rows].join('\n')+'\n');
    try { return checkStorage(file, 1, 1); }
    finally { fs.rmSync(directory, {recursive:true}); }
}
test('historical21 remains supported with original physical barrier rules', () => {
    assert.equal(fixture(false).schema, 21);
    assert.throws(() => fixture(false, value => value.sync_calls--));
});
test('current35 distinguishes logical barriers from physical synchronization', () => {
    const value = fixture();
    assert.equal(value.schema, 35);
    assert.equal(value.correlationComplete, true);
    assert.equal(value.totals.barrier_calls, 12);
    assert.equal(value.totals.synchronized_barrier_calls, 6);
    assert.equal(value.totals.no_io_barrier_calls, 6);
});
test('ambiguous I/O remains explicit without inventing exact per-call pairs', () => {
    const value = fixture(true, (r,n) => {
        r.no_io_barrier_calls = n; r.ambiguous_io_barrier_calls = n;
        r.sync_calls += 2*n; r.publish_calls += 2*n;
    });
    assert.equal(value.totals.ambiguous_io_barrier_calls, 3);
    assert.throws(() => fixture(true, (r,n) => {
        r.no_io_barrier_calls = n; r.ambiguous_io_barrier_calls = n;
    }));
});
test('missing/dropped metadata is visible and cannot establish full original correlation', () => {
    const value = fixture(true, (r,n) => {
        r.barrier_missing_tickets = n; r.barrier_dropped_tickets = n;
        r.barrier_original_histogram = `3:${3*n}`; r.barrier_known_originals = 9*n;
    });
    assert.equal(value.correlationComplete, false);
    assert.equal(value.totals.barrier_missing_tickets, 3);
    assert.throws(() => fixture(true, (r,n) => { r.barrier_missing_tickets = n; }));
});
test('overflow65 bins provide lower bounds rather than false exact widths', () => {
    fixture(true, r => Object.assign(r, {
        append_calls:66, append_units:66, append_commands:80, io_append_calls:66,
        batch_histogram:'1:66', group_units:'1:66', barrier_calls:1, barrier_tickets:66,
        successful_barriers:1, barrier_unit_histogram:'65:1', barrier_original_histogram:'65:1',
        barrier_same_group_unit_histogram:'65:1', barrier_known_originals:80,
        synchronized_barrier_calls:1, no_io_barrier_calls:0, sync_calls:2, publish_calls:2,
        synchronized_same_group_multi_unit_calls:1, synchronized_same_group_multi_unit_known_originals:80,
    }));
});
test('schema, bins, integer fields, classifications and exact conservation reject mutations', () => {
    for (const mutate of [
        r => { r.batch_histogram = '2:2|2:2'; },
        r => { r.barrier_unit_histogram = '66:1'; },
        r => { r.barrier_unit_histogram = '0:4'; },
        r => { r.barrier_unit_histogram = '1:1:2'; },
        r => { r.barrier_known_originals = ''; },
        r => { r.barrier_known_originals = 1.5; },
        r => { r.successful_barriers++; },
        r => { r.no_io_barrier_calls++; },
        r => { r.synchronized_barrier_calls--; r.ambiguous_io_barrier_calls++; r.synchronized_same_group_multi_unit_calls--; r.synchronized_same_group_multi_unit_known_originals -= 3; },
        r => { r.barrier_pending_tickets++; },
        r => { r.barrier_known_originals++; },
        r => { r.barrier_same_group_unit_histogram = '3:4'; },
        r => { r.synchronized_same_group_multi_unit_calls = 0; },
        r => { r.synchronized_same_group_multi_unit_known_originals = 999; },
        r => { r.sync_calls++; r.publish_calls++; },
        r => { r.barrier_original_histogram = '0:4'; },
        r => { r.barrier_original_histogram = `0:${r.successful_barriers}`; r.barrier_known_originals = 0; r.synchronized_same_group_multi_unit_known_originals = 0; },
    ]) assert.throws(() => fixture(true, mutate));
    assert.throws(() => fixture(true, undefined, fields => fields.slice(0,-1)));
    assert.throws(() => fixture(true, undefined, fields => [...fields,'invented']));
    assert.throws(() => fixture(true, undefined, fields => [fields[1],fields[0],...fields.slice(2)]));
});
test('a histogram cannot lose an earlier bin across measurement snapshots', () => {
    assert.throws(() => fixture(true, (r,n) => {
        if (n === 1) { r.batch_histogram = '1:2|3:2'; }
    }));
});
