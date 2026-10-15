// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';

const fields = ['publication_requested', 'publication_effective', 'publication_provider', 'publication_timings', 'publication_scope', 'storage_error_scope'];
export function validatePublicationProfile(summary, run) {
    if (fields.every(field => summary[field] === undefined)) {
        assert.ok(fields.every(field => run[field] === undefined), 'publication profile missing from summary');
        return false;
    }
    assert.equal(summary.mode, 'offered', 'publication selector requires offered mode');
    assert.equal(run.mode, 'offered', 'publication run metadata requires offered mode');
    assert.equal(summary.assembly, 'shared');
    assert.ok(['atomic', 'witness'].includes(summary.publication_requested));
    assert.equal(summary.publication_effective, summary.publication_requested);
    assert.equal(summary.publication_provider, summary.publication_effective === 'atomic' ? 'FileLogIo' : 'WitnessLogIo');
    assert.equal(summary.publication_timings, 'native');
    assert.equal(summary.publication_scope, 'ordinary_barrier');
    assert.equal(summary.storage_error_scope, 'overlapping_observations');
    for (const field of fields) assert.equal(run[field], summary[field], `run/summary ${field} mismatch`);
    return true;
}
