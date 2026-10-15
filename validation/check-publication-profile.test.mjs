// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import test from 'node:test';
import { validatePublicationProfile } from './check-publication-profile.mjs';

function fixture(provider) {
    return { mode: 'offered', assembly: 'shared', publication_requested: provider, publication_effective: provider,
        publication_provider: provider === 'atomic' ? 'FileLogIo' : 'WitnessLogIo', publication_timings: 'native', publication_scope: 'ordinary_barrier', storage_error_scope: 'overlapping_observations' };
}
test('unchanged legacy results need no publication profile; explicit choices match run metadata', () => {
    assert.equal(validatePublicationProfile({}, {}), false);
    for (const provider of ['atomic', 'witness']) assert.equal(validatePublicationProfile(fixture(provider), fixture(provider)), true);
});
test('partial or forged selected providers, scope and recovery metadata are refused', () => {
    for (const provider of ['atomic', 'witness']) {
        const valid = fixture(provider);
        for (const field of Object.keys(valid)) {
            const missing = { ...valid }; delete missing[field];
            assert.throws(() => validatePublicationProfile(missing, valid), `missing ${field}`);
            assert.throws(() => validatePublicationProfile({ ...valid, [field]: 'wrong' }, valid), field);
        }
        for (const field of Object.keys(valid).filter(field => field.startsWith('publication_') || field === 'storage_error_scope')) {
            assert.throws(() => validatePublicationProfile(valid, { ...valid, [field]: 'wrong' }), field);
        }
        assert.throws(() => validatePublicationProfile({}, valid));
        assert.throws(() => validatePublicationProfile(valid, { ...valid, mode: 'closed' }));
    }
});
