// SPDX-License-Identifier: RPL-1.5
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { test } from 'node:test';
import { checkObligations } from './check-provider-conformance.mjs';

const root = new URL('../', import.meta.url);
const inventory = JSON.parse(fs.readFileSync(new URL('docs/component-contracts.json', root), 'utf8'));
const ledger = JSON.parse(fs.readFileSync(new URL('docs/provider-conformance.json', root), 'utf8'));
const read = file => fs.readFileSync(new URL(file, root), 'utf8');
const admissionReview = value => value.reviews.find(
    review => review.public_contract === 'AdmissionPolicy / AdmissionRequest / AdmissionLease');

test('review counts only audited operations and retains unreviewed contracts', () => {
    const report = checkObligations(inventory, ledger, read);
    assert.equal(report.reviewed_contracts, 6);
    assert.equal(report.reviewed_operations, 39);
    assert.equal(report.unreviewed_contracts.length, inventory.contracts.length - 6);
    for (const name of ['LogStore', 'SnapshotStore', 'SnapshotRetention', 'SnapshotWorker', 'CredentialJournal / CredentialRecordIo', 'AdmissionPolicy / AdmissionRequest / AdmissionLease']) {
        assert.ok(!report.unreviewed_contracts.includes(name));
    }
});

for (const [name, mutate] of [
    ['unknown contract', d => { d.reviews[0].public_contract = 'InventedStore'; }],
    ['unknown operation', d => { d.reviews[0].operations[0].names[0] = 'invented'; }],
    ['missing operation', d => { d.reviews[0].operations.pop(); }],
    ['duplicate operation', d => { d.reviews[0].operations[1].names.push('binding'); }],
    ['missing assertion', d => { d.reviews[0].operations[0].assertions = []; }],
    ['missing function', d => { d.reviews[0].operations[0].assertions[0].symbol = 'invented'; }],
    ['missing test', d => { d.reviews[0].runners[0].symbol = 'invented'; }],
    ['escaping path', d => { d.reviews[0].runners[0].file = '../src/lib.rs'; }],
    ['false completion claim', d => { d.reviews[0].status = 'complete'; }],
    ['missing limitation', d => { d.reviews[0].operations[0].remaining = []; }],
    ['missing snapshot operation', d => { d.reviews[1].operations.pop(); }],
    ['missing retention assertion', d => { d.reviews[2].operations[0].assertions[0].symbol = 'invented'; }],
    ['missing admission operation', d => { admissionReview(d).operations.pop(); }],
    ['missing admission assertion', d => { admissionReview(d).operations[0].assertions = []; }]
]) {
    test(`rejects ${name}`, () => {
        const changed = structuredClone(ledger);
        mutate(changed);
        assert.throws(() => checkObligations(inventory, changed, read));
    });
}
