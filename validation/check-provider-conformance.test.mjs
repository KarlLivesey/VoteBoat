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
const review = (value, name = 'LogStore') => value.reviews.find(row => row.public_contract === name);

test('review counts only audited operations and retains unreviewed contracts', () => {
    const report = checkObligations(inventory, ledger, read);
    assert.equal(report.reviewed_contracts, 7);
    assert.equal(report.reviewed_operations, 45);
    assert.equal(report.unreviewed_contracts.length, inventory.contracts.length - 7);
    for (const name of ['TimerService', 'LogStore', 'SnapshotStore', 'SnapshotRetention', 'SnapshotWorker', 'CredentialJournal / CredentialRecordIo', 'AdmissionPolicy / AdmissionRequest / AdmissionLease']) {
        assert.ok(!report.unreviewed_contracts.includes(name));
    }
});

for (const [name, mutate] of [
    ['unknown contract', d => { review(d).public_contract = 'InventedStore'; }],
    ['unknown operation', d => { review(d).operations[0].names[0] = 'invented'; }],
    ['missing operation', d => { review(d).operations.pop(); }],
    ['duplicate operation', d => { review(d).operations[1].names.push('binding'); }],
    ['missing assertion', d => { review(d).operations[0].assertions = []; }],
    ['missing function', d => { review(d).operations[0].assertions[0].symbol = 'invented'; }],
    ['missing test', d => { review(d).runners[0].symbol = 'invented'; }],
    ['escaping path', d => { review(d).runners[0].file = '../src/lib.rs'; }],
    ['false completion claim', d => { review(d).status = 'complete'; }],
    ['missing limitation', d => { review(d).operations[0].remaining = []; }],
    ['missing snapshot operation', d => { review(d, 'SnapshotStore').operations.pop(); }],
    ['missing retention assertion', d => { review(d, 'SnapshotRetention').operations[0].assertions[0].symbol = 'invented'; }],
    ['missing admission operation', d => { admissionReview(d).operations.pop(); }],
    ['missing admission assertion', d => { admissionReview(d).operations[0].assertions = []; }],
    ['missing timer operation', d => { review(d, 'TimerService').operations.pop(); }],
    ['missing timer assertion', d => { review(d, 'TimerService').operations[0].assertions = []; }],
    ['missing timer limitation', d => { review(d, 'TimerService').operations[0].remaining = []; }]
]) {
    test(`rejects ${name}`, () => {
        const changed = structuredClone(ledger);
        mutate(changed);
        assert.throws(() => checkObligations(inventory, changed, read));
    });
}
