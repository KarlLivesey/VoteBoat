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
    assert.equal(report.reviewed_contracts, 40);
    assert.equal(report.reviewed_operations, 388);
    assert.equal(report.unreviewed_contracts.length, inventory.contracts.length - 40);
    for (const name of ['WireCodec', 'StateMachine', 'ReadableStateMachine', 'CheckpointStateMachine', 'BoundedStateMachine + ApplicationReceipt', 'PeerTransport', 'TimerService', 'ReadyScheduler', 'LogStore', 'SnapshotStore', 'SnapshotRetention', 'SnapshotWorker', 'CredentialJournal / CredentialRecordIo', 'AdmissionPolicy / AdmissionRequest / AdmissionLease', 'BufferPool / FrameBuffer / BufferClass / BufferLimits / BufferUsage / BufferOwner / BufferOwnerLimits']) {
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
    ['missing logical tracked cancellation', d => {
        const logical = d.reviews.find(r => r.public_contract.startsWith('LogicalNode /'));
        logical.operations.find(o => o.names.includes('cancel_learner_readiness_tracked')).names = ['request_learner_readiness_tracked'];
    }],
    ['missing logical session retirement assertion', d => {
        const logical = d.reviews.find(r => r.public_contract.startsWith('LogicalNode /'));
        logical.operations.find(o => o.names.includes('retire_peer_session')).assertions = [];
    }],
    ['missing logical attachment limitation', d => {
        const logical = d.reviews.find(r => r.public_contract.startsWith('LogicalNode /'));
        logical.operations.find(o => o.names.includes('attach_peer_session')).remaining = [];
    }],
    ['missing original channel retirement', d => {
        const channel = d.reviews.find(r => r.public_contract.startsWith('PeerNetworkChannelReplacement /'));
        channel.operations[0].names = channel.operations[0].names.filter(n => n !== 'retire_channel');
    }],
    ['missing delegated receiving access assertion', d => {
        const source = d.reviews.find(r => r.public_contract.startsWith('AuthenticatedSourceView /'));
        source.operations.find(o => o.names.includes('receiving_access')).assertions = [];
    }],
    ['missing delegated path limitation', d => {
        const source = d.reviews.find(r => r.public_contract.startsWith('AuthenticatedSourceView /'));
        source.operations.find(o => o.names.includes('path')).remaining = [];
    }],
    ['missing retention assertion', d => { review(d, 'SnapshotRetention').operations[0].assertions[0].symbol = 'invented'; }],
    ['missing admission operation', d => { admissionReview(d).operations.pop(); }],
    ['missing admission assertion', d => { admissionReview(d).operations[0].assertions = []; }],
    ['missing timer operation', d => { review(d, 'TimerService').operations.pop(); }],
    ['missing timer assertion', d => { review(d, 'TimerService').operations[0].assertions = []; }],
    ['missing timer limitation', d => { review(d, 'TimerService').operations[0].remaining = []; }],
    ['missing exact owner isolation assertion', d => { review(d, 'TimerService').operations[1].assertions.find(a => a.symbol === 'exact_owner_isolation').symbol = 'invented'; }],
    ['missing poll preservation assertion', d => { review(d, 'TimerService').operations[2].assertions.find(a => a.symbol === 'refusal_preserves_deadlines').symbol = 'invented'; }],
    ['missing timer defective-provider runner', d => { review(d, 'TimerService').runners.find(a => a.symbol === 'shared_poll_assertions_detect_budget_bypass').symbol = 'invented'; }],
    ['missing transport operation', d => { review(d, 'PeerTransport').operations.pop(); }],
    ['missing transport assertion', d => { review(d, 'PeerTransport').operations[0].assertions = []; }],
    ['missing transport limitation', d => { review(d, 'PeerTransport').operations[0].remaining = []; }],
    ['missing transport inspection', d => { review(d, 'PeerTransport').operations = review(d, 'PeerTransport').operations.filter(o => !o.names.includes('drain_status')); }],
    ['missing inspection assertion', d => { review(d, 'PeerTransport').operations.find(o => o.names.includes('drain_status')).assertions = []; }],
    ['missing inspection limitation', d => { review(d, 'PeerTransport').operations.find(o => o.names.includes('drain_status')).remaining = []; }],
    ['missing wire operation', d => { review(d, 'WireCodec').operations.pop(); }],
    ['missing wire assertion', d => { review(d, 'WireCodec').operations[0].assertions = []; }],
    ['missing wire limitation', d => { review(d, 'WireCodec').operations[0].remaining = []; }],
    ['missing buffer operation', d => { d.reviews.at(-1).operations.pop(); }],
    ['missing buffer assertion', d => { d.reviews.at(-1).operations[0].assertions = []; }],
    ['missing buffer limitation', d => { d.reviews.at(-1).operations[0].remaining = []; }]
]) {
    test(`rejects ${name}`, () => {
        const changed = structuredClone(ledger);
        mutate(changed);
        assert.throws(() => checkObligations(inventory, changed, read));
    });
}
