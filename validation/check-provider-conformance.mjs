// SPDX-License-Identifier: RPL-1.5
// Metadata integrity only. Named assertions do not prove full conformance.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const text = value => typeof value === 'string' && value.trim().length > 0;

function anchor(reference, read, runner) {
    assert.ok(reference && text(reference.file) && text(reference.symbol), 'assertion reference');
    assert.match(reference.file, /^tests\/[A-Za-z0-9_/-]+\.rs$/, 'local Rust test path');
    assert.match(reference.symbol, /^[A-Za-z_][A-Za-z0-9_]*$/, 'Rust function symbol');
    const source = read(reference.file);
    const name = reference.symbol;
    assert.ok(new RegExp(`\\bfn\\s+${name}\\s*(?:<|\\()`).test(source), `missing function ${name}`);
    if (runner) {
        assert.ok(new RegExp(`#\\[test\\]\\s*(?:pub\\s+)?fn\\s+${name}\\s*\\(`).test(source), `missing test ${name}`);
    }
}

export function checkObligations(inventory, ledger, read) {
    assert.equal(ledger.version, 1, 'ledger version');
    assert.ok(text(ledger.meaning), 'evidence boundary');
    assert.ok(Array.isArray(ledger.reviews) && ledger.reviews.length > 0, 'reviews');
    const contracts = new Map(inventory.contracts.map(c => [c.public_contract, c]));
    const reviewed = new Set();
    let operations = 0;
    for (const review of ledger.reviews) {
        const contract = contracts.get(review.public_contract);
        assert.ok(contract, `unknown contract ${review.public_contract}`);
        assert.ok(!reviewed.has(review.public_contract), 'duplicate review');
        reviewed.add(review.public_contract);
        assert.equal(review.status, 'partial', 'selected assertions are not certification');
        assert.ok(Array.isArray(review.runners) && review.runners.length > 0, 'runners');
        review.runners.forEach(r => anchor(r, read, true));
        assert.ok(Array.isArray(review.operations) && review.operations.length > 0, 'operations');
        const seen = new Set();
        for (const row of review.operations) {
            assert.ok(Array.isArray(row.names) && row.names.length > 0, 'operation names');
            for (const name of row.names) {
                assert.ok(contract.operations.includes(name), `unknown operation ${name}`);
                assert.ok(!seen.has(name), `duplicate operation ${name}`);
                seen.add(name);
            }
            assert.ok(text(row.obligation), 'obligation');
            assert.ok(Array.isArray(row.assertions) && row.assertions.length > 0, 'assertions');
            row.assertions.forEach(r => anchor(r, read, false));
            assert.ok(Array.isArray(row.remaining) && row.remaining.length > 0 && row.remaining.every(text), 'remaining scope');
        }
        assert.equal(seen.size, new Set(contract.operations).size, `incomplete operation review ${review.public_contract}`);
        operations += seen.size;
    }
    return {
        reviewed_contracts: reviewed.size,
        reviewed_operations: operations,
        unreviewed_contracts: [...contracts.keys()].filter(name => !reviewed.has(name)),
        note: 'Metadata references only; execute the named tests and inspect their scope separately.'
    };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    const inventory = JSON.parse(fs.readFileSync(path.join(root, 'docs/component-contracts.json'), 'utf8'));
    const ledger = JSON.parse(fs.readFileSync(path.join(root, 'docs/provider-conformance.json'), 'utf8'));
    const report = checkObligations(inventory, ledger, file => fs.readFileSync(path.join(root, file), 'utf8'));
    console.log(JSON.stringify(report, null, 2));
}
