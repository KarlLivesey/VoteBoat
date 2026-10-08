// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Repository metadata validation only; not component or consensus conformance.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const source = process.argv[2] ?? path.join(root, 'docs/component-contracts.json');
const inventory = JSON.parse(fs.readFileSync(source, 'utf8'));
assert.equal(inventory.version, 1);
assert.ok(Array.isArray(inventory.contracts) && inventory.contracts.length > 0);
assert.ok(Array.isArray(inventory.not_yet_implemented));
assert.ok(inventory.not_yet_implemented.every(item => typeof item === 'string' && item.length > 0),
    'not_yet_implemented must contain names, not implemented contract records');
assert.equal(new Set(inventory.not_yet_implemented).size, inventory.not_yet_implemented.length);
const names = new Set();
for (const contract of inventory.contracts) {
    assert.ok(contract && typeof contract === 'object' && !Array.isArray(contract));
    for (const key of ['design_id', 'public_contract', 'native', 'scope']) {
        assert.ok(typeof contract[key] === 'string' && contract[key].length > 0, key);
    }
    assert.ok(!names.has(contract.public_contract), 'duplicate contract: ' + contract.public_contract);
    names.add(contract.public_contract);
    for (const key of ['operations', 'conformance']) {
        assert.ok(Array.isArray(contract[key]) && contract[key].length > 0, key);
        assert.ok(contract[key].every(value => typeof value === 'string' && value.length > 0), key);
    }
    for (const file of contract.conformance) {
        assert.ok(fs.statSync(path.resolve(root, file)).isFile(), 'conformance file: ' + file);
    }
}
console.log(`${names.size} implemented contracts: inventory shape and conformance paths pass.`);
