// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import { validateRoundedRate } from './check-rounded-rate.mjs';

// Actual successful Linux readiness output rejected by the former fixed tolerance.
validateRoundedRate(128, '0.104657', '1223.047');
validateRoundedRate(15000, '80.000000', '187.500');
validateRoundedRate(1000000000, '1.000000', '1000000000.000');
for (const [count, elapsed, rate] of [
    [128, '0.104657', '1223.060'],
    [128, '0.104650', '1223.047'],
    [15000, '80.000000', '187.501'],
    [0, '1.000000', '1.000'],
    [1, '0.000000', '1.000'],
    [1, '1.000000', '0.000'],
    [1, 'NaN', '1.000'],
    [1, '1.000000', 'Infinity'],
    [1, '-1.000000', '1.000'],
    [1, '1.0', '1.000'],
]) {
    assert.throws(() => validateRoundedRate(count, elapsed, rate));
}
console.log('Printed-rate precision and incompatible-output controls pass');
