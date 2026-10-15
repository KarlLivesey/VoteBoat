// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';

export function validateRoundedRate(count, elapsedText, rateText) {
    assert.ok(Number.isSafeInteger(count) && count > 0);
    assert.match(elapsedText, /^\d+\.\d{6}$/);
    assert.match(rateText, /^\d+\.\d{3}$/);
    const elapsed = BigInt(elapsedText.replace('.', ''));
    const rate = BigInt(rateText.replace('.', ''));
    assert.ok(elapsed > 0n && rate > 0n);
    // Intersect count/time with the half-unit intervals of both printed values.
    const scaledCount = BigInt(count) * 4_000_000_000n;
    assert.ok(scaledCount >= (2n * rate - 1n) * (2n * elapsed - 1n));
    assert.ok(scaledCount <= (2n * rate + 1n) * (2n * elapsed + 1n));
}
