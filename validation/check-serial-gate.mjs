// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Fixed reference-workload performance gate; not a consensus or capacity proof.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';

export const P99_BUDGET_US = 250000;
export function verifyContext(context, summaryHash, samplesHash) {
    assert.equal(context.schema, 1);
    assert.equal(context.mode, 'disk_reference');
    assert.ok(['btrfs', 'ext4', 'xfs', 'apfs', 'hfs'].includes(context.filesystem),
        'reference provenance must identify a supported disk filesystem');
    assert.ok(typeof context.root === 'string');
    if (context.root_path_kind === 'workspace_relative') {
        assert.match(context.root, /^target\/bench-[a-z0-9-]+$/,
            'published relative roots must name a benchmark directory without traversal');
    } else {
        assert.equal(context.root_path_kind, undefined);
        assert.ok(path.isAbsolute(context.root));
    }
    assert.match(context.source_revision, /^[a-f0-9]{40}$/);
    assert.match(context.executable_sha256, /^[a-f0-9]{64}$/);
    assert.equal(context.summary_sha256, summaryHash, 'summary does not match recorded context');
    assert.equal(context.samples_sha256, samplesHash, 'samples do not match recorded context');
}
export function classifySerial(summary) {
    assert.equal(summary.protocol, 'TcpTls', 'reference transport must be TCP/TLS');
    assert.equal(summary.assembly, 'startup', 'reference uses native startup assembly');
    assert.equal(summary.journal_timings, undefined, 'instrumented diagnostic is not the uninstrumented acceptance reference');
    assert.equal(summary.mode, undefined, 'offered/other modes cannot replace the serial gate');
    assert.equal(summary.leader_placement, '1:1', 'reference starts with replica 1 leading');
    const integer = (key, expected) => {
        assert.ok(/^\d+$/.test(String(summary[key])), `missing/invalid ${key}`);
        const value = Number(summary[key]);
        assert.ok(Number.isSafeInteger(value));
        assert.equal(value, expected, `reference ${key} differs`);
    };
    for (const [key, value] of Object.entries({
        replicas: 3, groups: 1, payload_bytes: 8, warmup: 64, operations: 256,
        window: 1, max_inflight: 1, recovered_value: 320,
        wal_workers_per_replica: 1, snapshot_workers_per_replica: 1,
        peer_endpoints_per_replica: 1, heartbeat_ms: 50,
        election_min_ms: 1000, election_spread_ms: 1000,
    })) integer(key, value);
    assert.equal(summary.retry_verified, 'true', 'original retry verification required');
    assert.equal(summary.workers_joined, 'true', 'complete worker joins required');
    const number = key => {
        assert.ok(/^\d+(?:\.\d+)?$/.test(String(summary[key])), `missing/invalid ${key}`);
        const value = Number(summary[key]);
        assert.ok(Number.isFinite(value) && value > 0, `nonpositive ${key}`);
        return value;
    };
    const elapsed = number('elapsed_s');
    const rate = number('applied_ops_s');
    assert.ok(Math.abs(256 / elapsed - rate) < 0.001, 'rate differs from measured duration');
    const p50 = number('p50_us'), p95 = number('p95_us'), p99 = number('p99_us'), max = number('max_us');
    assert.ok(p50 <= p95 && p95 <= p99 && p99 <= max, 'percentile ordering invalid');
    return Object.freeze({
        gate: 'tcp-startup-serial-p99', budget_us: P99_BUDGET_US,
        observed_p99_us: p99, applied_ops_s: rate, operations: 256,
        passed: p99 <= P99_BUDGET_US,
        evidence_scope: 'finite reference run; raw checker and native recovery gates required',
    });
}
function main() {
    assert.equal(process.argv.length, 3, 'usage: node validation/check-serial-gate.mjs RUN_DIRECTORY_OR_ARCHIVED_PREFIX');
    const input = path.resolve(process.argv[2]);
    const checker = fileURLToPath(new URL('./check-native-benchmark.mjs', import.meta.url));
    // No shell or optional bypass: validate raw receipts and recovery flags first.
    const raw = spawnSync(process.execPath, [checker, input], { encoding: 'utf8', maxBuffer: 1024 * 1024 });
    assert.ifError(raw.error);
    assert.equal(raw.status, 0, `raw result validation failed: ${raw.stderr ?? ''}`);
    process.stderr.write(raw.stdout);
    const summaryPath = fs.existsSync(path.join(input, 'summary.txt')) ? path.join(input, 'summary.txt') : `${input}.txt`;
    const directory = fs.existsSync(path.join(input, 'summary.txt'));
    const samplesPath = directory ? path.join(input, 'samples.csv') : `${input}.csv`;
    const contextPath = directory ? path.join(input, 'gate-context.json') : `${input}.context.json`;
    const hash = p => createHash('sha256').update(fs.readFileSync(p)).digest('hex');
    // Captured host provenance is trusted input, not proof of device power-loss behavior.
    verifyContext(JSON.parse(fs.readFileSync(contextPath, 'utf8')), hash(summaryPath), hash(samplesPath));
    const tokens = fs.readFileSync(summaryPath, 'utf8').trim().split(/\s+/);
    const summary = Object.create(null);
    for (const token of tokens) {
        const parts = token.split('=');
        assert.equal(parts.length, 2, 'malformed summary token');
        assert.ok(parts[0] && parts[1] && !Object.hasOwn(summary, parts[0]), 'empty/duplicate summary field');
        summary[parts[0]] = parts[1];
    }
    const result = classifySerial(summary);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    if (!result.passed) process.exitCode = 1;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
