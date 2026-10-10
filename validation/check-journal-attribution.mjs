// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Offline original-receipt mapping, not synchronization or latency proof.
import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';

const receiptHeader = 'operation,submitted_ns,completed_ns,latency_ns,applied_index,value,group';
const journalHeader = 'store,store_incarnation,group,group_incarnation,batch,offset,frame_bytes,class,event,operation,index,term,delta,commit_before,commit_after,revision,generation,appended_entries,appended_commands,committed_commands,hard_state_changed';
const digest = text => createHash('sha256').update(text).digest('hex');
const integer = (text, positive = false) => {
    assert.match(text, /^(0|[1-9][0-9]*)$/);
    const value = BigInt(text);
    assert.ok(!positive || value > 0n);
    return value;
};
function rows(text, expected) {
    assert.ok(Buffer.byteLength(text) <= 8 * 1024 * 1024, 'CSV byte budget');
    const lines = text.trimEnd().split('\n');
    assert.equal(lines.shift(), expected, 'CSV header');
    assert.ok(lines.length > 0 && lines.length <= 24576, 'CSV row budget');
    const fields = expected.split(',');
    return lines.map(line => {
        const values = line.split(',');
        assert.equal(values.length, fields.length, 'CSV row width');
        return Object.fromEntries(fields.map((field, i) => [field, values[i]]));
    });
}

function receipts(text) {
    const result = rows(text, receiptHeader);
    assert.ok(result.length <= 2048);
    const operations = new Set(), positions = new Set();
    for (const row of result) {
        for (const field of receiptHeader.split(',')) integer(row[field]);
        assert.ok(integer(row.operation, true) < (1n << 128n));
        assert.ok(integer(row.applied_index, true) <= 2048n);
        assert.equal(row.group, '1');
        assert.equal(row.value, row.operation, 'original serial counter value');
        assert.equal(integer(row.completed_ns) - integer(row.submitted_ns), integer(row.latency_ns));
        assert.ok(!operations.has(row.operation) && !positions.has(row.applied_index), 'duplicate receipt');
        operations.add(row.operation); positions.add(row.applied_index);
    }
    return result;
}

function journal(text, store) {
    const input = rows(text, journalHeader), batches = [], append = [], commit = [];
    let current, offset = 0n, prefix = 0n, generation = 0n, committedPosition = 0n;
    for (const row of input) {
        assert.equal(row.store, store, 'wrong replica');
        for (const field of ['store_incarnation', 'group', 'group_incarnation']) assert.equal(row[field], '1');
        for (const field of ['batch', 'frame_bytes', 'revision', 'generation']) integer(row[field], true);
        for (const field of ['offset', 'commit_before', 'commit_after', 'appended_entries', 'appended_commands', 'committed_commands']) integer(row[field]);
        assert.ok(['true', 'false'].includes(row.hard_state_changed));
        if (row.event === 'batch') {
            assert.equal(integer(row.batch), BigInt(batches.length + 1));
            assert.equal(row.revision, row.batch, 'single-group revision');
            assert.equal(integer(row.offset), offset, 'physical offset discontinuity');
            assert.equal(integer(row.commit_before), prefix, 'commit prefix discontinuity');
            prefix = integer(row.commit_after);
            assert.ok(prefix >= integer(row.commit_before) && prefix <= 2048n);
            assert.ok(integer(row.generation) >= generation, 'generation regression');
            generation = integer(row.generation);
            assert.ok(integer(row.frame_bytes) <= 1024n * 1024n);
            offset += integer(row.frame_bytes);
            assert.ok(offset <= 8n * 1024n * 1024n);
            for (const field of ['operation', 'index', 'term', 'delta']) assert.equal(row[field], '');
            current = { row, append: [], commit: [] };
            batches.push(current);
        } else {
            assert.ok(current && ['append', 'commit'].includes(row.event), 'event without physical batch');
            for (const field of journalHeader.split(',')) {
                if (!['event', 'operation', 'index', 'term', 'delta'].includes(field)) {
                    assert.equal(row[field], current.row[field], 'event batch metadata');
                }
            }
            assert.ok(integer(row.operation, true) < (1n << 128n));
            assert.ok(integer(row.index, true) <= 2048n);
            integer(row.term, true);
            assert.equal(row.delta, '1', 'original benchmark delta');
            if (row.event === 'commit') {
                assert.ok(integer(row.index) > integer(row.commit_before) && integer(row.index) <= integer(row.commit_after));
                assert.ok(integer(row.index) > committedPosition, 'committed position repeated/out of order');
                committedPosition = integer(row.index);
                commit.push(row); current.commit.push(row);
            } else { append.push(row); current.append.push(row); }
        }
    }
    assert.ok(batches.length <= 8192 && append.length + commit.length <= 16384);
    const classes = {};
    for (const batch of batches) {
        const row = batch.row;
        assert.equal(integer(row.appended_commands), BigInt(batch.append.length));
        assert.equal(integer(row.committed_commands), BigInt(batch.commit.length));
        assert.ok(integer(row.appended_entries) >= integer(row.appended_commands));
        const expected = row.batch === '1' ? 'bootstrap' : batch.append.length ?
            (batch.commit.length ? 'combined' : 'append_only') : batch.commit.length ? 'commit_only' : 'control';
        assert.equal(row.class, expected, 'physical classification');
        classes[expected] = (classes[expected] ?? 0) + 1;
    }
    return { batches, append, commit, classes, wal_bytes: offset.toString(), final_commit: prefix.toString() };
}

export function verifyAttribution(receiptText, journalTexts) {
    assert.equal(journalTexts.length, 3, 'three replica journals required');
    const original = receipts(receiptText);
    const journals = journalTexts.map((text, i) => journal(text, String(i + 1)));
    const matches = original.map(receipt => ({
        operation: receipt.operation, index: receipt.applied_index, group: receipt.group,
        recorded_latency_ns: receipt.latency_ns,
        replicas: journals.map((source, i) => {
            const committed = source.commit.filter(row => row.operation === receipt.operation && row.index === receipt.applied_index);
            assert.equal(committed.length, 1, `original committed position missing/duplicated: ${receipt.operation}/${receipt.applied_index}`);
            const end = committed[0];
            const appended = source.append.filter(row => row.operation === end.operation && row.index === end.index &&
                row.term === end.term && row.delta === end.delta && integer(row.batch) <= integer(end.batch));
            assert.ok(appended.length > 0, 'original append missing');
            const start = appended.at(-1);
            assert.ok(integer(start.generation) <= integer(end.generation));
            return { store: String(i + 1), append_batch: start.batch, commit_batch: end.batch,
                append_generation: start.generation, commit_generation: end.generation,
                term: end.term, append_class: start.class, commit_class: end.class,
                distinct_physical_records: start.batch === end.batch ? 1 : 2 };
        }),
    }));
    return {
        schema: 1, scope: 'offline_original_receipt_positions', synchronization_measured: false,
        receipts_sha256: digest(receiptText), receipts: original.length,
        replicas: journals.map((source, i) => {
            const selected = matches.map(row => row.replicas[i]);
            const selectedBatches = new Set(selected.flatMap(row => [row.append_batch, row.commit_batch]));
            const distribution = {};
            for (const row of selected) distribution[row.distinct_physical_records] = (distribution[row.distinct_physical_records] ?? 0) + 1;
            return { store: String(i + 1), csv_sha256: digest(journalTexts[i]), frames: source.batches.length,
                wal_bytes: source.wal_bytes, final_commit: source.final_commit, classes: source.classes,
                original_receipts_matched: selected.length, selected_physical_batches: selectedBatches.size,
                records_per_original_receipt: distribution };
        }), matches,
    };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    try {
        const [receiptFile, ...journalFiles] = process.argv.slice(2);
        assert.ok(receiptFile && journalFiles.length === 3,
            'usage: check-journal-attribution RECEIPTS_CSV REPLICA1_CSV REPLICA2_CSV REPLICA3_CSV');
        const read = file => {
            assert.ok(fs.statSync(file).size <= 8 * 1024 * 1024, 'CSV byte budget');
            return fs.readFileSync(file, 'utf8');
        };
        console.log(JSON.stringify(verifyAttribution(read(receiptFile), journalFiles.map(read)), null, 2));
    } catch (error) { console.error(error.message); process.exitCode = 1; }
}
