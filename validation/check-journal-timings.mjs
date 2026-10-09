// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Native file-call diagnostics only; not a durability or critical-path proof.
import fs from 'node:fs';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
export function validateJournal(csv) {
const lines = csv.trimEnd().split('\n');
const fields = lines.shift().split(',');
assert.equal(new Set(fields).size,fields.length);
for(const name of ['stage','replica','io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns','errors']) assert.ok(fields.includes(name));
const stages=new Map();
for(const line of lines) {
    const values=line.split(',');assert.equal(values.length,fields.length);
    const row=Object.fromEntries(fields.map((f,i)=>[f,values[i]]));
    assert.ok(['before_measurement','after_measurement','after_create_join','after_recover_join'].includes(row.stage));
    assert.ok(['1','2','3'].includes(row.replica));
    const rows=stages.get(row.stage)??new Map();assert.ok(!rows.has(row.replica));rows.set(row.replica,row);stages.set(row.stage,rows);
    for(const name of ['io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns','errors']) assert.match(row[name],/^\d+$/);
    assert.equal(row.errors,'0','successful diagnostic must have no recorded file errors');
    // Native startup observes JournalIo only; do not fabricate LogStore metrics.
    for(const name of ['append_calls','append_units','append_commands','barrier_calls','barrier_tickets','encoded_bytes']) assert.equal(row[name],'0');
}
assert.equal(stages.size,4);
for(const rows of stages.values()) assert.equal(rows.size,3);
const deltas=[];
for(const replica of ['1','2','3']) {
    const before=stages.get('before_measurement').get(replica),after=stages.get('after_measurement').get(replica),joined=stages.get('after_create_join').get(replica);
    const delta={replica};
    for(const name of ['io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']) {
        const b=BigInt(before[name]),a=BigInt(after[name]),j=BigInt(joined[name]);assert.ok(b<=a&&a<=j,`${name} regressed before join`);
        delta[name]=(a-b).toString();
    }
    assert.ok(BigInt(delta.sync_calls)>0n && BigInt(delta.publish_calls)>0n);
    deltas.push(delta);
}
return {scope:'finite completed native startup diagnostic; parallel durations overlap; not critical-path proof',measurement_deltas:deltas};
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    assert.equal(process.argv.length,3,'usage: node check-journal-timings.mjs JOURNAL_CSV');
    process.stdout.write(JSON.stringify(validateJournal(fs.readFileSync(process.argv[2],'utf8')),null,2)+'\n');
}
