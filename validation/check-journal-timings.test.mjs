// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { validateJournal } from './check-journal-timings.mjs';
const csv=fs.readFileSync(new URL('./performance/slice138/tcp-diagnostic.journal.csv',import.meta.url),'utf8');
function mutate(field,value) {
    const lines=csv.trimEnd().split('\n'),headers=lines[0].split(',');
    const row=lines[4].split(',');row[headers.indexOf(field)]=value;lines[4]=row.join(',');return lines.join('\n')+'\n';
}
test('actual completed journal diagnostic has three finite per-replica deltas',()=>{
    const result=validateJournal(csv);assert.equal(result.measurement_deltas.length,3);
    assert.equal(result.measurement_deltas[0].sync_calls,'512');
});
test('missing or duplicate replica stage cannot certify a complete diagnostic',()=>{
    const lines=csv.trimEnd().split('\n');assert.throws(()=>validateJournal(lines.slice(0,-1).join('\n')));
    assert.throws(()=>validateJournal(csv+lines[1]+'\n'));
    assert.throws(()=>validateJournal(mutate('stage','measurement_failure')));
});
test('regression, errors, malformed values and fabricated logical metrics are refused',()=>{
    for(const [field,value] of [['sync_calls','0'],['sync_ns','0'],['publish_ns','NaN'],['errors','1'],['append_commands','256'],['replica','4']]) {
        assert.throws(()=>validateJournal(mutate(field,value)),field);
    }
});
