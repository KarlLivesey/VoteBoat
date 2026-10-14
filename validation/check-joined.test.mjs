// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
import assert from 'node:assert/strict';
import { validateJoined } from './check-joined.mjs';
const summary = {ready_reply_inputs_effective: '2', ready_reply_inputs_requested: '2', ready_commit_batch_capable: 'true', warmup: '64', offers: '100', joined_transitions: '3', joined_reply_inputs: '5', joined_originals: '10'};
const header = 'phase,replica,joined_transitions,joined_reply_inputs,joined_originals\n';
const rows = ['warmup,1,0,0,0', 'warmup,2,0,0,0', 'warmup,3,1,2,4', 'measurement,1,2,3,6', 'measurement,2,1,2,4', 'measurement,3,0,0,0'];
const csv = header + rows.join('\n');
assert.deepEqual(validateJoined(summary, csv).measurement, [3, 5, 10]);
for (const [edited, data] of [
    [{...summary, ready_reply_inputs_effective: '0'}, csv],
    [{...summary, joined_originals: '9'}, csv],
    [summary, csv.replace('measurement,2,1,2,4', 'measurement,1,1,2,4')],
    [summary, csv.replace('measurement,2,1,2,4', 'measurement,2,1,3,4')],
    [summary, csv.replace('measurement,3,0,0,0', 'measurement,3,0,0,1')],
    [summary, header + rows.slice(1).join('\n')],
]) assert.throws(() => validateJoined(edited, data));
const zeros = header + rows.map(line => line.split(',').slice(0, 2).concat(['0', '0', '0']).join(',')).join('\n');
validateJoined({...summary, ready_reply_inputs_effective: '0', ready_reply_inputs_requested: '0', joined_transitions: '0', joined_reply_inputs: '0', joined_originals: '0'}, zeros);
console.log('joined counter positive/disabled and six rejection controls pass');
