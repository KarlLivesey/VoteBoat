import fs from 'node:fs';
import path from 'node:path';
import {checkObligations} from '../../../validation/check-provider-conformance.mjs';
const root = path.resolve('target/p9/slice428/source');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
console.log(JSON.stringify(checkObligations(JSON.parse(read('docs/component-contracts.json')), JSON.parse(read('docs/provider-conformance.json')), read), null, 2));
