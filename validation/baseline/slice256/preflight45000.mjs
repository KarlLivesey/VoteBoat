// SPDX-License-Identifier: RPL-1.5
import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import dgram from 'node:dgram';
import {spawn} from 'node:child_process';
import {setTimeout as pause} from 'node:timers/promises';

const base=45000, mac='karllivesey@192.168.192.76';
const local=process.cwd(), evidence=`${local}/target/slice256`;
const remote='/tmp/voteboat-macos-CzK1if';
const binary=`${local}/target/debug/voteboat-counter`;
const macBinary=`${remote}/repo254e/target/debug/voteboat-counter`;
const root='/tmp/voteboat-cross-host-256';
const quote=s=>`'${String(s).replaceAll("'", "'\\''")}'`;
const sshArgs=['-o','BatchMode=yes','-o','ConnectTimeout=10',mac];
const transcript=fs.openSync(`${evidence}/commands.log`,'wx');
function record(event) {fs.writeSync(transcript,`${new Date().toISOString()} ${JSON.stringify(event)}\n`);}
async function execute(file,args,timeout=15000) {
  const child=spawn(file,args,{stdio:['ignore','pipe','pipe']});
  let out='',err='';
  child.stdout.on('data',b=>out+=b);child.stderr.on('data',b=>err+=b);
  const timer=setTimeout(()=>child.kill('SIGKILL'),timeout);
  const result=await new Promise((resolve,reject)=>{
    child.on('error',reject);child.on('close',(code,signal)=>resolve({code,signal,out,err}));
  }).finally(()=>clearTimeout(timer));
  record({file,args,...result});return result;
}
async function remoteCommand(command) {return execute('ssh',[...sshArgs,command],20000);}
async function probe(tcp,udp) {
  const sockets=[];
  try {
    for(const [host,port] of tcp) {
      const s=net.createServer();sockets.push(s);
      await new Promise((resolve,reject)=>{s.once('error',reject);s.listen(port,host,resolve);});
    }
    for(const [host,port] of udp) {
      const s=dgram.createSocket('udp4');sockets.push(s);
      await new Promise((resolve,reject)=>{s.once('error',reject);s.bind(port,host,resolve);});
    }
  } finally {await Promise.all(sockets.map(s=>new Promise(resolve=>{try{s.close(resolve);}catch{resolve();}})));}
}
async function probePorts() {
  await probe([['192.168.223.133',base+1],['192.168.223.133',base+2],['127.0.0.1',base+101],['127.0.0.1',base+102]], [['192.168.223.133',base+1],['192.168.223.133',base+2]]);
  const code=`const net=require('node:net'),dgram=require('node:dgram'); const tcp=[['192.168.192.76',${base+3}],['127.0.0.1',${base+103}]]; const held=[];(async()=>{for(const [host,port] of tcp){const s=net.createServer();held.push(s);await new Promise((ok,no)=>{s.once('error',no);s.listen(port,host,ok);});}const u=dgram.createSocket('udp4');held.push(u);await new Promise((ok,no)=>{u.once('error',no);u.bind(${base+3},'192.168.192.76',ok);});for(const s of held)s.close();})().catch(e=>{console.error(e);process.exit(1);});`;
  const r=await remoteCommand(`node -e ${quote(code)}`);assert.equal(r.code,0);
}
const live=new Map();let sequence=0;
async function start(protocol,id,mode) {
  assert(!live.has(id));
  const store=id===3?`${remote}/slice256/${protocol}-store3`:`${root}/${protocol}-${id}`;
  const args=['serve',mode,store,String(id),String(base),id===3?`${remote}/repo254e/tests/fixtures/tls`:`${local}/tests/fixtures/tls`,id===3?`${remote}/slice256/peers.txt`:`${evidence}/peers.txt`,'--transport',protocol];
  const log=`${evidence}/${protocol}-${id}-${mode}-${++sequence}.log`;
  const fd=fs.openSync(log,'wx');
  const pidFile=`${remote}/slice256/${protocol}-node3.pid`;
  const expected=[macBinary,...args].join(' ');
  const child=id===3?spawn('ssh',[...sshArgs,`printf '%s\n' "$$" >${quote(pidFile)}; exec ${[macBinary,...args].map(quote).join(' ')}`],{stdio:['ignore',fd,fd]}):spawn(binary,args,{stdio:['ignore',fd,fd]});
  fs.closeSync(fd);
  const state={child,log,pidFile,expected,result:null};
  state.closed=new Promise((resolve,reject)=>{child.on('error',reject);child.on('close',(code,signal)=>{state.result={code,signal};record({exit:id,protocol,mode,...state.result});resolve(state.result);});});
  live.set(id,state);record({start:id,protocol,mode,store,pid:child.pid,log});
  const deadline=Date.now()+15000;
  while(!fs.readFileSync(log,'utf8').includes(`ready node=${id} `)) {
    assert(!state.result,`node ${id} exited: ${fs.readFileSync(log,'utf8')}`);
    assert(Date.now()<deadline,`node ${id} not ready`);await pause(50);
  }
}
async function request(id,...args) {
  const words=['client',String(base),String(id),...args];
  return id===3?remoteCommand([macBinary,...words].map(quote).join(' ')):execute(binary,words);
}
async function leader(ids=[...live.keys()]) {
  const deadline=Date.now()+30000;
  do {
    for(const id of ids) {
      const r=await request(id,'status');if(r.code===0&&r.out.includes('role=Leader'))return id;
    }
    await pause(100);
  }while(Date.now()<deadline);
  throw new Error(`no leader among ${ids}`);
}
async function add(operation,delta,value,duplicate) {
  let uncertain=false;
  for(let attempt=0;attempt<4;attempt++) {
    const id=await leader(),r=await request(id,'add',String(operation),String(delta));
    if(r.code===0) {
      assert(r.out.includes(`outcome=Value(${value})`),r.out);
      if(duplicate!==null)assert(r.out.includes(`duplicate=${duplicate}`),r.out);
      else assert(r.out.includes('duplicate=false')||(uncertain&&r.out.includes('duplicate=true')),r.out);
      return;
    }
    const unknown=r.out.startsWith('UNKNOWN ')||r.err.includes('write outcome unknown');
    assert(unknown||r.out==='ERR NOT_LEADER\n',JSON.stringify(r));
    uncertain ||= unknown;
  }
  throw new Error(`operation ${operation} exhausted same-ID retries`);
}
async function read(value) {
  for(let attempt=0;attempt<4;attempt++) {
    const r=await request(await leader(),'read');
    if(r.code===0){assert.equal(r.out.trim(),`OK value=${value}`);return;}
    assert(/NOT_LEADER|ReadNotReady|LeadershipChanged/.test(r.out),JSON.stringify(r));
  }
  throw new Error('read retry bound');
}
async function exited(id,success) {
  const state=live.get(id);assert(state);
  let timer;
  const r=await Promise.race([state.closed,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(`node ${id} failed to exit`)),15000);})]).finally(()=>clearTimeout(timer));
  if(success)assert.equal(r.code,0,JSON.stringify(r));
  if(id===3) {
    const r=await remoteCommand(`vb_pid=$(cat ${quote(state.pidFile)}); if kill -0 "$vb_pid" 2>/dev/null; then exit 1; fi`);
    assert.equal(r.code,0,'remote process remains live');
  }
  live.delete(id);
}
async function kill(id) {
  const state=live.get(id);assert(state&&!state.result);
  if(id===3) {
    const r=await remoteCommand(`set -e; vb_pid=$(cat ${quote(state.pidFile)}); vb_command=$(ps -p "$vb_pid" -o command=); test "$vb_command" = ${quote(state.expected)}; kill -KILL "$vb_pid"`);
    assert.equal(r.code,0,'remote PID/command ownership mismatch');
  }else assert(state.child.kill('SIGKILL'));
  await exited(id,false);
}
async function quitAll() {
  for(const id of [...live.keys()]){const r=await request(id,'quit');assert.equal(r.code,0,JSON.stringify(r));}
  for(const id of [...live.keys()])await exited(id,true);
}
try {
  for(const protocol of ['tcp','quic']) {
    record({protocol,phase:'start'});await probePorts();
    for(const id of [1,2,3])await start(protocol,id,'create');
    await add(25601,7,7,null);await add(25601,7,7,true);await read(7);
    // Removing one Linux voter requires a real Linux/Mac quorum for this write.
    await kill(1);await add(25602,5,12,null);await read(12);
    await start(protocol,1,'recover');
    const lostLeader=await leader();await kill(lostLeader);
    await add(25603,3,15,null);await read(15);await start(protocol,lostLeader,'recover');
    for(const id of [1,2,3]){const r=await request(id,'checkpoint');assert.equal(r.code,0);}
    await quitAll();
    for(const id of [1,2,3])await start(protocol,id,'recover');
    await add(25601,7,7,true);await add(25602,5,12,true);await add(25603,3,15,true);await read(15);
    await add(25604,2,17,null);await read(17);await quitAll();
    record({protocol,phase:'accepted',final:17});
  }
  fs.writeFileSync(`${evidence}/result.json`,JSON.stringify({accepted:['tcp','quic'],final:17},null,2)+'\n');
}catch(error) {
  record({failure:error.stack});
  for(const id of [...live.keys()]) {
    try {const r=await request(id,'quit');if(r.code===0)await exited(id,true);else await kill(id);}
    catch(cleanup){record({cleanup:id,error:cleanup.stack});}
  }
  process.exitCode=1;
}finally{fs.closeSync(transcript);}
