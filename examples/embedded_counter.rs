// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Minimal Rust host using NativeStartup and the same facade as voteboat-counter.
//! A standalone single-voter example; use the service quickstart for replication.
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::*,
        startup::*,
        tls::*,
        worker::*,
    },
    quorum::*,
    runtime::*,
};
type Failure = Box<dyn std::error::Error>;
fn check<T, E: std::fmt::Debug>(v: Result<T, E>) -> Result<T, Failure> {
    v.map_err(|e| format!("{e:?}").into())
}
fn material(path: &Path) -> Result<Vec<u8>, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(65537)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > 65536 {
        return Err("invalid credential size".into());
    }
    Ok(bytes)
}
fn drive(
    n: &mut NativeNode<Counter, NativeServiceConnector>,
    start: Instant,
    mut done: impl FnMut(&mut NativeNode<Counter, NativeServiceConnector>) -> Result<bool, Failure>,
) -> Result<(), Failure> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        check(n.poll(
            MonoTime(start.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        ))?;
        if done(n)? {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("progress timed out; recover authoritative files before retry".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, root, tls, operation, delta] = args.as_slice() else {
        return Err(
            "usage: embedded_counter create|recover DIRECTORY TLS_DIRECTORY OPERATION_ID DELTA"
                .into(),
        );
    };
    let mode = match mode.as_str() {
        "create" => NativeOpenMode::Create,
        "recover" => NativeOpenMode::Recover,
        _ => return Err("expected create or recover".into()),
    };
    let operation = OperationId::new(operation.parse()?).ok_or("nonzero operation ID required")?;
    let delta: i64 = delta.parse()?;
    let tls = Path::new(tls);
    let config = startup(root, tls, mode)?;
    let group = config.bootstrap.group;
    let mut node = match config.open_with_protocol_and_timers(
        NativePeerProtocol::TcpTls,
        NativeTimingProfile::Throughput.timers(),
        check(Counter::new(10000))?,
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    ) {
        Ok(n) => n,
        Err(mut rejected) => {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !rejected.try_cleanup()? {
                if Instant::now() > deadline {
                    return Err("startup cleanup timed out".into());
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
            return Err(rejected.reason.into());
        }
    };
    let start = Instant::now();
    check(node.control(group, NodeControl::Campaign))?;
    drive(&mut node, start, |n| {
        let core = n.local().owner.core(group).ok_or("missing core")?;
        Ok(core.role() == voteboat::raft::Role::Leader
            && !core.has_pending_dependency()
            && n.local().applications[&group].applied_index() == core.state().commit_index)
    })?;
    let ticket = check(node.propose(ClientRequest {
        group,
        operation,
        bytes: delta.to_le_bytes().to_vec(),
    }))?;
    drive(&mut node, start, |n| {
        let Some(output) = n.poll_client() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("unexpected client ticket".into());
        }
        let outcome = check(n.complete_client(output).map_err(|r| r.reason))?;
        match outcome {
            ClientOutcome::Applied { receipt, .. } => println!(
                "outcome={:?} duplicate={}",
                receipt.outcome, receipt.duplicate
            ),
            other => return Err(format!("{other:?}").into()),
        }
        Ok(true)
    })?;
    let ticket = check(node.read(group, ()))?;
    drive(&mut node, start, |n| {
        let Some(output) = n.poll_read() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("unexpected read ticket".into());
        }
        match check(n.complete_read(output).map_err(|r| r.reason))? {
            ReadOutcome::Read {
                result: Ok(value), ..
            } => println!("linearizable_value={value}"),
            other => return Err(format!("{other:?}").into()),
        }
        Ok(true)
    })?;
    close(node, group, start)
}

fn startup(root: &str, tls: &Path, mode: NativeOpenMode) -> Result<NativeStartup, Failure> {
    let node_id = NodeId::new(1).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    Ok(NativeStartup {
        directory: root.into(),
        mode,
        node: node_id,
        store,
        bootstrap: Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: check(Policy::new(Tree::Voter(node_id), Limits::default()))?,
            voter_stores: [(node_id, store)].into(),
        },
        listen: "127.0.0.1:0".parse()?,
        peers: BTreeMap::new(),
        entropy_seed: 17,
        limits: NodeLimits::default(),
        tls: check(NativeTlsConfig::new(TlsCredentials {
            roots: vec![material(&tls.join("ca.der"))?],
            certificate_chain: vec![material(&tls.join("node1.der"))?],
            private_key: material(&tls.join("node1-key.der"))?,
        }))?,
    })
}
fn close(
    mut node: NativeNode<Counter, NativeServiceConnector>,
    group: GroupIdentity,
    start: Instant,
) -> Result<(), Failure> {
    check(node.control(group, NodeControl::Checkpoint))?;
    node.begin_shutdown();
    drive(&mut node, start, |n| Ok(n.is_drained()))?;
    let mut parts = node.into_parts().map_err(|_| "node not drained")?;
    let mut dialer = parts
        .peers
        .take()
        .ok_or("missing peers")?
        .connector
        .into_dialer()
        .map_err(|_| "connector not drained")?
        .ok_or("missing TCP dialer")?;
    let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let (mut log, mut snap) = (false, false);
    loop {
        let dial = check(dialer.try_finish())?;
        if !log {
            log = parts.local.persistence.try_reclaim()?.is_some();
        }
        if !snap {
            snap = snapshots.worker.try_reclaim()?.is_some();
        }
        if dial && log && snap {
            break;
        }
        if Instant::now() > deadline {
            return Err("worker join timed out".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    println!("workers_joined=true");
    Ok(())
}
