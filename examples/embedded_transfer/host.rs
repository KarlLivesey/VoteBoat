// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    log::Bootstrap,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
        startup::*,
        tls::*,
        worker::ThreadWake,
    },
    quorum::*,
    runtime::*,
    transaction::*,
};
pub type Failure = Box<dyn std::error::Error>;
pub type HostNode = NativeNode<TransferApplication, NativeServiceConnector>;
pub fn check<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, Failure> {
    value.map_err(|e| format!("{e:?}").into())
}
pub fn now(clock: Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
pub fn groups() -> [GroupIdentity; 3] {
    [77, 78, 79].map(|id| GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(4).unwrap(),
    })
}
pub fn profile() -> TransferProfile {
    let [c, a, b] = groups();
    TransferProfile::new(c, [a, b]).unwrap()
}
pub fn account(slot: usize) -> AccountId {
    AccountId::new(10 + slot as u64).unwrap()
}
pub fn spec(id: TransactionId, amount: i64, direction: &str) -> Result<TransferSpec, Failure> {
    let base = id
        .get()
        .checked_mul(16)
        .ok_or("transaction ID too large for phase IDs")?;
    let phase = |offset| -> Result<OperationId, Failure> {
        OperationId::new(base.checked_add(offset).ok_or("phase ID overflow")?)
            .ok_or_else(|| "nonzero phase ID required".into())
    };
    let phases = check(PhaseIds::new(
        [phase(1)?, phase(2)?, phase(3)?, phase(4)?],
        [
            [phase(5)?, phase(6)?, phase(7)?],
            [phase(8)?, phase(9)?, phase(10)?],
        ],
    ))?;
    let [left, right] = [0, 1].map(|i| AccountRef::new(profile().participants()[i], account(i)));
    let (debit, credit) = match direction {
        "forward" => (left, right),
        "reverse" => (right, left),
        _ => return Err("expected forward or reverse".into()),
    };
    check(TransferSpec::new(
        id,
        profile(),
        debit,
        credit,
        amount,
        phases,
    ))
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
pub fn open(root: &str, tls: &Path, mode: NativeOpenMode) -> Result<HostNode, Failure> {
    let node = NodeId::new(1).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let mut bootstraps = Vec::new();
    for group in groups() {
        bootstraps.push(Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: check(Policy::new(Tree::Voter(node), Limits::default()))?,
            voter_stores: BTreeMap::from([(node, store)]),
        });
    }
    let config = NativeMultiStartup {
        startup: NativeStartup {
            directory: root.into(),
            mode,
            node,
            store,
            bootstrap: bootstraps.remove(0),
            listen: "127.0.0.1:0".parse()?,
            peers: BTreeMap::new(),
            entropy_seed: 17,
            limits: NodeLimits::default(),
            ready_visit_items: 16,
            append_window: None,
            tls: check(
                check(NativeTlsConfig::new(TlsCredentials {
                    roots: vec![material(&tls.join("ca.der"))?],
                    certificate_chain: vec![material(&tls.join("node1.der"))?],
                    private_key: material(&tls.join("node1-key.der"))?,
                }))?
                .with_wire_version(8),
            )?,
        },
        additional_groups: bootstraps,
        provisioned_stores: BTreeMap::from([(node, store)]),
    };
    let limits = check(TransferLimits::new(1024, 1))?;
    let mut apps = BTreeMap::from([(
        profile().coordinator(),
        check(TransferApplication::coordinator(
            profile().coordinator(),
            profile(),
            limits,
        ))?,
    )]);
    for (slot, group) in profile().participants().into_iter().enumerate() {
        apps.insert(
            group,
            check(TransferApplication::participant(
                group,
                profile(),
                &[(account(slot), if slot == 0 { 1000 } else { 0 })],
                limits,
            ))?,
        );
    }
    match config.open(
        NativePeerProtocol::TcpTls,
        NativeTimingProfile::Throughput.timers(),
        apps,
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    ) {
        Ok(node) => Ok(node),
        Err(mut rejected) => {
            let end = Instant::now() + Duration::from_secs(10);
            while !rejected.try_cleanup()? {
                if Instant::now() > end {
                    return Err("startup cleanup timed out".into());
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
            Err(rejected.reason.into())
        }
    }
}
pub fn drive(
    node: &mut HostNode,
    clock: Instant,
    mut done: impl FnMut(&mut HostNode) -> Result<bool, Failure>,
) -> Result<(), Failure> {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        check(node.poll(now(clock), NodePollBudget::default()))?;
        if done(node)? {
            return Ok(());
        }
        if Instant::now() > end {
            return Err("native progress timed out; files retained".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
#[path = "host/cleanup.rs"]
mod cleanup;
pub use cleanup::close;
