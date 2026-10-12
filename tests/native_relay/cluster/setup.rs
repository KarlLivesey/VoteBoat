// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! One native owner assembly shared by direct-star and actual tree histories.
use super::*;
struct Assembly {
    root: PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    mode: NativeOpenMode,
    bootstrap: Bootstrap,
    observations: Rc<RefCell<Vec<Observation>>>,
    fault: Rc<Cell<bool>>,
    identities: BTreeMap<NodeId, LocalIdentity>,
    tree: RelayTreePlan,
}
pub(super) fn open(
    root: PathBuf,
    protocol: NativePeerProtocol,
    mode: NativeOpenMode,
    tree: bool,
) -> Cluster {
    let clock = Instant::now();
    let bootstrap = bootstrap(352, 3);
    let observations = Rc::new(RefCell::new(Vec::new()));
    let fault = Rc::new(Cell::new(false));
    let identities: BTreeMap<_, _> = (9..=if tree { 10 } else { 9 })
        .map(|n| {
            (
                node(n),
                LocalIdentity {
                    node: node(n),
                    store: StoreBinding {
                        identity: identity(u128::from(n)),
                        session: StoreSession::new(
                            n * 100 + u64::from(mode == NativeOpenMode::Recover),
                        )
                        .unwrap(),
                    },
                },
            )
        })
        .collect();
    let topology = RelayTreePlan::new(
        bootstrap.voter_stores.clone(),
        identities
            .iter()
            .map(|(n, i)| (*n, i.store.identity))
            .collect(),
        if tree {
            BTreeSet::from([(node(9), node(10))])
        } else {
            BTreeSet::new()
        },
        (1..=3)
            .map(|n| (node(n), node(if tree && n == 3 { 10 } else { 9 })))
            .collect(),
        8,
        if tree { 2 } else { 1 },
    )
    .unwrap();
    let assembly = Assembly {
        root: root.clone(),
        clock,
        protocol,
        mode,
        bootstrap,
        observations: observations.clone(),
        fault: fault.clone(),
        identities,
        tree: topology.clone(),
    };
    let (mut boats, mut gates, mut hops) = (
        vec![],
        vec![],
        BTreeMap::<NodeId, Vec<RelayHop<Gated>>>::new(),
    );
    for n in 1..=3 {
        let (boat, hop, gate, helper) = assembly.voter(n);
        boats.push(boat);
        gates.push(gate);
        hops.entry(helper).or_default().push(hop);
    }
    if tree {
        gates.push(assembly.link(&mut hops));
    }
    let helper = assembly.helper(node(9), hops.remove(&node(9)).unwrap());
    let branches = hops
        .into_iter()
        .map(|(n, h)| assembly.helper(n, h))
        .collect();
    Cluster {
        root,
        clock,
        protocol,
        boats,
        helper,
        branches,
        controls: vec![],
        tree: topology,
        gates,
        helper_identity: assembly.identities[&node(9)],
        discarded: 0,
        observations,
        scope_fault: fault,
        forwarded_acks: vec![],
        held_read_acks: BTreeSet::new(),
        held_inputs: BTreeMap::new(),
        phase: "initial",
        retiring: false,
        forwarded_probes: vec![],
    }
}
impl Assembly {
    fn link(&self, hops: &mut BTreeMap<NodeId, Vec<RelayHop<Gated>>>) -> Gate {
        let (child, parent, gate) = pair(
            self.protocol,
            self.identities[&node(10)],
            self.identities[&node(9)],
            1,
            self.clock,
        );
        let child = Gated {
            actual: child,
            gate: Gate::default(),
        };
        for (id, session) in [(node(9), parent), (node(10), child)] {
            hops.entry(id).or_default().push(
                RelayHop::for_helper(
                    session,
                    RelayHelperPlan::for_tree(self.identities[&id], self.tree.clone()).unwrap(),
                    Default::default(),
                )
                .unwrap_or_else(|e| panic!("actual helper link {:?}", e.reason)),
            );
        }
        gate
    }

    fn voter(&self, id: u64) -> (Boat, RelayHop<Gated>, Gate, NodeId) {
        let local = prepare(
            &self.root,
            self.clock,
            id,
            &self.bootstrap,
            self.protocol,
            self.mode,
        );
        let physical = LocalIdentity {
            node: node(id),
            store: local.owner.identity().store,
        };
        let plan = NativeRelayPlan::for_tree(physical, self.tree.clone()).unwrap();
        let helper = plan.helper().node;
        let (a, b, gate) = pair(
            self.protocol,
            physical,
            self.identities[&helper],
            1,
            self.clock,
        );
        let hop = RelayHop::for_voter(a, plan.clone(), Default::default())
            .unwrap_or_else(|e| panic!("voter hop {:?}", e.reason));
        let branch = RelayHop::for_helper(
            b,
            RelayHelperPlan::for_tree(self.identities[&helper], self.tree.clone()).unwrap(),
            Default::default(),
        )
        .unwrap_or_else(|e| panic!("leaf helper hop {:?}", e.reason));
        let boat = Boat::from_parts(
            NativeNetworkNodeParts {
                local,
                peers: Some(NativeRelayParts {
                    plan,
                    hop,
                    codec: ObservedWire::new(id, self.observations.clone())
                        .with_scope_fault(self.fault.clone()),
                    limits: Default::default(),
                    routes: None,
                }),
            },
            NodeLimits::default(),
            MonoTime(self.clock.elapsed().as_millis() as u64),
        )
        .unwrap_or_else(|e| panic!("relay Node {:?}", e.reason));
        (boat, branch, gate, helper)
    }
    fn helper(
        &self,
        node: NodeId,
        hops: Vec<RelayHop<Gated>>,
    ) -> RelayForwarder<Gated, ObservedWire> {
        RelayForwarder::build(RelayForwarderParts {
            plan: RelayHelperPlan::for_tree(self.identities[&node], self.tree.clone()).unwrap(),
            codec: ObservedWire::new(node.get(), self.observations.clone()),
            limits: Default::default(),
            hops,
        })
        .unwrap_or_else(|e| panic!("actual helper {:?}", e.reason))
    }
}
