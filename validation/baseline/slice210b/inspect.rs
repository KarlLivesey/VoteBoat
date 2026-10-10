use std::path::PathBuf;
use voteboat::{
    application::*,
    identity::*,
    log::*,
    maintenance::*,
    native::{log_store::*, snapshot_store::*},
    snapshot::*,
};
fn main() {
    for root in std::env::args().skip(1).map(PathBuf::from) {
        println!("root={}", root.display());
        for node in 1..=3 {
            let directory = root.join(node.to_string());
            let store = StoreIdentity {
                id: StoreId::new(node).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            };
            let log = NativeLogStore::recover(
                FileLogIo::open(&directory).unwrap(),
                store,
                LogLimits::default(),
            )
            .unwrap();
            for (id, incarnation) in [(1, 1), (7, 3), (8, 2)] {
                let group = GroupIdentity {
                    id: GroupId::new(id).unwrap(),
                    incarnation: GroupIncarnation::new(incarnation).unwrap(),
                };
                let state = log.state(group).unwrap();
                println!(
                    "node={node} group={id}:{incarnation} term={} last={} commit={} base={}",
                    state.hard_state.term,
                    state.last_index(),
                    state.commit_index,
                    state.base_index()
                );
                let mut snapshots = NativeSnapshotStore::recover(
                    FileSnapshotIo::open(directory.join(if id == 1 {
                        "snapshots".to_string()
                    } else {
                        format!("snapshots-{id}-{incarnation}")
                    }))
                    .unwrap(),
                    SnapshotIdentity { group, store },
                    SnapshotLimits::default(),
                )
                .unwrap();
                let mut app = Maintenance::new(group, 2, 64, Counter::new(10000).unwrap()).unwrap();
                let (_, _) = recover_member_replica(
                    NodeId::new(node as u64).unwrap(),
                    group,
                    &log,
                    &mut snapshots,
                    &mut app,
                )
                .unwrap();
                println!(
                    "applied={} data={:?} record={:?}",
                    app.applied_index(),
                    app.inner().read_applied(app.applied_index()),
                    app.record(OperationId::new(40001).unwrap())
                );
                for entry in &state.entries {
                    if entry.index > state.commit_index {
                        println!("uncommitted={entry:?}");
                    }
                }
            }
        }
    }
}
