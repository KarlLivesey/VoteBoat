use std::{fs, path::PathBuf};
use voteboat::{
    identity::*,
    log::{EntryPayload, LogLimits, LogStore},
    native::log_store::{FileLogIo, NativeLogStore},
};

fn main() {
    let root = PathBuf::from(std::env::args_os().nth(1).unwrap());
    let mut groups: Vec<_> = fs::read_dir(&root).unwrap().map(Result::unwrap).collect();
    groups.retain(|e| e.file_type().unwrap().is_dir()
        && e.file_name().to_str().unwrap().parse::<u128>().is_ok());
    groups.sort_by_key(|e| e.file_name());
    for g in groups {
        let id: u128 = g.file_name().to_str().unwrap().parse().unwrap();
        let group = GroupIdentity {
            id: GroupId::new(id).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        };
        for node in 1..=3 {
            let directory = g.path().join(node.to_string());
            let io = FileLogIo::open(&directory).unwrap();
            let identity = StoreIdentity {
                id: StoreId::new(node).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            };
            let store = NativeLogStore::recover(io, identity, LogLimits::default()).unwrap();
            let log = store.state(group).unwrap();
            println!("group={id} node={node} revision={:?} term={} vote={:?} base={} commit={} last={} snapshot={:?}",
                log.revision, log.hard_state.term, log.hard_state.voted_for,
                log.base_index(), log.commit_index, log.last_index(), log.snapshot);
            for e in &log.entries {
                if let EntryPayload::Command {operation, bytes} = &e.payload {
                    println!("  command index={} term={} operation={operation:?} bytes={} committed={}", e.index, e.term, bytes.len(), e.index <= log.commit_index);
                }
            }
        }
    }
}
