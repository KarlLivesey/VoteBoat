// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "voteboat-lanes-{}-{label}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    root
}
#[test]
fn assignments_preserve_totals_and_never_overlap() {
    for lanes in 1..=4 {
        for groups in lanes..=32 {
            let config = Config {
                lanes,
                groups,
                count: 37,
                window: 11,
            }
            .validate()
            .unwrap();
            let plans = (0..lanes).map(|i| config.plan(i)).collect::<Vec<_>>();
            assert_eq!(plans.iter().map(|p| p.count).sum::<usize>(), 37);
            assert_eq!(plans.iter().map(|p| p.window).sum::<usize>(), 11);
            assert_eq!(plans.iter().map(|p| p.warmup).sum::<usize>(), WARMUP);
            let actual = plans
                .iter()
                .flat_map(|p| p.partition.offset + 1..=p.partition.offset + p.partition.groups)
                .collect::<Vec<_>>();
            assert_eq!(actual, (1..=groups).collect::<Vec<_>>());
        }
    }
    for config in [
        Config {
            lanes: 0,
            groups: 4,
            count: 8,
            window: 8,
        },
        Config {
            lanes: 5,
            groups: 8,
            count: 8,
            window: 8,
        },
        Config {
            lanes: 3,
            groups: 2,
            count: 8,
            window: 8,
        },
        Config {
            lanes: 3,
            groups: 4,
            count: 2,
            window: 8,
        },
        Config {
            lanes: 3,
            groups: 4,
            count: 8,
            window: 2,
        },
    ] {
        assert!(config.validate().is_err());
    }
}
fn history(protocol: NativePeerProtocol) {
    let root = root(&format!("{protocol:?}"));
    let config = Config {
        lanes: 2,
        groups: 4,
        count: 8,
        window: 4,
    }
    .validate()
    .unwrap();
    execute(&root, config, protocol).unwrap();
    let csv = std::fs::read_to_string(root.join("samples.csv")).unwrap();
    assert_eq!(csv.lines().count(), 9);
    for row in csv.lines().skip(1) {
        let columns = row.split(',').collect::<Vec<_>>();
        let lane = columns[0].parse::<usize>().unwrap();
        let group = columns[1].parse::<usize>().unwrap();
        assert!((1..=2).contains(&lane));
        assert_eq!((group - 1) / 2 + 1, lane);
        assert!(columns[4].parse::<u128>().unwrap() >= columns[3].parse::<u128>().unwrap());
    }
    for lane in 1..=2 {
        let dir = root.join(format!("lane{lane}"));
        let storage = std::fs::read_to_string(dir.join("storage.csv")).unwrap();
        assert!(storage.contains("after_recover_join"));
        for n in 1..=3 {
            use voteboat::native::log_store::{FileLogIo, NativeLogStore};
            let log = NativeLogStore::recover(
                FileLogIo::open(dir.join(format!("replica{n}"))).unwrap(),
                store((lane - 1) * 3 + n),
                LogLimits::default(),
            )
            .unwrap();
            for g in (lane - 1) * 2 + 1..=lane * 2 {
                let state = log.state(group_id(g as usize)).unwrap();
                assert!(state.commit_index > 0);
                assert_eq!(
                    state.bootstrap.voter_stores[&node(n)],
                    store((lane - 1) * 3 + n)
                );
            }
        }
    }
    let summary = std::fs::read_to_string(root.join("summary.txt")).unwrap();
    assert!(summary.contains(
        "lanes=2 groups=4 host_owner_threads=2 wal_workers=6 snapshot_workers=6 peer_endpoints=6"
    ));
    assert!(summary.contains("retry_verified=true workers_joined=true host_threads_joined=true"));
    assert!(!root.join("failure.txt").exists());
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_two_lanes_share_groups_and_recover() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_two_lanes_share_groups_and_recover() {
    history(NativePeerProtocol::Quic);
}
#[test]
fn failed_lane_aborts_prepared_lanes_and_joins_hosts() {
    let root = root("failure");
    std::fs::create_dir(root.join("lane2")).unwrap();
    let config = Config {
        lanes: 2,
        groups: 4,
        count: 8,
        window: 4,
    }
    .validate()
    .unwrap();
    assert!(execute(&root, config, NativePeerProtocol::TcpTls).is_err());
    assert!(!root.join("summary.txt").exists());
    let failure = std::fs::read_to_string(root.join("failure.txt")).unwrap();
    assert!(failure.contains("valid_measurement=false host_threads_joined=true"));
    let cleanup = std::fs::read_to_string(root.join("lane1/failure-cleanup.txt")).unwrap();
    assert_eq!(cleanup, "workers_joined=true\n");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn uneven_three_lane_workload_keeps_exact_global_counts() {
    let root = root("uneven");
    let config = Config {
        lanes: 3,
        groups: 5,
        count: 11,
        window: 7,
    }
    .validate()
    .unwrap();
    execute(&root, config, NativePeerProtocol::TcpTls).unwrap();
    let csv = std::fs::read_to_string(root.join("samples.csv")).unwrap();
    assert_eq!(csv.lines().count(), 12);
    let counts = (1..=3)
        .map(|lane| {
            csv.lines()
                .skip(1)
                .filter(|line| line.starts_with(&format!("{lane},")))
                .count()
        })
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![4, 4, 3]);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn partial_replica_startup_joins_previously_opened_nodes() {
    let root = root("partial-replica");
    std::fs::write(root.join("replica2"), b"injected path conflict").unwrap();
    let clock = Instant::now();
    assert!(shared::open_partition(
        &root,
        NativeOpenMode::Create,
        NativePeerProtocol::TcpTls,
        16,
        &clock,
        shared::Partition {
            offset: 0,
            groups: 2,
            lane: 1
        }
    )
    .is_err());
    assert_eq!(
        std::fs::read_to_string(root.join("failure-cleanup.txt")).unwrap(),
        "workers_joined=true\n"
    );
    assert!(!root.join("summary.txt").exists());
    std::fs::remove_dir_all(root).unwrap();
}
