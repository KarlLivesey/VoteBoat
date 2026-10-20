// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A fork temporarily copies even CLOEXEC descriptors. A lane's close/reopen
//! must not race that copy: the child can retain its old WAL LOCK until exec.
//! In-process lane owners may overlap; exclude only fork/exec, not child work.
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

static OWNERS: RwLock<()> = RwLock::new(());

pub(super) fn owner() -> RwLockReadGuard<'static, ()> {
    OWNERS.read().unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn spawning() -> RwLockWriteGuard<'static, ()> {
    OWNERS.write().unwrap_or_else(PoisonError::into_inner)
}

#[test]
fn inherited_descriptor_control_and_spawn_exclusion() {
    use std::{
        fs::OpenOptions,
        io::{BufRead, BufReader, Write},
        process::{Child, Command, Stdio},
    };
    use voteboat::native::log_store::{FileLogIo, NativeLogStore};
    struct Reap(Child);
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root =
        std::env::temp_dir().join(format!("voteboat-lane-lock-control-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    // Keep other fixtures out of the deliberately injected inheritance case.
    let spawn = spawning();
    let log = NativeLogStore::create(
        FileLogIo::create(&root).unwrap(),
        super::store(1),
        super::LogLimits::default(),
    )
    .unwrap();
    drop(log);
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("LOCK"))
        .unwrap();
    lock.try_lock().unwrap();
    // Pass the copied descriptor as stderr to keep the pre-exec inheritance
    // window open until an explicit stdin command. No sleep hits the race.
    let mut inherited = Reap(
        Command::new("sh")
            .args(["-c", "printf 'ready\\n'; read -r done"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(lock.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    let mut ready = String::new();
    BufReader::new(inherited.0.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready, "ready\n");
    drop(lock);
    eprintln!(
        "inherited WAL lock control: path={} child_pid={}",
        root.join("LOCK").display(),
        inherited.0.id()
    );
    let error = FileLogIo::open(&root)
        .err()
        .expect("duplicate retains the WAL lock");
    assert!(matches!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<std::fs::TryLockError>()),
        Some(std::fs::TryLockError::WouldBlock)
    ));
    inherited
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(b"release\n")
        .unwrap();
    assert!(inherited.0.wait().unwrap().success());
    let log = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        super::store(1),
        super::LogLimits::default(),
    )
    .unwrap();
    drop(spawn);
    let _owner = owner();
    assert!(
        matches!(OWNERS.try_write(), Err(std::sync::TryLockError::WouldBlock)),
        "fork/exec must be excluded throughout owner close/reopen"
    );
    drop(log);
    let reopened = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        super::store(1),
        super::LogLimits::default(),
    )
    .unwrap();
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
