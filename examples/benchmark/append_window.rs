// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit bounded pipeline selection and actual selected-provider readback.
use super::*;
use voteboat::{
    raft::{AppendWindowLimits, BatchLimits},
    worker::PersistenceWorker,
};

pub(super) fn extract(args: &mut Vec<String>) -> Result<Option<AppendWindowLimits>, Failure> {
    let Some(position) = args.iter().position(|arg| arg == "--append-window") else {
        return Ok(None);
    };
    if args
        .iter()
        .skip(position + 1)
        .any(|arg| arg == "--append-window")
    {
        return Err("duplicate --append-window".into());
    }
    let units = args
        .get(position + 1)
        .ok_or("missing --append-window value")?
        .parse()?;
    let selected = checked(
        AppendWindowLimits {
            max_units: units,
            max_originals: BatchLimits::default().max_proposal_entries,
            max_retained_bytes: LogLimits::default().max_batch_bytes,
        }
        .validate(LogLimits::default(), BatchLimits::default()),
    )?;
    args.drain(position..position + 2);
    Ok(Some(selected))
}

pub(super) fn settings(selection: Option<AppendWindowLimits>) -> String {
    selection.map_or_else(
        || "append_window_enabled=false append_window_units=1".to_owned(),
        |window| format!(
            "append_window_enabled=true append_window_units={} append_window_originals={} append_window_core_bytes={}",
            window.max_units, window.max_originals, window.max_retained_bytes,
        ),
    )
}

pub(super) fn verify<L: LogStore + Send + 'static>(
    root: &Path,
    mode: NativeOpenMode,
    replicas: &[Replica<L>],
    selection: Option<AppendWindowLimits>,
) -> Result<(), Failure> {
    let mut rows = String::new();
    use std::fmt::Write;
    for (replica, node) in replicas.iter().enumerate() {
        let local = node.local();
        let capacity = local.persistence.max_outstanding_units_per_group();
        if capacity != selection.map_or(1, |window| window.max_units) {
            return Err("actual worker append-window selection differs".into());
        }
        for group in local.owner.groups() {
            let core = local.owner.core(group).ok_or("missing selected Core")?;
            if core.append_window_limits() != selection {
                return Err("actual Core append-window selection differs".into());
            }
            writeln!(
                rows,
                "replica={} group={} worker_units={capacity} {}",
                replica + 1,
                group.id.get(),
                settings(core.append_window_limits())
            )?;
        }
    }
    let name = match mode {
        NativeOpenMode::Create => "append-window-create.txt",
        NativeOpenMode::Recover => "append-window-recover.txt",
    };
    let mut file = exclusive(&root.join(name))?;
    file.write_all(rows.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_or_ambiguous_selection_without_consuming_arguments() {
        for input in [
            vec!["--append-window"],
            vec!["--append-window", "0"],
            vec!["--append-window", "1"],
            vec!["--append-window", "65"],
            vec!["--append-window", "4", "--append-window", "4"],
        ] {
            let mut args: Vec<_> = input.into_iter().map(str::to_owned).collect();
            let original = args.clone();
            assert!(extract(&mut args).is_err());
            assert_eq!(args, original);
        }
        let mut args = vec![
            "root".to_owned(),
            "--append-window".to_owned(),
            "4".to_owned(),
        ];
        let selection = extract(&mut args).unwrap().unwrap();
        assert_eq!(args, ["root"]);
        assert_eq!(selection.max_units, 4);
        assert_eq!(selection.max_originals, 64);
        assert!(settings(Some(selection)).contains("append_window_units=4"));
        assert!(settings(None).contains("append_window_enabled=false"));
    }
}
