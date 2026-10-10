// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    preview_input,
    setup::{checked, Failure},
};
use std::{fmt::Write, path::Path};
use voteboat::{identity::GroupIdentity, transfer::*};
fn group(group: GroupIdentity) -> String {
    format!("{}:{}", group.id.get(), group.incarnation.get())
}
pub fn command(args: &[String]) -> Result<(), Failure> {
    let [file] = args else {
        return Err("expected split-preview PROFILE".into());
    };
    let input = preview_input::load(Path::new(file))?;
    let adapter = input.intent.before().input().application;
    let sources = input
        .sources
        .iter()
        .map(|s| PreviewSource {
            group: s.group,
            adapter,
            application: &s.app,
            export_bytes: s.budget,
        })
        .collect::<Vec<_>>();
    let targets = input
        .targets
        .iter()
        .map(|t| PreviewTarget {
            group: t.application.group,
            adapter,
            application: &t.application.app,
            configuration: &t.configuration,
            replicas: &t.replicas,
            import_bytes: t.application.budget,
        })
        .collect::<Vec<_>>();
    let preview = checked(preview_transfer(
        &input.intent,
        &sources,
        &targets,
        input.budget,
    ))?;
    print!("{}", render(&preview, adapter)?);
    Ok(())
}
fn render(
    preview: &TransferPreview,
    adapter: voteboat::routing::ApplicationAdapter,
) -> Result<String, Failure> {
    let mut out =
        String::from("preview_only=true source_view=configured_bounds data_import_checked=false\n");
    writeln!(
        out,
        "application={}:{} export_capability=scope_adapter import_capability=scope_adapter",
        adapter.id.get(),
        adapter.version
    )?;
    writeln!(
        out,
        "responsibility={}:{} epoch={}->{} generation={} payload_upper_bound={}",
        preview.responsibility.id.get(),
        preview.responsibility.incarnation.get(),
        preview.expected_epoch.get(),
        preview.next_epoch.get(),
        preview.expected_generation.get(),
        preview.payload_bytes
    )?;
    let digest = preview
        .intent_digest
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    writeln!(out, "intent_sha256={digest}")?;
    out.push_str("pause=source_fence_to_target_activation duration_upper_bound=unknown\nordering=per_target_group cross_group_atomicity=false\nretention=until_durable_activation_and_release wal_bytes=unknown\n");
    for e in &preview.exports {
        writeln!(
            out,
            "move source={} target={} scope={}..{} payload_upper_bound={}",
            group(e.source),
            group(e.target),
            e.scope.start(),
            e.scope.end(),
            e.payload_bytes
        )?;
    }
    for s in &preview.sources {
        for scope in &s.retained_scopes {
            writeln!(
                out,
                "retained source={} scope={}..{}",
                group(s.group),
                scope.start(),
                scope.end()
            )?;
        }
    }
    for t in &preview.targets {
        writeln!(
            out,
            "target={} configuration={} payload_upper_bound={}",
            group(t.group),
            t.configuration.get(),
            t.payload_bytes
        )?;
        for r in &t.replicas {
            writeln!(
                out,
                "replica target={} node={} store={}:{} domain={} role={}",
                group(t.group),
                r.node.get(),
                r.placement.store.id.get(),
                r.placement.store.incarnation.get(),
                r.placement.domain.get(),
                if r.voter { "voter" } else { "learner" }
            )?;
        }
    }
    out.push_str("required=authorize_current_generations,source_fence,target_durable_import,publication,activation,preserve_retry_outbox,retention_release\n");
    Ok(out)
}
