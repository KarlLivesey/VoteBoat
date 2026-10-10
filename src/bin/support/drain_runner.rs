// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Foreground orchestration over existing authenticated, idempotent commands.
use super::{
    command_client::Attempt,
    command_endpoints::{self, Endpoint},
    local_client::{self, repeat_observation},
    service_access::ClientAccess,
    setup::Failure,
};
use std::time::{Duration, Instant};
#[path = "drain_runner/multi.rs"]
mod multi;
#[cfg(test)]
#[path = "drain_runner/observation_tests.rs"]
mod observation_tests;
#[cfg(test)]
#[path = "drain_runner/recovery_tests.rs"]
mod recovery_tests;

#[derive(Debug, Eq, PartialEq)]
struct Progress {
    ready: bool,
    configuration: u128,
    digest: String,
}
#[derive(Debug, Eq, PartialEq)]
enum ConfigurationReply {
    AnotherPeer,
    ObserveSource,
}
fn configuration_attempt(attempt: Attempt, operation: u128) -> Result<ConfigurationReply, Failure> {
    match attempt {
        Attempt::Unavailable => Ok(ConfigurationReply::AnotherPeer),
        Attempt::Reply(reply) => configuration_reply(&reply, operation),
        // The request may have been accepted. Observe the bound source journal
        // before retrying; neither a deadline nor a closed socket proves commit.
        Attempt::Interrupted(
            "request deadline expired"
            | "reply deadline expired"
            | "connection closed during request"
            | "connection closed without a complete reply",
        ) => Ok(ConfigurationReply::ObserveSource),
        Attempt::Interrupted(reason) => {
            Err(format!("UNKNOWN configuration: {reason}; rerun the same drain identity").into())
        }
    }
}
fn configuration_reply(reply: &str, operation: u128) -> Result<ConfigurationReply, Failure> {
    if reply.trim() == "ERR NOT_LEADER" {
        return Ok(ConfigurationReply::AnotherPeer);
    }
    // No proposal was accepted. Recheck the source and retry the same record
    // under the existing total request/time budget, rather than guessing success.
    if reply.trim() == "ERR not_proposed=Busy" {
        return Ok(ConfigurationReply::ObserveSource);
    }
    // Leadership loss leaves the original operation unresolved. The bound
    // source journal decides whether another phase is needed; this is not a
    // committed receipt and must not skip the readiness/stop checks.
    if reply == "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\n"
    {
        return Ok(ConfigurationReply::ObserveSource);
    }
    if reply.starts_with("OK ") {
        if field(reply, "operation")?.parse::<u128>()? != operation {
            return Err("configuration replied with a different operation".into());
        }
        return Ok(ConfigurationReply::ObserveSource);
    }
    Err(format!("{reply}preserve original drain and configuration identities").into())
}
fn field<'a>(text: &'a str, name: &str) -> Result<&'a str, Failure> {
    let prefix = format!("{name}=");
    let mut values = text
        .split_whitespace()
        .filter_map(|word| word.strip_prefix(&prefix));
    let value = values
        .next()
        .ok_or_else(|| format!("missing source field {name}"))?;
    if values.next().is_some() {
        return Err(format!("duplicate source field {name}").into());
    }
    Ok(value)
}
fn identity(text: &str, sequence: u64, operation: u128) -> Result<(), Failure> {
    if !text.starts_with("OK ") {
        return Err(format!("source drain observation unsuccessful: {text:?}").into());
    }
    if field(text, "sequence")?.parse::<u64>()? != sequence
        || field(text, "operation")?.parse::<u128>()? != operation
    {
        return Err("source replied with a different drain identity".into());
    }
    Ok(())
}
fn progress(text: &str, sequence: u64, operation: u128) -> Result<Progress, Failure> {
    identity(text, sequence, operation)?;
    if field(text, "phase")? != "Active" || field(text, "membership_change")? != "true" {
        return Err("runner requires the active original membership drain".into());
    }
    let configuration = field(text, "configuration_operation")?.parse::<u128>()?;
    let digest = field(text, "plan_digest")?;
    if configuration == 0 || digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid bound membership plan fields".into());
    }
    Ok(Progress {
        ready: field(text, "ready")?.parse()?,
        configuration,
        digest: digest.to_owned(),
    })
}
struct Runner {
    endpoints: Vec<Endpoint>,
    source: usize,
    auth: ClientAccess,
    sequence: u64,
    operation: u128,
    start: Instant,
    deadline: Instant,
    remaining: usize,
}
impl Runner {
    fn exchange(&mut self, target: usize, command: &str) -> Result<Attempt, Failure> {
        if self.remaining == 0 || Instant::now() >= self.deadline {
            return Err(
                "UNKNOWN drain runner budget expired; rerun the same sequence and operation".into(),
            );
        }
        self.remaining -= 1;
        Ok(local_client::exchange(
            &self.endpoints[target],
            format!("{command}\n").as_bytes(),
            self.deadline.min(Instant::now() + Duration::from_secs(5)),
            Some(&self.auth),
            self.start,
        ))
    }
    fn source(&mut self, command: &str) -> Result<String, Failure> {
        match self.exchange(self.source, command)? {
            Attempt::Reply(reply) => Ok(reply),
            Attempt::Unavailable => {
                Err("UNKNOWN source unavailable; preserve original drain identity".into())
            }
            Attempt::Interrupted(reason) => Err(format!(
                "UNKNOWN source request: {reason}; preserve original drain identity"
            )
            .into()),
        }
    }
    fn source_observation(&mut self, offset: Option<usize>) -> Result<String, Failure> {
        let command = match offset {
            Some(offset) => format!("drain-group {} {} {offset}", self.sequence, self.operation),
            None => format!("drain-status {} {}", self.sequence, self.operation),
        };
        loop {
            match self.exchange(self.source, &command)? {
                Attempt::Reply(reply) => return Ok(reply),
                Attempt::Unavailable => (),
                Attempt::Interrupted(reason) if repeat_observation(reason) => (),
                Attempt::Interrupted(reason) => {
                    return Err(format!("UNKNOWN source observation {command}: {reason}; preserve original drain identity").into());
                }
            }
            // Only reads reach this loop. Drop each failed channel and repeat
            // the same source/identity/offset within the original shared budget.
            std::thread::park_timeout(
                Duration::from_millis(25)
                    .min(self.deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
    fn status(&mut self) -> Result<Progress, Failure> {
        let text = self.source_observation(None)?;
        progress(&text, self.sequence, self.operation)
    }
    fn resolve_admission(&mut self) -> Result<Progress, Failure> {
        self.status().map_err(|error| {
            format!(
                "UNKNOWN initial drain admission: {error}; rerun the same sequence and operation"
            )
            .into()
        })
    }
    fn configure(&mut self, operation: u128) -> Result<(), Failure> {
        for target in 0..self.endpoints.len() {
            let attempt = self.exchange(target, &format!("configure {operation}"))?;
            match configuration_attempt(attempt, operation)? {
                ConfigurationReply::AnotherPeer => continue,
                ConfigurationReply::ObserveSource => return Ok(()),
            }
        }
        Ok(()) // No leader observed. A later bounded iteration may observe one.
    }
    fn execute(&mut self) -> Result<(), Failure> {
        let begin = self.exchange(
            self.source,
            &format!("drain-node {} {}", self.sequence, self.operation),
        )?;
        let original = match begin {
            Attempt::Reply(ref text) if text.starts_with("OK ") => {
                progress(text, self.sequence, self.operation)?
            }
            // An unobserved admission is resolved through the original journal.
            Attempt::Interrupted(_) => self.resolve_admission()?,
            Attempt::Reply(ref text)
                if text.starts_with("UNKNOWN ") || text.contains("drain busy") =>
            {
                self.resolve_admission()?
            }
            Attempt::Reply(text) => return Err(text.into()),
            Attempt::Unavailable => return Err("source unavailable before drain request".into()),
        };
        let resume = self.source(&format!(
            "resume-drain {} {}",
            self.sequence, self.operation
        ))?;
        let mut current = progress(&resume, self.sequence, self.operation)?;
        loop {
            if current.configuration != original.configuration || current.digest != original.digest
            {
                return Err("source plan identity changed during drain".into());
            }
            if current.ready {
                break;
            }
            self.configure(original.configuration)?;
            std::thread::park_timeout(Duration::from_millis(25));
            current = self.status()?;
        }
        let stopped = self.source(&format!("drain-stop {} {}", self.sequence, self.operation))?;
        identity(&stopped, self.sequence, self.operation)?;
        if field(&stopped, "stopping")? != "true" {
            return Err("source did not accept shutdown".into());
        }
        println!("OK sequence={} operation={} shutdown_requested=true evidence=source_ready_and_stop_accepted retained_replica=true", self.sequence, self.operation);
        Ok(())
    }
}
fn configured(base: u16, source: u64, input: &[String]) -> Result<Runner, Failure> {
    let mut args = input.to_vec();
    let options = local_client::options(&mut args)?;
    let [sequence, operation] = args.as_slice() else {
        return Err(
            "expected drain-run BASE SOURCE SEQUENCE OP --service-tls DIR --principal ID".into(),
        );
    };
    let sequence = sequence.parse::<u64>()?;
    let operation = operation.parse::<u128>()?;
    if sequence == 0 || operation == 0 || options.discover_via.is_some() {
        return Err(
            "drain-run requires positive identities and explicit endpoint configuration".into(),
        );
    }
    let endpoints = command_endpoints::targets(base, None, options.command_peers.as_deref())?;
    let source = endpoints
        .iter()
        .position(|p| p.node == source)
        .ok_or("source missing from command peers")?;
    let (Some(directory), Some(principal)) = (options.tls_directory.as_deref(), options.principal)
    else {
        return Err("drain-run requires --service-tls and --principal".into());
    };
    let auth = ClientAccess::load(directory, principal, &endpoints)?;
    let start = Instant::now();
    Ok(Runner {
        endpoints,
        source,
        auth,
        sequence,
        operation,
        start,
        deadline: start + Duration::from_secs(45),
        remaining: 128,
    })
}
pub fn run(base: u16, source: u64, input: &[String]) -> Result<(), Failure> {
    configured(base, source, input)?.execute()
}
pub fn run_multi(base: u16, source: u64, input: &[String]) -> Result<(), Failure> {
    let mut runner = configured(base, source, input)?;
    runner.deadline = runner.start + Duration::from_secs(120);
    runner.remaining = 4096;
    runner.execute_multi()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> String {
        format!("OK sequence=1 operation=2 phase=Active membership_change=true configuration_operation=3 plan_digest={} ready=false", "ab".repeat(32))
    }
    #[test]
    fn exhausted_runner_budgets_refuse_before_connection() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let args = [
            "1",
            "2",
            "--service-tls",
            path.to_str().unwrap(),
            "--principal",
            "3",
        ]
        .map(str::to_owned);
        let mut runner = configured(10000, 3, &args).unwrap();
        runner.remaining = 0;
        assert!(runner.exchange(0, "status").is_err());
        runner.remaining = 1;
        runner.deadline = Instant::now();
        assert!(runner.exchange(0, "status").is_err());
        assert_eq!(runner.remaining, 1);
    }
    #[test]
    fn configuration_busy_reobserves_but_does_not_mask_other_rejections() {
        assert_eq!(
            configuration_reply(
                "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\n",
                3,
            )
            .unwrap(),
            ConfigurationReply::ObserveSource
        );
        assert_eq!(
            configuration_reply("ERR not_proposed=Busy\n", 3).unwrap(),
            ConfigurationReply::ObserveSource
        );
        assert_eq!(
            configuration_reply("ERR NOT_LEADER\n", 3).unwrap(),
            ConfigurationReply::AnotherPeer
        );
        assert_eq!(
            configuration_reply("OK operation=3 action=wait_for_commit\n", 3).unwrap(),
            ConfigurationReply::ObserveSource
        );
        for rejected in [
            "ERR not_proposed=Busy extra=unknown\n",
            "ERR not_proposed=WrongIdentity\n",
            "ERR Unauthorized\n",
            "UNKNOWN configuration\n",
            "UNKNOWN LeadershipChanged; changed record\n",
            "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\nextra\n",
            "OK operation=4 action=completed\n",
            "OK action=completed\n",
        ] {
            assert!(configuration_reply(rejected, 3).is_err(), "{rejected}");
        }
    }
    #[test]
    fn source_progress_rejects_wrong_duplicate_missing_and_non_membership_fields() {
        let valid = sample();
        assert_eq!(progress(&valid, 1, 2).unwrap().configuration, 3);
        for malformed in [
            valid.replace("sequence=1", "sequence=2"),
            valid.replace("operation=2", "operation=4"),
            format!("{valid} sequence=1"),
            valid.replace("phase=Active", "phase=Cancelled"),
            valid.replace("membership_change=true", "membership_change=false"),
            valid.replace("configuration_operation=3", "configuration_operation=0"),
            valid.replace("plan_digest=", "missing="),
            valid.replace("abab", "zzzz"),
        ] {
            assert!(progress(&malformed, 1, 2).is_err(), "{malformed}");
        }
    }
}
