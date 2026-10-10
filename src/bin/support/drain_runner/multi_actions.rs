// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use rows::Voter;

fn reobserve(text: &str) -> bool {
    matches!(
        text,
        "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n"
            | "ERR Unavailable(LeadershipChanged)\n"
            | "ERR NotRead(ReadNotReady)\n"
            | "ERR not_proposed=Busy\n"
    )
}
impl Runner {
    fn group_leader(&mut self, row: &Assignment) -> Result<Option<usize>, Failure> {
        for peer in 0..self.endpoints.len() {
            match self.exchange(peer, &row.command("status"))? {
                Attempt::Unavailable | Attempt::Interrupted(_) => (),
                Attempt::Reply(text) if text == "ERR unknown group or incarnation\n" => (),
                Attempt::Reply(text) if text.starts_with("OK ") => match field(&text, "role")? {
                    "Leader" => return Ok(Some(peer)),
                    "Follower" | "Candidate" => (),
                    _ => return Err("invalid group role".into()),
                },
                Attempt::Reply(text) => return Err(text.into()),
            }
        }
        Ok(None)
    }
    fn group_exchange(
        &mut self,
        peer: usize,
        row: &Assignment,
        command: &str,
    ) -> Result<Option<String>, Failure> {
        match self.exchange(peer, &row.command(command))? {
            Attempt::Unavailable => Ok(None),
            Attempt::Reply(text) if text == "ERR NOT_LEADER\n" => Ok(None),
            // These commands are reads or retries of an exact bound handoff.
            // Observe the group again; these replies prove no completion. Busy
            // explicitly means no proposal was admitted, so the same bound
            // action can be retried without inventing a new operation.
            Attempt::Reply(text) if reobserve(&text) => Ok(None),
            Attempt::Reply(text) if text.starts_with("OK ") => Ok(Some(text)),
            Attempt::Reply(text) => {
                Err(format!("{text}preserve original group drain identities").into())
            }
            Attempt::Interrupted(reason) => Err(format!(
                "UNKNOWN group command: {reason}; rerun the same drain identity"
            )
            .into()),
        }
    }
    fn handoff(&mut self, peer: usize, row: &Assignment, voter: Voter) -> Result<bool, Failure> {
        let Some(mut text) =
            self.group_exchange(peer, row, &format!("leadership-status {}", self.operation))?
        else {
            return Ok(false);
        };
        let command = format!(
            "move-leader {} {} {} {} {} {} {} {}",
            self.operation,
            row.configuration.get(),
            row.source.node.get(),
            row.source.store.id.get(),
            row.source.store.incarnation.get(),
            voter.target,
            voter.store.id.get(),
            voter.store.incarnation.get()
        );
        if text == "OK phase=Absent evidence=quorum_read\n" {
            let Some(begun) = self.group_exchange(peer, row, &command)? else {
                return Ok(false);
            };
            text = begun;
        } else if field(&text, "evidence")? != "quorum_read" {
            return Err("original handoff observation lacks quorum evidence".into());
        }
        self.check_handoff(&text, row, voter)?;
        if field(&text, "phase")? == "Completed" {
            // Completion preserves the original intent, not current leadership.
            // The bound configuration proposal checks actual authority again.
            return Ok(true);
        }
        let command = format!("resume-leadership {}", self.operation);
        if let Some(text) = self.group_exchange(peer, row, &command)? {
            self.check_handoff(&text, row, voter)?;
            return Ok(field(&text, "phase")? == "Completed");
        }
        Ok(false)
    }
    fn check_handoff(&self, text: &str, row: &Assignment, voter: Voter) -> Result<(), Failure> {
        if field(text, "operation")?.parse::<u128>()? != self.operation
            || field(text, "configuration")?.parse::<u64>()? != row.configuration.get()
            || field(text, "target")?.parse::<u64>()? != voter.target
            || field(text, "target_store")?.parse::<u128>()? != voter.store.id.get()
            || field(text, "target_incarnation")?.parse::<u64>()? != voter.store.incarnation.get()
            || !matches!(field(text, "phase")?, "Pending" | "Completed")
        {
            return Err("group handoff response differs from original intent".into());
        }
        Ok(())
    }
    pub(super) fn advance_group(&mut self, row: &Assignment) -> Result<(), Failure> {
        let Some(voter) = row.voter else {
            return Ok(());
        };
        let Some(peer) = self.group_leader(row)? else {
            return Ok(());
        };
        let op = voter.operation.get();
        let Some(text) = self.group_exchange(peer, row, &format!("configuration-status {op}"))?
        else {
            return Ok(());
        };
        if field(&text, "operation")?.parse::<u128>()? != op {
            return Err("group configuration response has wrong operation".into());
        }
        match field(&text, "action")? {
            "completed" | "wait_for_commit" => Ok(()),
            "inconclusive_local_absence" | "finalize_requires_authorization" => {
                if field(&text, "action")? == "inconclusive_local_absence"
                    && !self.handoff(peer, row, voter)?
                {
                    return Ok(());
                }
                let attempt = self.exchange(peer, &row.command(&format!("configure {op}")))?;
                configuration_attempt(attempt, op).map(|_| ())
            }
            _ => Err("invalid group configuration progress".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handoff_receipts_must_match_original_bound_target_and_operation() {
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
        let runner = configured(10000, 3, &args).unwrap();
        let row = Row::parse("OK sequence=1 operation=2 offset=0 groups=1 group=7 incarnation=3 configuration=9 done=false source=3 source_store=3 source_incarnation=1 kind=voter target=1 store=1 store_incarnation=1 configuration_operation=7001", 1, 2, 0, 1).unwrap().assignment;
        let voter = row.voter.unwrap();
        let receipt = "OK operation=2 source=3 configuration=9 target=1 target_store=1 target_incarnation=1 phase=Pending";
        runner.check_handoff(receipt, &row, voter).unwrap();
        // A handoff initiated after another election has that leader as its
        // source. This is distinct from the node whose local drain is planned.
        runner
            .check_handoff(&receipt.replace("source=3", "source=2"), &row, voter)
            .unwrap();
        runner
            .check_handoff(&receipt.replace("Pending", "Completed"), &row, voter)
            .unwrap();
        for invalid in [
            receipt.replace("operation=2", "operation=3"),
            receipt.replace("configuration=9", "configuration=10"),
            receipt.replace("target=1", "target=2"),
            receipt.replace("target_store=1", "target_store=2"),
            receipt.replace("target_incarnation=1", "target_incarnation=2"),
            receipt.replace("Pending", "Cancelled"),
            receipt.replace("target_store=1", ""),
            format!("{receipt} operation=2"),
        ] {
            assert!(
                runner.check_handoff(&invalid, &row, voter).is_err(),
                "{invalid}"
            );
        }
    }
    #[test]
    fn only_known_authority_transitions_return_to_observation() {
        for text in [
            "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n",
            "ERR Unavailable(LeadershipChanged)\n",
            "ERR NotRead(ReadNotReady)\n",
            "ERR not_proposed=Busy\n",
        ] {
            assert!(reobserve(text));
        }
        for text in [
            "UNKNOWN storage failure\n",
            "ERR Unauthorized\n",
            "ERR Unavailable(Other)\n",
            "UNKNOWN LeadershipChanged; changed payload\n",
            "ERR not_proposed=Busy extra=unknown\n",
        ] {
            assert!(!reobserve(text));
        }
    }
}
