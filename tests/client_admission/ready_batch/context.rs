// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct ContextOnly(Counter);
impl StateMachine for ContextOnly {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl BoundedStateMachine for ContextOnly {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for ContextOnly {
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        _: &[u8],
    ) -> Result<(), ApplicationError> {
        if operation == Counter::operation(1, 2).unwrap() && core.state().last_index() != 1 {
            Err(ApplicationError::InvalidCommand)
        } else {
            Ok(())
        }
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
}

#[test]
fn default_host_context_is_rechecked_after_original_singleton_durability() {
    let (mut owner, mut worker, mut clients, mut results, apps) = setup_with_visit_items(1, 4, 64);
    let mut apps: BTreeMap<_, _> = apps.into_iter().map(|(g, a)| (g, ContextOnly(a))).collect();
    assert!(!apps[&group(1)].supports_ready_batch());
    let first = clients
        .submit(&mut owner, &apps[&group(1)], request(1, 1, 1))
        .unwrap();
    let second = clients
        .submit(&mut owner, &apps[&group(1)], request(1, 2, 1))
        .unwrap();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 1);
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &lease.effect else {
        panic!()
    };
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 1);
    owner
        .submit_persists(&mut worker, vec![lease], MonoTime(400))
        .unwrap();
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    let mut completions = BTreeMap::new();
    while let Some(completion) = clients.poll() {
        let ticket = completion.ticket();
        completions.insert(ticket.sequence, clients.complete(completion).unwrap());
    }
    assert!(matches!(
        completions[&first.sequence],
        ClientOutcome::Applied { .. }
    ));
    assert!(matches!(
        completions[&second.sequence],
        ClientOutcome::NotProposed(RaftError::Admission(ApplicationError::InvalidCommand))
    ));
    assert_eq!(owner.core(group(1)).unwrap().state().last_index(), 2);
    assert_eq!(apps[&group(1)].0.read_applied(2), Ok(1));
    assert!(clients.is_drained());
}
