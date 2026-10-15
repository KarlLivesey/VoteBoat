// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Temporary admission reservations, lent only across synchronous submissions.
use super::*;

/// A synchronous, exclusively borrowed proposal scope. Each submission retains
/// its own ownership/result contract and bounds; this grants no maintenance,
/// authentication, application or durability authority. No Core transitions or
/// application mutation can be driven through this capability.
pub trait ProposalIngress {
    /// Current leader role with a committed entry in the current term, for
    /// diagnostic routing only. Fencing need not change this predicate;
    /// submission independently rechecks the owner's failure state. This is
    /// not a lease or linearizable-read authorization.
    fn ready_leader(&self, group: GroupIdentity) -> bool;
    fn submit(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected>;
}

pub(super) struct SubmissionValidation<'app, A> {
    application: Option<&'app A>,
    group: Option<GroupIdentity>,
    applied: u64,
    pub(super) accepted: bool,
    attempted: bool,
    context: Option<PreparedAdmission<'app>>,
}
impl<A> Default for SubmissionValidation<'_, A> {
    fn default() -> Self {
        Self {
            application: None,
            group: None,
            applied: 0,
            accepted: false,
            attempted: false,
            context: None,
        }
    }
}
impl<'app, A: ProposalAdmission> SubmissionValidation<'app, A>
where
    A::Receipt: ApplicationReceipt,
{
    pub(super) fn validate<'pending, I: Iterator<Item = (OperationId, &'pending [u8])>>(
        &mut self,
        application: &'app A,
        request: &ClientRequest,
        pending: impl Fn() -> I,
    ) -> Result<usize, ApplicationError> {
        if self.group != Some(request.group)
            || !self
                .application
                .is_some_and(|old| std::ptr::eq(old, application))
            || self.applied != application.applied_index()
        {
            *self = Self {
                application: Some(application),
                group: Some(request.group),
                applied: application.applied_index(),
                ..Self::default()
            };
        }
        if self.accepted && !self.attempted {
            // The iterator's bytes are temporary; only immutable application
            // state can be borrowed by the resulting capability.
            self.context = application.prepare_proposal_admission(pending());
            self.attempted = true;
        }
        match self.context.as_mut() {
            Some(context) => context(request.operation, &request.bytes),
            None => application.validate_proposal(request.operation, &request.bytes, pending()),
        }
    }
}

struct SubmissionBatch<
    'scope,
    'app,
    A,
    R,
    Q: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    F,
> {
    router: &'scope mut ClientRouter<R>,
    owner: &'scope mut EffectOwner<Q, T, E>,
    application: F,
    validation: SubmissionValidation<'app, A>,
    draining: bool,
}
impl<'app, A, R, Q, T, E, F> ProposalIngress for SubmissionBatch<'_, 'app, A, R, Q, T, E, F>
where
    A: ProposalAdmission<Receipt = R> + 'app,
    R: ApplicationReceipt,
    Q: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    F: Fn(GroupIdentity) -> Option<&'app A>,
{
    fn ready_leader(&self, group: GroupIdentity) -> bool {
        self.owner.core(group).is_some_and(|core| {
            core.role() == Role::Leader
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
        })
    }
    fn submit(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        // Leave the scope empty until complete successful ownership. Even a
        // provider/lookup panic caught inside the callback drops all temporary
        // reservations instead of poisoning a subsequent original.
        let mut validation = std::mem::take(&mut self.validation);
        if self.draining {
            return Err(ClientRejected {
                reason: ClientError::Draining,
                request,
            });
        }
        let Some(application) = (self.application)(request.group) else {
            return Err(ClientRejected {
                reason: ClientError::UnknownGroup,
                request,
            });
        };
        let result =
            self.router
                .submit_validated(self.owner, application, request, Some(&mut validation));
        if result.is_ok() {
            self.validation = validation;
        }
        result
    }
}
impl<R: ApplicationReceipt> ClientRouter<R> {
    /// Lend an ingress capability for sequential originals against unchanged
    /// owner/application state. Successful admissions remain owned if a later
    /// original fails or the callback unwinds. Temporary validation drops on
    /// return; application/poll/advance authority cannot escape through it.
    pub fn with_submission_batch<
        'app,
        A: ProposalAdmission<Receipt = R> + 'app,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        application: impl Fn(GroupIdentity) -> Option<&'app A>,
        callback: impl FnOnce(&mut dyn ProposalIngress) -> O,
    ) -> O {
        self.with_submission_policy(owner, application, false, callback)
    }
    pub(crate) fn with_submission_policy<
        'app,
        A: ProposalAdmission<Receipt = R> + 'app,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        application: impl Fn(GroupIdentity) -> Option<&'app A>,
        draining: bool,
        callback: impl FnOnce(&mut dyn ProposalIngress) -> O,
    ) -> O {
        callback(&mut SubmissionBatch {
            router: self,
            owner,
            application,
            validation: SubmissionValidation::default(),
            draining,
        })
    }
}
