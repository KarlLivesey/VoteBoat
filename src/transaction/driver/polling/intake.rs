// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > TransferDriver<'_, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    pub(super) fn issue(&mut self, work: &mut Work) -> Result<(), DriverError> {
        if work.stage == DriverStage::CoordinatorStatus {
            let group = self.profile.coordinator();
            let Some(node) = self.route(group, work.route) else {
                return Ok(());
            };
            let query = TransferQuery::Status(work.ticket.transaction);
            match self.nodes[node].read(group, query) {
                Ok(ticket) => {
                    work.attempt = Some(Attempt::Read {
                        node,
                        ticket,
                        query,
                    })
                }
                Err(error) => {
                    work.route = work.route.wrapping_add(1);
                    if !retry_read(&error.reason) {
                        return Err(DriverError::Read {
                            node,
                            reason: Box::new(error.reason),
                        });
                    }
                }
            }
            return Ok(());
        }
        let (group, command) = work.command()?;
        let operation = command.operation(group).map_err(DriverError::Application)?;
        let bytes = codec::encode(command).map_err(DriverError::Application)?;
        let Some(node) = self.route(group, work.route) else {
            return Ok(());
        };
        match self.nodes[node].propose(ClientRequest {
            group,
            operation,
            bytes: bytes.clone(),
        }) {
            Ok(ticket) => {
                work.attempt = Some(Attempt::Client {
                    node,
                    ticket,
                    command: Box::new(command),
                    bytes,
                })
            }
            Err(error) => {
                work.route = work.route.wrapping_add(1);
                if !retry_client(&error.reason) {
                    return Err(DriverError::Client {
                        node,
                        reason: Box::new(error.reason),
                    });
                }
            }
        }
        Ok(())
    }
}
pub(super) fn retry_raft(error: &RaftError) -> bool {
    matches!(
        error,
        RaftError::Busy
            | RaftError::NotLeader
            | RaftError::ReadNotReady
            | RaftError::ReadInFlight
            | RaftError::StaleRead
            | RaftError::NotApplied
    )
}
fn retry_client(error: &ClientError) -> bool {
    matches!(error, ClientError::Overloaded)
        || matches!(error,ClientError::Consensus(reason) if retry_raft(reason))
}
fn retry_read(error: &ReadInvocationError) -> bool {
    matches!(
        error,
        ReadInvocationError::Overloaded | ReadInvocationError::InFlight
    ) || matches!(error,ReadInvocationError::Consensus(reason) if retry_raft(reason))
}
