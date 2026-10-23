// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The NORMAL client producer/observer; the synchronous adapter uses prepare()
//! and the same public invocation's noncancelling try_take, not private replies.
use super::*;
use std::mem::size_of;

pub(super) enum Reply {
    Port(OriginalReply<OperationId>),
    Owned(oneshot::Sender<OperationId>),
}
pub(super) struct Original {
    pub operation: OperationId,
    pub bytes: Vec<u8>,
    pub reply: Reply,
}
pub(super) enum Request {
    Port(WriteSubmission<OperationId>),
    Owned { originals: Vec<Original> },
}
pub(super) enum Originals {
    Port(WriteSubmission<OperationId>),
    Owned(std::vec::IntoIter<Original>),
}
pub(super) enum Observer {
    Port(WriteInvocation<OperationId>),
    Owned(Vec<oneshot::Receiver<OperationId>>),
}
pub(crate) struct Client {
    pub(super) sender: mpsc::Sender<Request>,
    port: WritePort<OperationId>,
    inputs: Vec<WriteInput<'static>>,
}
pub(super) fn port_limits(originals: usize) -> Result<WritePortLimits, Failure> {
    let mut limits = WritePortLimits {
        originals,
        command_bytes: 1,
        command_capacity_bytes: originals,
        receipt_bytes: size_of::<OperationId>(),
        waker_bytes: 1024,
        retained_bytes: 0,
    };
    // Descriptor storage is caller-side but charged to this live registration.
    limits.retained_bytes = limits
        .required_bytes::<OperationId>()
        .map_err(debug_error)?
        .checked_add(
            originals
                .checked_mul(size_of::<WriteInput<'static>>())
                .ok_or("descriptor budget overflow")?,
        )
        .ok_or("port reservation overflow")?;
    Ok(limits)
}
impl Client {
    pub(super) fn new(
        sender: mpsc::Sender<Request>,
        factory: &WriteFactory<OperationId>,
        limits: WritePortLimits,
    ) -> Result<Self, Failure> {
        let mut inputs = Vec::new();
        inputs.try_reserve_exact(limits.originals)?;
        if inputs.capacity() > limits.originals {
            return Err("descriptor allocated capacity exceeds bound".into());
        }
        let port = factory.open_port(limits).map_err(debug_error)?;
        Ok(Self {
            sender,
            port,
            inputs,
        })
    }
    pub(super) fn prepare(&mut self, originals: &[u128]) -> Result<(Request, Observer), Failure> {
        if originals.is_empty() {
            return owned(originals);
        }
        if originals.len() > self.port.limits().originals {
            // Fully functional pre-admission fallback for larger callers that
            // the existing Actor supports. No split or late mechanism change.
            return owned(originals);
        }
        self.inputs.clear();
        for &original in originals {
            self.inputs.push(WriteInput {
                operation: OperationId::new(original).ok_or("zero original identity")?,
                bytes: &[],
            });
        }
        let (submission, invocation) = self.port.begin(&self.inputs).map_err(debug_error)?;
        Ok((Request::Port(submission), Observer::Port(invocation)))
    }
    pub async fn write(&mut self, originals: &[u128]) -> Result<usize, Failure> {
        let (request, observer) = self.prepare(originals)?;
        self.sender.send(request).await?;
        observer.wait(originals).await
    }
}
fn owned(originals: &[u128]) -> Result<(Request, Observer), Failure> {
    let mut entries = Vec::with_capacity(originals.len());
    let mut waits = Vec::with_capacity(originals.len());
    for &original in originals {
        let operation = OperationId::new(original).ok_or("zero original identity")?;
        let (reply, wait) = oneshot::channel();
        entries.push(Original {
            operation,
            bytes: Vec::new(),
            reply: Reply::Owned(reply),
        });
        waits.push(wait);
    }
    Ok((
        Request::Owned { originals: entries },
        Observer::Owned(waits),
    ))
}
impl Observer {
    async fn wait(self, originals: &[u128]) -> Result<usize, Failure> {
        match self {
            Self::Port(mut invocation) => {
                if invocation.len() != originals.len() {
                    return Err("original invocation cardinality differs".into());
                }
                for (ordinal, expected) in originals.iter().enumerate() {
                    let envelope = invocation
                        .waiter(ordinal)
                        .map_err(debug_error)?
                        .await
                        .map_err(debug_error)?;
                    if !invocation.matches(&envelope, ordinal)
                        || envelope.operation().get() != *expected
                    {
                        return Err("original envelope identity differs".into());
                    }
                    let OriginalOutcome::Applied { position, receipt } = envelope.into_outcome()
                    else {
                        return Err("channel closed".into());
                    };
                    if receipt.get() != *expected || position.index == 0 || position.term == 0 {
                        return Err("VoteBoat original applied result differs".into());
                    }
                }
                invocation.finish().map_err(debug_error)?;
            }
            Self::Owned(waits) => {
                if waits.len() != originals.len() {
                    return Err("owned original cardinality differs".into());
                }
                for (wait, expected) in waits.into_iter().zip(originals) {
                    if wait.await?.get() != *expected {
                        return Err("VoteBoat original applied result differs".into());
                    }
                }
            }
        }
        Ok(originals.len())
    }
}
impl Request {
    #[cfg(test)]
    pub fn owned_originals(&self) -> &[Original] {
        match self {
            Self::Owned { originals } => originals,
            Self::Port(_) => panic!("owned control requires an owned request"),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Port(s) => s.len(),
            Self::Owned { originals } => originals.len(),
        }
    }
    pub fn validate(&self, domain: &WriteDomain<OperationId>) -> Result<(), Failure> {
        if let Self::Port(submission) = self {
            domain
                .validate_submission(submission)
                .map_err(debug_error)?;
        }
        Ok(())
    }
    pub fn into_originals(self) -> Originals {
        match self {
            Self::Port(s) => Originals::Port(s),
            Self::Owned { originals } => Originals::Owned(originals.into_iter()),
        }
    }
}
impl Iterator for Originals {
    type Item = Result<Original, Failure>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Owned(items) => items.next().map(Ok),
            Self::Port(submission) => match submission.take_next() {
                Ok(Some((operation, bytes, reply))) => Some(Ok(Original {
                    operation,
                    bytes,
                    reply: Reply::Port(reply),
                })),
                Ok(None) => None,
                Err(error) => Some(Err(debug_error(error))),
            },
        }
    }
}
impl Reply {
    pub fn bind(
        &mut self,
        domain: &WriteDomain<OperationId>,
        entry: &LogEntry,
    ) -> Result<(), Failure> {
        if let Self::Port(reply) = self {
            reply.bind_proposal(domain, entry).map_err(debug_error)?;
        }
        Ok(())
    }
    pub fn publish(
        self,
        domain: &WriteDomain<OperationId>,
        entry: &LogEntry,
        operation: OperationId,
    ) -> Result<(), Failure> {
        match self {
            Self::Port(reply) => {
                reply
                    .publish_applied(domain, entry, operation, 0)
                    .map_err(|rejected| {
                        if rejected.reason == WriteError::ConsumerClosed {
                            "original client dropped result wait".into()
                        } else {
                            debug_error(rejected.reason)
                        }
                    })
            }
            Self::Owned(reply) => reply
                .send(operation)
                .map_err(|_| "original client dropped result wait".into()),
        }
    }
    #[cfg(test)]
    pub fn legacy_send(self, operation: OperationId) -> Result<(), Failure> {
        match self {
            Self::Owned(reply) => reply
                .send(operation)
                .map_err(|_| "original client dropped result wait".into()),
            Self::Port(_) => Err("public port requires actual committed-entry context".into()),
        }
    }
}

pub(super) fn registration_limits(
    settings: Settings,
) -> Result<(WritePortLimits, WriteDomainLimits), Failure> {
    let port_limits = writes::port_limits(settings.batch)?;
    let ports = settings
        .clients
        .checked_add(1)
        .ok_or("port count overflow")?;
    let domain_bytes = port_limits
        .retained_bytes
        .checked_mul(ports)
        .and_then(|n| n.checked_add(ports * 32 + 4096))
        .ok_or("port byte budget overflow")?;
    Ok((
        port_limits,
        WriteDomainLimits {
            ports,
            retained_bytes: domain_bytes,
        },
    ))
}
