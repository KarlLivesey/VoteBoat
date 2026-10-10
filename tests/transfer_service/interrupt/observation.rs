// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use voteboat::{identity::*, transfer::*};

pub(super) fn ready(
    node: u16,
    reply: Result<String, (String, String)>,
) -> Result<Option<u16>, String> {
    let text = match reply {
        Ok(text) => text,
        Err((text, error))
            if text.is_empty()
                && error.trim_end()
                    == "Error: \"group unavailable; original operation remains unresolved\"" =>
        {
            return Ok(None)
        }
        Err((text, error)) => return Err(format!("metadata preparation: {text:?} {error}")),
    };
    let hex = text
        .trim_end()
        .strip_prefix("OK observation ")
        .ok_or("missing quorum observation")?;
    if hex.len() > 2 * MAX_TRANSFER_OBSERVATION_BYTES || !hex.len().is_multiple_of(2) {
        return Err("observation hex budget".into());
    }
    let bytes = hex
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            std::str::from_utf8(pair)
                .map_err(|e| e.to_string())
                .and_then(|p| u8::from_str_radix(p, 16).map_err(|e| e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let observation = TransferObservation::decode_authenticated(&bytes)
        .map_err(|e| format!("observation: {e:?}"))?;
    let expected = TransferRead {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        kind: TransferReadKind::Intent,
    };
    if observation.read() != expected {
        return Err("wrong metadata readiness query".into());
    }
    Ok(Some(node))
}

pub(super) fn released(reply: Result<String, (String, String)>) -> Result<Option<()>, String> {
    let text =
        reply.map_err(|(text, error)| format!("status after disconnect: {text:?} {error}"))?;
    if !text.starts_with("OK ") || text.lines().count() != 1 {
        return Err(format!("invalid cleanup status: {text:?}"));
    }
    let mut fields = text
        .split_whitespace()
        .filter_map(|word| word.strip_prefix("pending_reads="));
    let count = fields
        .next()
        .ok_or("missing pending read usage")?
        .parse::<usize>()
        .map_err(|e| e.to_string())?;
    if fields.next().is_some() {
        return Err("duplicate pending read usage".into());
    }
    Ok((count == 0).then_some(()))
}
