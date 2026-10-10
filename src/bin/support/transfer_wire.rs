// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::setup::{checked, Failure};
use std::{io::Read, path::Path};
use voteboat::{
    identity::RoutingSchemeId,
    routing::{BucketRange, PartitionScheme},
    scope::ScopeImage,
};
pub const COMMAND_BYTES: usize = 270 * 1024;
pub const APP_BYTES: usize = 65536;
pub fn file(path: &Path, max: usize) -> Result<String, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err("file budget exceeded".into());
    }
    Ok(String::from_utf8(bytes)?)
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn unhex(s: &str, max: usize) -> Result<Vec<u8>, Failure> {
    if !s.len().is_multiple_of(2) || s.len() / 2 > max {
        return Err("invalid hex length".into());
    }
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| {
            let h = (p[0] as char).to_digit(16).ok_or("invalid hex")?;
            let l = (p[1] as char).to_digit(16).ok_or("invalid hex")?;
            Ok((h * 16 + l) as u8)
        })
        .collect()
}
pub fn image(value: &ScopeImage) -> String {
    let mut bytes = Vec::new();
    bytes.extend(value.schema().to_le_bytes());
    bytes.extend(value.scheme().id.get().to_le_bytes());
    bytes.extend(value.scheme().version.to_le_bytes());
    bytes.extend(value.scope().start().to_le_bytes());
    bytes.extend(value.scope().end().to_le_bytes());
    bytes.extend(value.source_applied().to_le_bytes());
    bytes.extend(value.bytes());
    hex(&bytes)
}
pub fn read_image(s: &str) -> Result<ScopeImage, Failure> {
    let b = unhex(s, APP_BYTES)?;
    if b.len() < 41 {
        return Err("short scope image".into());
    }
    checked(ScopeImage::new(
        u64::from_le_bytes(b[..8].try_into()?),
        PartitionScheme {
            id: RoutingSchemeId::new(u128::from_le_bytes(b[8..24].try_into()?))
                .ok_or("invalid scheme")?,
            version: u32::from_le_bytes(b[24..28].try_into()?),
        },
        checked(BucketRange::new(
            u16::from_le_bytes(b[28..30].try_into()?),
            u16::from_le_bytes(b[30..32].try_into()?),
        ))?,
        u64::from_le_bytes(b[32..40].try_into()?),
        b[40..].to_vec(),
    ))
}
