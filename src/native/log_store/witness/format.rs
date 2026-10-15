// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{identity::StoreBinding, native::vote_store::crc32c};

pub(super) const SIZE: usize = 128;
const MAGIC: &[u8; 8] = b"VBWACK01";
const SELECTOR: &[u8; 8] = b"VBWCUR01";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub generation: u64,
    pub counter: u64,
    pub binding: StoreBinding,
    pub length: u64,
}
pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
impl Record {
    pub fn encode(self, role: u32) -> [u8; SIZE] {
        let mut bytes = [0; SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&role.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.generation.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.counter.to_le_bytes());
        bytes[32..48].copy_from_slice(&self.binding.identity.id.get().to_le_bytes());
        bytes[48..56].copy_from_slice(&self.binding.identity.incarnation.get().to_le_bytes());
        bytes[56..64].copy_from_slice(&self.binding.session.get().to_le_bytes());
        bytes[64..72].copy_from_slice(&self.length.to_le_bytes());
        let crc = crc32c(&bytes[..124]);
        bytes[124..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }
}
pub(super) fn read_record(
    file: &mut File,
    role: u32,
    generation: u64,
    exact: bool,
) -> io::Result<Option<Record>> {
    if exact && file.metadata()?.len() != SIZE as u64 {
        return Err(invalid("acknowledgement file length"));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.take(SIZE as u64).read_to_end(&mut bytes)?;
    decode(&bytes, role, generation)
}
pub(super) fn decode(bytes: &[u8], role: u32, generation: u64) -> io::Result<Option<Record>> {
    if bytes.len() != SIZE
        || crc32c(&bytes[..124]) != u32::from_le_bytes(bytes[124..].try_into().unwrap())
    {
        return Ok(None);
    }
    if &bytes[..8] != MAGIC
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != 1
        || u32::from_le_bytes(bytes[12..16].try_into().unwrap()) != role
        || bytes[72..124].iter().any(|&b| b != 0)
    {
        return Err(invalid("witness format/role/reserved"));
    }
    let n = |start| u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
    if n(16) != generation || n(24) == 0 {
        return Err(invalid("witness generation/counter"));
    }
    let mut canonical = b"VBLSTR02".to_vec();
    canonical.extend_from_slice(&bytes[32..72]);
    canonical.extend(crc32c(&canonical).to_le_bytes());
    let (binding, length) =
        super::super::read_manifest(&canonical).map_err(|_| invalid("witness identity/session"))?;
    Ok(Some(Record {
        generation,
        counter: n(24),
        binding,
        length,
    }))
}
pub(super) fn mandatory(a: Option<Record>, b: Option<Record>) -> io::Result<Record> {
    match (a, b) {
        (None, None) => Err(invalid("both publication witnesses invalid")),
        (Some(r), None) | (None, Some(r)) => Ok(r),
        (Some(a), Some(b)) if a.counter == b.counter && a == b => Ok(b),
        (Some(a), Some(b)) => {
            let same_or_next = a.binding.session == b.binding.session
                || b.binding.session.get().checked_add(1) == Some(a.binding.session.get());
            if a.generation != b.generation
                || a.binding.identity != b.binding.identity
                || a.counter <= b.counter
                || a.length < b.length
                || !same_or_next
            {
                return Err(invalid("contradictory publication witnesses"));
            }
            Ok(b)
        }
    }
}
pub(super) fn selector(generation: u64) -> Vec<u8> {
    let mut bytes = SELECTOR.to_vec();
    bytes.extend(generation.to_le_bytes());
    bytes.extend(crc32c(&bytes).to_le_bytes());
    bytes
}
pub(super) fn selection(bytes: &[u8]) -> io::Result<u64> {
    if bytes.len() != 20
        || &bytes[..8] != SELECTOR
        || crc32c(&bytes[..16]) != u32::from_le_bytes(bytes[16..].try_into().unwrap())
    {
        return Err(invalid("witness selection corrupt"));
    }
    Ok(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}
