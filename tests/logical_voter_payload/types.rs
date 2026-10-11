// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Tag {
    X,
    Y,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Entry {
    pub term: u8,
    pub tag: Tag,
    pub bytes: Option<[u8; 3]>,
}
impl Entry {
    pub fn exact(tag: Tag) -> Self {
        Self {
            term: 1,
            tag,
            bytes: Some(match tag {
                Tag::X => [88, 0, 1],
                Tag::Y => [89, 0, 2],
            }),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ParentIndex(pub u8);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChildIndex(pub usize);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Header {
    pub operation: u8,
    pub epoch: u8,
    pub generation: u8,
    pub revision: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Kind {
    Claim1,
    Promise,
    AppendX,
    Commit1,
    Meta0,
    Bytes0,
    Publish0,
    Anchor0,
    Tentative,
    Replace,
    Commit2,
    Meta1,
    Bytes1,
    Publish1,
    Anchor1,
    Reclaim0,
    Claim2,
    Serve2,
    FillX,
}
pub const PROGRAM: [Kind; 18] = [
    Kind::Claim1,
    Kind::Promise,
    Kind::AppendX,
    Kind::Commit1,
    Kind::Meta0,
    Kind::Bytes0,
    Kind::Publish0,
    Kind::Anchor0,
    Kind::Tentative,
    Kind::Replace,
    Kind::Commit2,
    Kind::Meta1,
    Kind::Bytes1,
    Kind::Publish1,
    Kind::Anchor1,
    Kind::Reclaim0,
    Kind::Claim2,
    Kind::Serve2,
];
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Record {
    pub header: Header,
    pub kind: Kind,
    pub body: Option<[u8; 3]>,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ParentBytes {
    pub at: ParentIndex,
    pub entries: [Option<Entry>; 2],
    pub term: u8,
    pub vote: Option<u8>,
    pub configuration: u8,
    pub epoch: u8,
    pub generation: u8,
    pub dedup: u8,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Snapshot {
    pub at: ParentIndex,
    pub bytes: Option<ParentBytes>,
    pub published: bool,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Image {
    pub term: u8,
    pub vote: Option<u8>,
    pub configuration: u8,
    pub epoch: u8,
    pub generation: u8,
    pub revision: u8,
    pub base: ParentIndex,
    pub entries: [Option<Entry>; 2],
    pub commit: ParentIndex,
    pub snapshots: [Option<Snapshot>; 2],
    pub pins: [bool; 2],
    pub anchor: Option<usize>,
    pub dedup: u8,
    pub used: u32,
    pub receipts: Vec<Completion>,
}
impl Default for Image {
    fn default() -> Self {
        Self {
            term: 0,
            vote: None,
            configuration: 1,
            epoch: 0,
            generation: 0,
            revision: 0,
            base: ParentIndex(0),
            entries: [None; 2],
            commit: ParentIndex(0),
            snapshots: [None, None],
            pins: [false; 2],
            anchor: None,
            dedup: 0,
            used: 0,
            receipts: Vec::new(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ChildImage {
    pub at: ChildIndex,
    pub image: Image,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Physical {
    pub written: Vec<Record>,
    pub synced: Vec<Record>,
    pub staged: Option<ChildImage>,
    pub published: Option<ChildImage>,
    pub anchor: Option<ChildImage>,
}
impl Physical {
    pub fn base(&self) -> usize {
        self.anchor.as_ref().map_or(0, |c| c.at.0)
    }
    pub fn end(&self) -> usize {
        self.base() + self.synced.len()
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CellWitness {
    pub logical: u8,
    pub written: [Vec<Entry>; 3],
    pub synced: [Vec<Entry>; 3],
    pub commit: ChildIndex,
    pub applied: ChildIndex,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Completion {
    pub header: Header,
    pub position: ChildIndex,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Weakening {
    None,
    MissingPayload,
    EarlyAnchor,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Admit,
    Submit(Kind),
    Reference,
    Disconnect(usize),
    Reconnect(usize),
    Write(usize),
    Sync(usize),
    Commit,
    Apply,
    Observe,
    Lose,
    Quorum,
    Resolve,
    Recover(usize),
    CheckpointStage(usize),
    CheckpointSync(usize),
    CheckpointAnchor(usize),
    Power,
    Process,
    Ack(usize),
}
