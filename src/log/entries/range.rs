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
//! A borrowed interval of a canonical log.
use super::{Iter, LogEntry, Span};
use std::{borrow::Cow, fmt};

/// Borrowed ordered interval of a canonical log, such as newly committed
/// entries. It may span several pieces; [`Self::contiguous`] borrows a single
/// piece and copies otherwise.
#[derive(Clone, Copy)]
pub struct EntryRange<'a> {
    /// Visible part of the first piece, whole pieces, visible part of the
    /// last piece (empty when the interval lies in one piece).
    front: &'a [LogEntry],
    middle: &'a [Span],
    back: &'a [LogEntry],
    len: usize,
}
impl<'a> EntryRange<'a> {
    pub(super) const EMPTY: EntryRange<'static> = EntryRange {
        front: &[],
        middle: &[],
        back: &[],
        len: 0,
    };
    /// One contiguous slice.
    pub(super) fn single(entries: &'a [LogEntry]) -> Self {
        Self {
            front: entries,
            middle: &[],
            back: &[],
            len: entries.len(),
        }
    }
    /// Visible `len` entries of `spans` after skipping `head` and cutting
    /// `tail`; the caller checked the bounds.
    pub(super) fn new(spans: &'a [Span], head: usize, tail: usize, len: usize) -> Self {
        match spans {
            _ if len == 0 => EntryRange::EMPTY,
            [] => EntryRange::EMPTY,
            [only] => Self {
                front: &only.entries()[head..only.len() - tail],
                middle: &[],
                back: &[],
                len,
            },
            [first, middle @ .., last] => Self {
                front: &first.entries()[head..],
                middle,
                back: &last.entries()[..last.len() - tail],
                len,
            },
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn first(&self) -> Option<&'a LogEntry> {
        self.front.first()
    }
    pub fn last(&self) -> Option<&'a LogEntry> {
        self.back.last().or_else(|| self.front.last())
    }
    pub fn get(&self, offset: usize) -> Option<&'a LogEntry> {
        if offset < self.front.len() {
            return self.front.get(offset);
        }
        let mut offset = offset - self.front.len();
        for span in self.middle {
            if offset < span.len() {
                return span.entries().get(offset);
            }
            offset -= span.len();
        }
        self.back.get(offset)
    }
    pub fn iter(&self) -> Iter<'a> {
        Iter::from_parts(self.front, self.middle, self.back, self.len)
    }
    /// The whole interval as one slice, when it lies in one piece.
    pub fn as_slice(&self) -> Option<&'a [LogEntry]> {
        (self.middle.is_empty() && self.back.is_empty()).then_some(self.front)
    }
    /// Contiguous pieces in order; their concatenation is this interval.
    pub fn slices(&self) -> Slices<'a> {
        Slices {
            front: (!self.front.is_empty()).then_some(self.front),
            middle: self.middle.iter(),
            back: (!self.back.is_empty()).then_some(self.back),
        }
    }
    /// Borrowed when the interval lies in one piece, otherwise one copy.
    pub fn contiguous(&self) -> Cow<'a, [LogEntry]> {
        if self.middle.is_empty() && self.back.is_empty() {
            Cow::Borrowed(self.front)
        } else {
            Cow::Owned(self.to_vec())
        }
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        let mut entries = Vec::with_capacity(self.len);
        for slice in self.slices() {
            entries.extend_from_slice(slice);
        }
        entries
    }
}
/// Contiguous pieces of an [`EntryRange`], in order.
#[derive(Clone, Debug)]
pub struct Slices<'a> {
    front: Option<&'a [LogEntry]>,
    middle: std::slice::Iter<'a, Span>,
    back: Option<&'a [LogEntry]>,
}
impl<'a> Slices<'a> {
    pub(super) fn single(entries: &'a [LogEntry]) -> Self {
        Self {
            front: (!entries.is_empty()).then_some(entries),
            middle: [].iter(),
            back: None,
        }
    }
    pub(super) fn pieces(spans: &'a [Span]) -> Self {
        Self {
            front: None,
            middle: spans.iter(),
            back: None,
        }
    }
}
impl<'a> Iterator for Slices<'a> {
    type Item = &'a [LogEntry];
    fn next(&mut self) -> Option<&'a [LogEntry]> {
        self.front
            .take()
            .or_else(|| self.middle.next().map(Span::entries))
            .or_else(|| self.back.take())
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = usize::from(self.front.is_some())
            + self.middle.len()
            + usize::from(self.back.is_some());
        (count, Some(count))
    }
}
impl ExactSizeIterator for Slices<'_> {}
impl<'a> DoubleEndedIterator for Slices<'a> {
    fn next_back(&mut self) -> Option<&'a [LogEntry]> {
        self.back
            .take()
            .or_else(|| self.middle.next_back().map(Span::entries))
            .or_else(|| self.front.take())
    }
}
impl std::iter::FusedIterator for Slices<'_> {}
impl<'a> IntoIterator for EntryRange<'a> {
    type Item = &'a LogEntry;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &EntryRange<'a> {
    type Item = &'a LogEntry;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl std::ops::Index<usize> for EntryRange<'_> {
    type Output = LogEntry;
    fn index(&self, offset: usize) -> &LogEntry {
        self.get(offset).expect("entry range offset out of bounds")
    }
}
impl fmt::Debug for EntryRange<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for EntryRange<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}
impl Eq for EntryRange<'_> {}
impl PartialEq<[LogEntry]> for EntryRange<'_> {
    fn eq(&self, other: &[LogEntry]) -> bool {
        self.len == other.len() && self.iter().eq(other)
    }
}
impl PartialEq<&[LogEntry]> for EntryRange<'_> {
    fn eq(&self, other: &&[LogEntry]) -> bool {
        *self == **other
    }
}
impl PartialEq<Vec<LogEntry>> for EntryRange<'_> {
    fn eq(&self, other: &Vec<LogEntry>) -> bool {
        *self == **other
    }
}
impl<const N: usize> PartialEq<[LogEntry; N]> for EntryRange<'_> {
    fn eq(&self, other: &[LogEntry; N]) -> bool {
        *self == other[..]
    }
}
