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
//! Ordered read-only iteration across the pieces of a sequence.
use super::{LogEntry, Span};
use std::{iter::FusedIterator, slice};

/// Iterator over the visible entries of a batch, log or range, in order.
#[derive(Clone, Debug)]
pub struct Iter<'a> {
    front: slice::Iter<'a, LogEntry>,
    middle: slice::Iter<'a, Span>,
    back: slice::Iter<'a, LogEntry>,
    remaining: usize,
}
impl<'a> Iter<'a> {
    /// `head` entries are skipped in the first span and `tail` cut from the
    /// last; `len` is the visible total. The caller checked these bounds.
    pub(super) fn new(spans: &'a [Span], head: usize, tail: usize, len: usize) -> Self {
        let empty: &'a [LogEntry] = &[];
        match spans {
            [] => Self {
                front: empty.iter(),
                middle: [].iter(),
                back: empty.iter(),
                remaining: 0,
            },
            [only] => Self {
                front: only.entries()[head..only.len() - tail].iter(),
                middle: [].iter(),
                back: empty.iter(),
                remaining: len,
            },
            [first, middle @ .., last] => Self {
                front: first.entries()[head..].iter(),
                middle: middle.iter(),
                back: last.entries()[..last.len() - tail].iter(),
                remaining: len,
            },
        }
    }
}
impl<'a> Iter<'a> {
    /// Precomputed visible first/last parts around whole middle pieces.
    pub(super) fn from_parts(
        front: &'a [LogEntry],
        middle: &'a [Span],
        back: &'a [LogEntry],
        len: usize,
    ) -> Self {
        Self {
            front: front.iter(),
            middle: middle.iter(),
            back: back.iter(),
            remaining: len,
        }
    }
}
impl<'a> Iterator for Iter<'a> {
    type Item = &'a LogEntry;
    fn next(&mut self) -> Option<&'a LogEntry> {
        loop {
            if let Some(entry) = self.front.next() {
                self.remaining -= 1;
                return Some(entry);
            }
            match self.middle.next() {
                Some(span) => self.front = span.entries().iter(),
                None => {
                    let entry = self.back.next()?;
                    self.remaining -= 1;
                    return Some(entry);
                }
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<&'a LogEntry> {
        loop {
            if let Some(entry) = self.back.next_back() {
                self.remaining -= 1;
                return Some(entry);
            }
            match self.middle.next_back() {
                Some(span) => self.back = span.entries().iter(),
                None => {
                    let entry = self.front.next_back()?;
                    self.remaining -= 1;
                    return Some(entry);
                }
            }
        }
    }
}
impl ExactSizeIterator for Iter<'_> {}
impl FusedIterator for Iter<'_> {}
