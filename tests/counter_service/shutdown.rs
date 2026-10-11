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
use std::time::{Duration, Instant};

const PREFIX: &str = "shutdown waiting_for_peer_close_ms=";
const ORDINARY: Duration = Duration::from_secs(15);
const JOIN_MARGIN: Duration = Duration::from_secs(5);

fn reported_interval(line: &str) -> Option<Duration> {
    let decimal = line.strip_suffix('\n')?.strip_prefix(PREFIX)?;
    if decimal.is_empty() || !decimal.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let millis = decimal.parse::<u64>().ok()?;
    (millis > 0).then(|| Duration::from_millis(millis))
}

fn reported_deadline(observed: Instant, interval: Duration) -> Option<Instant> {
    observed.checked_add(interval.checked_add(JOIN_MARGIN)?)
}

#[derive(Clone, Copy)]
pub(super) struct Window {
    ordinary: Instant,
    reported: Option<Instant>,
}
impl Window {
    pub(super) fn new(started: Instant) -> Self {
        Self {
            ordinary: started
                .checked_add(ORDINARY)
                .expect("representable ordinary process shutdown deadline"),
            reported: None,
        }
    }
    pub(super) fn observe(&mut self, now: Instant, log: &str) {
        if self.reported.is_some() {
            return;
        }
        self.reported = log.split_inclusive('\n').find_map(|line| {
            reported_interval(line).and_then(|interval| reported_deadline(now, interval))
        });
    }
    fn deadline(self) -> Instant {
        self.reported
            .map_or(self.ordinary, |end| end.max(self.ordinary))
    }
    pub(super) fn permits(self, now: Instant) -> bool {
        now < self.deadline()
    }
}

#[test]
fn exact_complete_positive_report_parses_without_consuming_partial_lines() {
    assert_eq!(
        reported_interval("shutdown waiting_for_peer_close_ms=6001\n"),
        Some(Duration::from_millis(6001))
    );
    assert_eq!(
        reported_interval("shutdown waiting_for_peer_close_ms=0001\n"),
        Some(Duration::from_millis(1))
    );
    assert!(reported_interval("shutdown waiting_for_peer_close_ms=6001").is_none());
}

#[test]
fn malformed_zero_and_decimal_overflow_reports_grant_no_window() {
    for line in [
        "shutdown waiting_for_peer_close_ms=\n",
        "shutdown waiting_for_peer_close_ms=0\n",
        "shutdown waiting_for_peer_close_ms=+1\n",
        "shutdown waiting_for_peer_close_ms=-1\n",
        "shutdown waiting_for_peer_close_ms=1.0\n",
        "shutdown waiting_for_peer_close_ms=1 \n",
        "shutdown waiting_for_peer_close_ms=1 extra\n",
        "shutdown waiting_for_peer_close_ms=１\n",
        "shutdown waiting_for_peer_close_ms=18446744073709551616\n",
        "other shutdown waiting_for_peer_close_ms=1\n",
    ] {
        assert!(reported_interval(line).is_none(), "{line:?}");
        let start = Instant::now();
        let mut window = Window::new(start);
        window.observe(start, line);
        assert_eq!(window.deadline(), start + ORDINARY);
        assert!(window.reported.is_none());
    }
}

#[test]
fn ordinary_cap_and_equality_are_preserved_for_short_reports() {
    let start = Instant::now();
    let mut window = Window::new(start);
    assert!(window.permits(start + ORDINARY - Duration::from_millis(1)));
    assert!(!window.permits(start + ORDINARY));
    window.observe(start, "shutdown waiting_for_peer_close_ms=1\n");
    assert_eq!(window.deadline(), start + ORDINARY);
    let mut equal = Window::new(start);
    equal.observe(
        start + Duration::from_secs(9),
        "shutdown waiting_for_peer_close_ms=1000\n",
    );
    assert_eq!(equal.deadline(), start + ORDINARY);
    assert!(!equal.permits(start + ORDINARY));
}

#[test]
fn repeated_and_fresh_reports_cannot_renew_the_first_pinned_window() {
    let start = Instant::now();
    let mut window = Window::new(start);
    let first = "shutdown waiting_for_peer_close_ms=6000\n";
    window.observe(start + Duration::from_secs(10), first);
    let deadline = start + Duration::from_secs(21);
    assert_eq!(window.deadline(), deadline);
    window.observe(start + Duration::from_secs(12), first);
    window.observe(
        start + Duration::from_secs(13),
        "shutdown waiting_for_peer_close_ms=900000\n",
    );
    assert_eq!(window.deadline(), deadline);
    assert!(window.permits(deadline - Duration::from_millis(1)));
    assert!(!window.permits(deadline));
}

#[test]
fn checked_margin_and_instant_overflow_refuse_instead_of_wrapping() {
    let start = Instant::now();
    assert!(reported_deadline(start, Duration::MAX).is_none());
    let instant_overflow = Duration::from_secs(u64::MAX - JOIN_MARGIN.as_secs());
    assert!(instant_overflow.checked_add(JOIN_MARGIN).is_some());
    assert!(reported_deadline(start, instant_overflow).is_none());
}

#[test]
fn sibling_windows_are_independent_and_only_a_valid_report_can_pin() {
    let start = Instant::now();
    let mut windows = [Window::new(start); 3];
    let observed = start + Duration::from_secs(10);
    windows[0].observe(
        observed,
        "shutdown waiting_for_peer_close_ms=oops\n\
shutdown waiting_for_peer_close_ms=12000\n\
shutdown waiting_for_peer_close_ms=999000\n",
    );
    windows[1].observe(observed, "shutdown waiting_for_peer_close_ms=oops\n");
    windows[2].observe(observed, "shutdown waiting_for_peer_close_ms=12000");
    assert!(windows[0].permits(start + ORDINARY));
    assert!(!windows[1].permits(start + ORDINARY));
    assert!(!windows[2].permits(start + ORDINARY));
    windows[2].observe(observed, "shutdown waiting_for_peer_close_ms=12000\n");
    assert_eq!(windows[2].deadline(), windows[0].deadline());
    assert!(!windows[1].permits(start + ORDINARY));
}
