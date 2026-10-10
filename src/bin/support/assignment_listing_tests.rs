// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn sample() -> View {
    View {
        node: NodeId::new(1).unwrap(),
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(9).unwrap(),
                incarnation: StoreIncarnation::new(2).unwrap(),
            },
            session: StoreSession::new(3).unwrap(),
        },
        rows: (1..=3)
            .map(|id| Row {
                group: GroupIdentity {
                    id: GroupId::new(id).unwrap(),
                    incarnation: GroupIncarnation::new(4).unwrap(),
                },
                accepted: ConfigurationId::new(5).unwrap(),
                stable: ConfigurationId::new(5).unwrap(),
                next: None,
            })
            .collect(),
    }
}
#[test]
fn cursor_rejects_changed_identity_inventory_and_configuration() {
    let original = sample();
    let cursor = format!("{}/1/4", original.fingerprint());
    assert!(original
        .page(&cursor, "1")
        .unwrap()
        .ends_with("assignments=2:4:5:5:-"));
    for change in 0..7 {
        let mut changed = sample();
        match change {
            0 => changed.store.session = StoreSession::new(4).unwrap(),
            1 => changed.store.identity.id = StoreId::new(10).unwrap(),
            2 => changed.store.identity.incarnation = StoreIncarnation::new(3).unwrap(),
            3 => changed.node = NodeId::new(2).unwrap(),
            4 => {
                changed.rows.pop();
            }
            5 => changed.rows[1].accepted = ConfigurationId::new(6).unwrap(),
            _ => changed.rows[1].next = Some(ConfigurationId::new(7).unwrap()),
        }
        assert!(changed.page(&cursor, "1").unwrap_err().contains("stale"));
    }
    for cursor in [
        "bad".to_owned(),
        format!("{}/1/4/extra", original.fingerprint()),
        format!("{}/0/4", original.fingerprint()),
        format!("{}/8/4", original.fingerprint()),
    ] {
        assert!(original.page(&cursor, "1").is_err());
    }
    for limit in ["0", "9", "-1", "invalid", "18446744073709551616"] {
        assert!(original.page("-", limit).is_err());
    }
    let end = format!("{}/3/4", original.fingerprint());
    assert!(original
        .page(&end, "1")
        .unwrap()
        .contains("rows=0 more=false next=-"));
}
#[test]
fn maximum_identity_page_and_inventory_are_bounded() {
    let mut view = sample();
    view.node = NodeId::new(u64::MAX).unwrap();
    view.store.identity.id = StoreId::new(u128::MAX).unwrap();
    view.store.identity.incarnation = StoreIncarnation::new(u64::MAX).unwrap();
    view.store.session = StoreSession::new(u64::MAX).unwrap();
    let row = Row {
        group: GroupIdentity {
            id: GroupId::new(u128::MAX).unwrap(),
            incarnation: GroupIncarnation::new(u64::MAX).unwrap(),
        },
        accepted: ConfigurationId::new(u64::MAX).unwrap(),
        stable: ConfigurationId::new(u64::MAX).unwrap(),
        next: Some(ConfigurationId::new(u64::MAX).unwrap()),
    };
    view.rows = (0..MAX_GROUPS)
        .map(|n| Row {
            group: GroupIdentity {
                id: GroupId::new(u128::MAX - MAX_GROUPS as u128 + n as u128).unwrap(),
                ..row.group
            },
            ..row
        })
        .collect();
    let text = view.page("-", "8").unwrap();
    assert!(text.len() + 1 < 4096);
    assert!(text.contains("groups=256 rows=8 more=true"));
    view.rows.push(row);
    assert!(view.page("-", "8").is_err());
}
