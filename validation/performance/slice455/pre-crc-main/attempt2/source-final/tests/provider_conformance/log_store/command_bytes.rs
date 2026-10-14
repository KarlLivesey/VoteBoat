// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Downstream immutable command ownership and complete-content contracts.
use super::*;

#[test]
fn construction_reads_and_default_expose_exact_content() {
    fn public_traits<T: Clone + Eq + std::fmt::Debug + Send + Sync>() {}
    public_traits::<CommandBytes>();
    let input = [0, 1, 127, 255];
    for bytes in [CommandBytes::from(input.as_slice()), input.to_vec().into()] {
        assert_eq!(bytes.as_slice(), input);
        assert_eq!(AsRef::<[u8]>::as_ref(&bytes), input);
        assert_eq!(&*bytes, &input);
        assert_eq!(format!("{bytes:?}"), format!("{input:?}"));
        assert_eq!(bytes.to_vec(), input);
    }
    for empty in [
        CommandBytes::default(),
        Vec::with_capacity(31).into(),
        (&[][..]).into(),
    ] {
        assert!(empty.is_empty());
        assert_eq!(empty, CommandBytes::default());
        assert_eq!(empty.retained_bytes(), 0);
    }
}

#[test]
fn shared_clone_survives_source_drop_and_replacement_is_independent() {
    let original = CommandBytes::from(vec![1, 2, 3]);
    let clone = original.clone();
    assert_eq!(original.as_ptr(), clone.as_ptr());
    let mut mutable = clone.to_vec();
    mutable[1] = 7;
    mutable.push(255);
    assert_eq!(original.as_slice(), &[1, 2, 3]);
    assert_eq!(clone.as_slice(), &[1, 2, 3]);
    let replaced: CommandBytes = mutable.into();
    assert_eq!(replaced.as_slice(), &[1, 7, 3, 255]);
    assert_ne!(replaced, clone);
    drop(original);
    assert_eq!(clone.as_slice(), &[1, 2, 3]);
    drop(clone);
    assert_eq!(replaced.as_slice(), &[1, 7, 3, 255]);
}

#[test]
fn independent_content_and_complete_entry_fields_determine_equality() {
    let left = CommandBytes::from(vec![1, 2, 255]);
    let equal = CommandBytes::from([1, 2, 255].as_slice());
    assert_ne!(left.as_ptr(), equal.as_ptr());
    assert_eq!(left, equal);
    for different in [&[1, 2][..], &[1, 2, 254][..], &[][..]] {
        assert_ne!(left, CommandBytes::from(different));
    }
    let original = LogEntry {
        index: u64::MAX,
        term: 7,
        payload: EntryPayload::Command {
            operation: OperationId::new((1 << 127) | 7).unwrap(),
            bytes: left,
        },
    };
    let mut independent = original.clone();
    let EntryPayload::Command { bytes, .. } = &mut independent.payload else {
        unreachable!()
    };
    *bytes = equal;
    assert_eq!(original, independent);
    let changes: &[fn(&mut LogEntry)] = &[
        |e| e.index -= 1,
        |e| e.term += 1,
        |e| {
            let EntryPayload::Command { operation, .. } = &mut e.payload else {
                unreachable!()
            };
            *operation = OperationId::new(7).unwrap();
        },
        |e| {
            let EntryPayload::Command { bytes, .. } = &mut e.payload else {
                unreachable!()
            };
            *bytes = vec![1, 2, 254].into();
        },
    ];
    for change in changes {
        let mut altered = independent.clone();
        change(&mut altered);
        assert_ne!(original, altered);
        assert_eq!(original.index, u64::MAX);
        assert_eq!(original.term, 7);
    }
}

#[test]
fn every_retained_clone_is_charged_without_source_spare_capacity() {
    let mut input = Vec::with_capacity(257);
    input.extend([1, 2, 3]);
    assert!(input.capacity() > input.len());
    let bytes: CommandBytes = input.into();
    let charge = 3 + 2 * std::mem::size_of::<usize>();
    assert_eq!(bytes.retained_bytes(), charge);
    let clones = [bytes.clone(), bytes.clone(), bytes.clone()];
    assert_eq!(
        clones
            .iter()
            .map(CommandBytes::retained_bytes)
            .sum::<usize>(),
        3 * charge
    );
    assert!(clones.iter().all(|clone| clone.as_ptr() == bytes.as_ptr()));
    assert_eq!(CommandBytes::default().clone().retained_bytes(), 0);
}
