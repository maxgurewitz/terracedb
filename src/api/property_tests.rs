use std::{collections::BTreeMap, sync::Arc};

use proptest::{prelude::*, prop_oneof};

use super::{
    CommitId, SequenceNumber, WatermarkRegistry, WatermarkSubscriptionSet, decode_mvcc_key,
    encode_mvcc_key,
};

proptest! {
    #[test]
    fn mvcc_keys_round_trip_and_sort_newest_first(
        user_key in prop::collection::vec(any::<u8>(), 0..32),
        sequences in prop::collection::vec(any::<u64>(), 1..16),
    ) {
        let mut encoded = sequences
            .iter()
            .map(|sequence| encode_mvcc_key(&user_key, CommitId::new(SequenceNumber::new(*sequence))))
            .collect::<Vec<_>>();

        for (encoded_key, original_sequence) in encoded.iter().zip(sequences.iter()) {
            let (decoded_key, commit_id) = decode_mvcc_key(encoded_key).expect("decode mvcc key");
            prop_assert_eq!(decoded_key, user_key.clone());
            prop_assert_eq!(commit_id.sequence(), SequenceNumber::new(*original_sequence));
        }

        encoded.sort();
        let decoded_sequences = encoded
            .iter()
            .map(|encoded_key| {
                decode_mvcc_key(encoded_key)
                    .expect("decode sorted mvcc key")
                    .1
                    .sequence()
            })
            .collect::<Vec<_>>();

        let mut expected = sequences
            .iter()
            .copied()
            .map(SequenceNumber::new)
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| right.cmp(left));

        prop_assert_eq!(decoded_sequences, expected);
    }
}

proptest! {
    #[test]
    fn watermark_subscription_sets_emit_only_monotonic_per_table_advances(
        updates in prop::collection::vec(
            (
                prop_oneof![Just("alpha".to_string()), Just("beta".to_string())],
                any::<u64>(),
            ),
            0..32,
        ),
    ) {
        let registry = Arc::new(WatermarkRegistry::new(BTreeMap::from([
            ("alpha".to_string(), SequenceNumber::default()),
            ("beta".to_string(), SequenceNumber::default()),
        ])));
        let mut subscriptions =
            WatermarkSubscriptionSet::new(&registry, vec!["beta".to_string(), "alpha".to_string()]);

        let mut expected = BTreeMap::from([
            ("alpha".to_string(), SequenceNumber::default()),
            ("beta".to_string(), SequenceNumber::default()),
        ]);
        let mut emitted = expected.clone();

        prop_assert!(subscriptions.pending_updates().is_empty());

        for (table, raw_sequence) in updates {
            let sequence = SequenceNumber::new(raw_sequence);
            registry.notify(&BTreeMap::from([(table.clone(), sequence)]));

            let current = expected
                .get_mut(&table)
                .expect("table should exist in expectations");
            if sequence > *current {
                *current = sequence;
            }

            let drained = subscriptions.pending_updates();
            let mut sorted = drained.clone();
            sorted.sort_by(|left, right| left.table.cmp(&right.table));
            prop_assert_eq!(drained.as_slice(), sorted.as_slice());

            for update in drained {
                let previous = emitted
                    .get(&update.table)
                    .copied()
                    .expect("table should exist in emitted map");
                prop_assert!(update.sequence > previous);
                prop_assert_eq!(update.sequence, expected[&update.table]);
                emitted.insert(update.table, update.sequence);
            }
        }
    }
}
