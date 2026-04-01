use std::{collections::BTreeSet, fmt::Debug};

pub fn assert_non_decreasing<T>(label: &str, values: &[T])
where
    T: Ord + Debug,
{
    for window in values.windows(2) {
        assert!(
            window[0] <= window[1],
            "{label} should be non-decreasing: left={:?}, right={:?}",
            window[0],
            window[1]
        );
    }
}

pub fn assert_unique<T>(label: &str, values: impl IntoIterator<Item = T>)
where
    T: Ord + Debug,
{
    let mut seen = BTreeSet::new();
    for value in values {
        assert!(seen.insert(value), "{label} should not contain duplicates");
    }
}
