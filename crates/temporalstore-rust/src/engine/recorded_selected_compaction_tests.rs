// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn selected_visits_skip_absent_keys_and_keep_the_complete_traversal() {
    let mut map: RecordedMap<HashKind> = RecordedMap::default();
    for i in 0..1000 {
        map.insert_element_for_test(
            &format!("object-{i}"),
            std::sync::Arc::from("field"),
            crate::block_store::ElementEntry::default(),
        );
    }
    let selected: HashSet<ModelKey> = ["object-7", "object-900", "absent"]
        .into_iter().map(std::sync::Arc::from).collect();
    let mut seen = HashSet::new();
    map.try_visit_element_values_mut(Some(&selected), |key, mut values| {
        assert_eq!(values.iter_mut().count(), 1);
        seen.insert(key.to_owned());
        Ok::<_, ()>(())
    }).unwrap();
    assert_eq!(seen, HashSet::from(["object-7".to_owned(), "object-900".to_owned()]));
    let mut complete = 0;
    map.try_visit_element_values_mut(None, |_, _| {
        complete += 1;
        Ok::<_, ()>(())
    }).unwrap();
    assert_eq!(complete, 1000);
    let mut stopped = 0;
    let error = map.try_visit_element_values_mut(Some(&selected), |_, _| {
        stopped += 1;
        Err("stop")
    });
    assert_eq!(error, Err("stop"));
    assert_eq!(stopped, 1);
}
