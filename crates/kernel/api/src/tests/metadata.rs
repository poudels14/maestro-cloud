use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AnnotationKey, FinalizerName, Generation, LabelKey, ObjectMeta, ResourceRevision, ServiceId,
};

#[test]
fn empty_optional_metadata_is_omitted_from_wire_shape() {
    let metadata = ObjectMeta {
        id: ServiceId::new("api").expect("service id"),
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(4),
        generation: Generation(2),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    };

    let value = serde_json::to_value(metadata).expect("serialize metadata");

    assert_eq!(
        value,
        serde_json::json!({
            "id": "api",
            "revision": 4,
            "generation": 2
        })
    );
}

#[test]
fn metadata_keys_remain_distinct_types() {
    let labels = BTreeMap::from([(LabelKey("region".to_string()), "west".to_string())]);
    let annotations = BTreeMap::from([(
        AnnotationKey("release.maestro.dev/note".to_string()),
        "canary".to_string(),
    )]);
    let finalizers = BTreeSet::from([FinalizerName("deployment.maestro.dev".to_string())]);

    assert!(labels.contains_key(&LabelKey("region".to_string())));
    assert!(annotations.contains_key(&AnnotationKey("release.maestro.dev/note".to_string())));
    assert!(finalizers.contains(&FinalizerName("deployment.maestro.dev".to_string())));
}
