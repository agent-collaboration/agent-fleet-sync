use super::*;

const ID: &str = "0197abc0-def0-7123-8123-abcdef012345";

#[test]
fn kind_is_part_of_the_identity_even_when_uuid_bits_match() {
    let machine = Submitter::Machine(PeerId::try_from(ID.to_owned()).unwrap());
    let project = Submitter::Project(DatabaseId::try_from(ID.to_owned()).unwrap());

    assert_ne!(machine, project);
    assert_eq!(id(machine), id(project));
    for owner in [machine, project] {
        let encoded = serde_json::to_value(owner).unwrap();
        assert_eq!(encoded, serde_json::json!({"kind":kind(owner),"value":ID}));
        assert_eq!(serde_json::from_value::<Submitter>(encoded).unwrap(), owner);
        assert_eq!(
            from_columns(Some(kind(owner).into()), Some(ID.into())).unwrap(),
            Some(owner)
        );
    }
}

#[test]
fn stored_submitter_requires_a_complete_known_kind_and_canonical_uuidv7() {
    assert_eq!(from_columns(None, None).unwrap(), None);
    for (kind, id) in [
        (Some("machine"), None),
        (None, Some(ID)),
        (Some("other"), Some(ID)),
        (
            Some("project"),
            Some("0197abc0-def0-4123-8123-abcdef012345"),
        ),
        (
            Some("machine"),
            Some("0197ABC0-DEF0-7123-8123-ABCDEF012345"),
        ),
    ] {
        assert!(from_columns(kind.map(str::to_owned), id.map(str::to_owned)).is_err());
    }
}
