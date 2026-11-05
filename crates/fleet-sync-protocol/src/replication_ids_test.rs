#![allow(clippy::unwrap_used)]
use super::*;
#[test]
fn connection_and_frame_ids_require_canonical_uuid_v7_on_every_entry_point() {
    let valid = "019eef00-0000-7000-8000-000000000001";
    for bad in [
        "019eef00-0000-4000-8000-000000000001",
        "00000000-0000-0000-0000-000000000001",
        "019EEF00-0000-7000-8000-000000000001",
        "019eef00000070008000000000000001",
    ] {
        assert!(
            ConnectionId::try_from(bad.to_owned()).is_err(),
            "accepted connection {bad}"
        );
        assert!(
            FrameId::try_from(bad.to_owned()).is_err(),
            "accepted frame {bad}"
        );
        let encoded = serde_json::to_string(bad).unwrap();
        assert!(serde_json::from_str::<ConnectionId>(&encoded).is_err());
        assert!(serde_json::from_str::<FrameId>(&encoded).is_err());
    }
    assert!(
        ConnectionId::new(Uuid::parse_str("019eef00-0000-4000-8000-000000000001").unwrap())
            .is_err()
    );
    assert!(
        FrameId::new(Uuid::parse_str("019eef00-0000-4000-8000-000000000001").unwrap()).is_err()
    );
    assert_eq!(
        ConnectionId::try_from(valid.to_owned())
            .unwrap()
            .to_string(),
        valid
    );
    assert_eq!(
        FrameId::try_from(valid.to_owned()).unwrap().to_string(),
        valid
    );
}
