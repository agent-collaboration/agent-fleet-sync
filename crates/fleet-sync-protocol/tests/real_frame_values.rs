#![allow(clippy::unwrap_used)]

use fleet_sync_protocol::*;
use uuid::Uuid;

fn snapshot_frame(real_bits: &[u64]) -> ServerFrame {
    ServerFrame::Answer {
        frame: FrameId::new(Uuid::from_u128(0x019eef00000070008000000000000001)).unwrap(),
        body: OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
            Box::new(SnapshotPage {
                database: DatabaseId::new(Uuid::from_u128(0x019eef00000070008000000000000002))
                    .unwrap(),
                position: ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(0)),
                schema: SchemaIdentity::new(1, "real-codec".into()).unwrap(),
                schema_steps: vec![],
                table: "real_values".into(),
                rows: real_bits
                    .iter()
                    .map(|bits| vec![SqlValue::Real(f64::from_bits(*bits))])
                    .collect(),
                next: None,
            }),
        ))),
    }
}

#[test]
fn real_frame_json_preserves_named_edges_and_200000_generated_bit_patterns() {
    let mut expected_bits = vec![
        3_485_510_186_621_062_260,
        f64::MIN_POSITIVE.to_bits(),
        1,
        f64::MAX.to_bits(),
        (-f64::MAX).to_bits(),
        (-0.0_f64).to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
    ];
    let target_count = expected_bits.len() + 200_000;
    let mut generated_bits = 0x9e37_79b9_7f4a_7c15_u64;
    while expected_bits.len() < target_count {
        generated_bits ^= generated_bits << 13;
        generated_bits ^= generated_bits >> 7;
        generated_bits ^= generated_bits << 17;
        if !f64::from_bits(generated_bits).is_nan() {
            expected_bits.push(generated_bits);
        }
    }

    let mut bit_mismatches = 0;
    let mut frame_decode_failures = 0;
    let mut first_mismatch = None;
    for batch in expected_bits.chunks(256) {
        let wire = serde_json::to_vec(&snapshot_frame(batch)).unwrap();
        let decoded = serde_json::from_slice::<ServerFrame>(&wire);

        match decoded {
            Ok(ServerFrame::Answer {
                body:
                    OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
                        page,
                    ))),
                ..
            }) => {
                assert_eq!(page.rows.len(), batch.len());
                for (row_index, expected) in batch.iter().enumerate() {
                    let actual = match page.rows.get(row_index).map(Vec::as_slice) {
                        Some([SqlValue::Real(value)]) => Some(value.to_bits()),
                        _ => None,
                    };
                    if actual != Some(*expected) {
                        bit_mismatches += 1;
                        first_mismatch.get_or_insert((*expected, actual));
                    }
                }
            }
            _ => frame_decode_failures += 1,
        }
    }

    println!(
        "OBS real frame sweep: patterns={}, bit_mismatches={bit_mismatches}, frame_decode_failures={frame_decode_failures}, first_mismatch={first_mismatch:?}",
        expected_bits.len()
    );
    assert_eq!((bit_mismatches, frame_decode_failures), (0, 0));
    assert!(serde_json::to_vec(&snapshot_frame(&[f64::NAN.to_bits()])).is_err());
}

#[test]
fn real_decode_rejects_map_infinities_directly_and_inside_frames() {
    let mut accepted_shapes = Vec::new();
    for (marker, value) in [
        ("positiveInfinity", f64::INFINITY),
        ("negativeInfinity", f64::NEG_INFINITY),
    ] {
        let mut object = serde_json::Map::new();
        object.insert(marker.into(), serde_json::Value::Null);
        let object = serde_json::Value::Object(object);
        let scalar = serde_json::json!({"kind":"real","value":object.clone()});
        if serde_json::from_slice::<SqlValue>(&serde_json::to_vec(&scalar).unwrap()).is_ok() {
            accepted_shapes.push(format!("scalar {marker}"));
        }

        let mut frame = serde_json::to_value(snapshot_frame(&[value.to_bits()])).unwrap();
        *frame
            .pointer_mut("/value/body/value/value/value/rows/0/0/value")
            .unwrap() = object;
        if serde_json::from_slice::<ServerFrame>(&serde_json::to_vec(&frame).unwrap()).is_ok() {
            accepted_shapes.push(format!("frame {marker}"));
        }
    }

    assert!(accepted_shapes.is_empty(), "accepted: {accepted_shapes:?}");
}
