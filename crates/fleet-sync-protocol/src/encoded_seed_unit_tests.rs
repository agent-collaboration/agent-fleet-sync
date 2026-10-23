use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn seed_page(
    body: String,
    generation: u64,
    index: u32,
) -> Result<SeedPage, Box<dyn std::error::Error>> {
    let contents = SnapshotPage {
        database: DatabaseId::new(uuid::Uuid::from_u128(0x019eef00000070008000000000000001))?,
        position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(1)),
        schema: SchemaIdentity::new(1, "fingerprint".into())?,
        schema_steps: vec!["CREATE TABLE records(id TEXT PRIMARY KEY,body TEXT)".into()],
        table: "records".into(),
        rows: vec![vec![SqlValue::Text("key".into()), SqlValue::Text(body)]],
        next: None,
    };
    Ok(SeedPage {
        seed: SeedId { generation },
        page: index,
        digest: PageDigest::of(&contents)?,
        contents,
    })
}

#[test]
fn actual_seed_lengths_preserve_the_full_original_carrier_bound() -> TestResult {
    for (generation, index) in [(1, 0), (9, 9), (10, 10), (u64::MAX, u32::MAX)] {
        for offset in [-1_isize, 0, 1] {
            let empty = seed_page(String::new(), generation, index)?;
            let overhead = measure_snapshot_unit(empty.contents)?.encoded_size;
            let desired = MAX_UNIT_BYTES
                .checked_add_signed(offset)
                .ok_or("bad size")?;
            let page = seed_page("x".repeat(desired - overhead), generation, index)?;
            let encoded = serde_json::to_string(&page)?;
            let actual = measure_snapshot_unit(page.contents.clone())?.encoded_size;
            if actual != desired {
                return Err(format!("real encoded boundary differs: {actual}/{desired}").into());
            }
            if encoded_seed_page_fits_unit_limit(&page, &encoded) != (actual <= MAX_UNIT_BYTES) {
                return Err(
                    format!("encoded guard differs at {generation}/{index}/{offset}").into(),
                );
            }
        }
    }
    Ok(())
}

#[test]
fn staged_allowance_bounds_every_actual_snapshot_carrier_for_varied_payloads() -> TestResult {
    for repetitions in [0, 1, 31, 255] {
        let mut page = seed_page("\"\\\n\u{0}雪🦀".repeat(repetitions), 123, 456)?;
        page.contents.rows.push(vec![
            SqlValue::Integer(i64::MIN),
            SqlValue::Real(f64::INFINITY),
            SqlValue::Blob(vec![0, 128, 255]),
        ]);
        page.contents.next = Some(SnapshotCursor {
            position: page.contents.position,
            table: "records".into(),
            after_key: vec![SqlValue::Text("\"\\雪".into())],
        });
        page.digest = PageDigest::of(&page.contents)?;
        // The reference measures all three real carriers. The independent
        // stored-length arithmetic must equal that maximum, not just fit below it.
        let normalized = SeedPage {
            seed: SeedId {
                generation: u64::MAX,
            },
            page: u32::MAX,
            digest: page.digest.clone(),
            contents: page.contents.clone(),
        };
        let staged = serde_json::to_vec(&normalized)?.len()
            + u64::MAX.to_string().len()
            + SQLITE_INTEGER_BYTES
            + normalized.digest.as_str().len()
            + STAGED_ROW_FRAMING_BYTES;
        let actual = measure_snapshot_unit(page.contents.clone())?.encoded_size;
        if staged != actual {
            return Err(
                format!("staged bound no longer covers every carrier: {staged}/{actual}").into(),
            );
        }
        if !encoded_seed_page_fits_unit_limit(&page, &serde_json::to_string(&page)?) {
            return Err("small valid encoded page refused".into());
        }
    }
    Ok(())
}
