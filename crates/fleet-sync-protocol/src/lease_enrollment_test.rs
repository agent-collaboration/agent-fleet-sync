#![expect(
    clippy::panic_in_result_fn,
    reason = "Assertions verify the exact refusal/retry contract; setup errors remain Results."
)]
use super::*;
#[test]
fn holder_not_enrolled_is_a_typed_additive_renewal_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    let wire = serde_json::json!({"kind":"refused","value":{"kind":"holderNotEnrolled"}});
    let answer: RenewAnswer = serde_json::from_value(wire.clone())?;
    assert_eq!(serde_json::to_value(answer)?, wire);
    Ok(())
}
