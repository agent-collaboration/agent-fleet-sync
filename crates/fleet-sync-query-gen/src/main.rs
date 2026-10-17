use fleet_sync_query_gen::{GenerateError, generate_manifest};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), GenerateError> {
    let mut arguments = std::env::args().skip(1);
    let mut manifest = None;
    let mut check = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" if manifest.is_none() => manifest = arguments.next().map(PathBuf::from),
            "--check" if !check => check = true,
            _ => {
                return Err(GenerateError::Manifest(
                    "usage: fleet-sync-query-gen --manifest <checked-queries.json> [--check]"
                        .into(),
                ));
            }
        }
    }
    let manifest =
        manifest.ok_or_else(|| GenerateError::Manifest("--manifest is required".into()))?;
    generate_manifest(&manifest, check).await?;
    println!(
        "{} checked SQL for {}",
        if check { "verified" } else { "generated" },
        manifest.display()
    );
    Ok(())
}
