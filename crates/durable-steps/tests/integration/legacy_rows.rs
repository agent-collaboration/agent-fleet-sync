//! Raw v1 columns remain an independent oracle across metadata-only upgrades.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct LegacyOperationRow {
    pub operation_id: String,
    pub kind: String,
    pub kind_version: i64,
    pub request_key: String,
    pub input: Option<Vec<u8>>,
    pub input_digest: Vec<u8>,
    pub state: String,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
    pub finished_at: Option<i64>,
    pub next_attempt_at: Option<i64>,
    pub attempt: i64,
    pub drive_token: Option<String>,
    pub drive_until: Option<i64>,
}
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct LegacyStepRow {
    pub operation_id: String,
    pub position: i64,
    pub step_name: String,
    pub kind: String,
    pub intent: Option<String>,
    pub output: Option<Vec<u8>>,
    pub first_attempt_at: Option<i64>,
    pub may_have_reached: Option<i64>,
}
#[allow(
    dead_code,
    reason = "The shared DO fixture has no native SQLx receipt table"
)]
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct LegacyReceiptRow {
    pub version: i64,
    pub description: String,
    pub installed_on: String,
    pub success: i64,
    pub checksum: Vec<u8>,
    pub execution_time: i64,
}
pub(crate) const OPERATIONS: &str = "SELECT operation_id,kind,kind_version,request_key,input,input_digest,state,result,failure,finished_at,next_attempt_at,attempt,drive_token,drive_until FROM _fleet_sync_operations ORDER BY operation_id";
pub(crate) const STEPS: &str = "SELECT operation_id,position,step_name,kind,intent,output,first_attempt_at,may_have_reached FROM _fleet_sync_steps ORDER BY operation_id,position";
#[allow(
    dead_code,
    reason = "The shared DO fixture has no native SQLx receipt table"
)]
pub(crate) const RECEIPTS: &str = "SELECT version,description,CAST(installed_on AS TEXT) AS installed_on,CAST(success AS INTEGER) AS success,checksum,execution_time FROM _fleet_sync_migrations ORDER BY version";

pub(crate) const NATURAL_CONSTRAINTS: &[(&str, &str)] = &[
    (
        "INSERT INTO _fleet_sync_operations(operation_id,kind,kind_version,request_key,input,input_digest,state,result,failure,finished_at,next_attempt_at,attempt,drive_token,drive_until,created_at,updated_at) SELECT '0197abc0-def0-7123-8123-abcdef012349',kind,kind_version,request_key,input,input_digest,state,result,failure,finished_at,next_attempt_at,attempt,drive_token,drive_until,created_at,updated_at FROM _fleet_sync_operations WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345'",
        "_fleet_sync_operations.kind, _fleet_sync_operations.request_key",
    ),
    (
        "INSERT INTO _fleet_sync_steps(id,operation_id,position,step_name,kind,intent,output,first_attempt_at,may_have_reached,created_at,updated_at) SELECT '0197abc0-def0-7123-8123-abcdef012349',operation_id,position,'another-name',kind,intent,output,first_attempt_at,may_have_reached,created_at,updated_at FROM _fleet_sync_steps WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0",
        "_fleet_sync_steps.operation_id, _fleet_sync_steps.position",
    ),
    (
        "INSERT INTO _fleet_sync_steps(id,operation_id,position,step_name,kind,intent,output,first_attempt_at,may_have_reached,created_at,updated_at) SELECT '0197abc0-def0-7123-8123-abcdef012349',operation_id,66,step_name,kind,intent,output,first_attempt_at,may_have_reached,created_at,updated_at FROM _fleet_sync_steps WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0",
        "_fleet_sync_steps.operation_id, _fleet_sync_steps.step_name",
    ),
    (
        "INSERT INTO _fleet_sync_schema(id,version,created_at,updated_at) SELECT '0197abc0-def0-7123-8123-abcdef012349',version,created_at,updated_at FROM _fleet_sync_schema",
        "_fleet_sync_schema.version",
    ),
];

pub(crate) fn unique_failure(error: &str, columns: &str) -> bool {
    let fields = columns
        .split(", ")
        .filter_map(|column| column.rsplit_once('.'))
        .collect::<Vec<_>>();
    let Some((table, _)) = fields.first() else {
        return false;
    };
    let grouped = format!(
        "{table}.({})",
        fields
            .iter()
            .map(|(_, field)| *field)
            .collect::<Vec<_>>()
            .join(", ")
    );
    error.contains("UNIQUE constraint failed")
        && (error.contains(columns) || error.contains(&grouped))
}
