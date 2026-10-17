use crate::{InstalledSnapshot, ReceiverHook};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::HookRefusal;

/// Run inside seed publication or InstallSnapshot's ready transaction, after rows are visible.
pub fn run_snapshot_hook<'scope, S, H>(
    scope: &'scope mut S,
    hook: &'scope H,
    installed: &'scope InstalledSnapshot,
) -> impl std::future::Future<Output = Result<Vec<H::FollowUp>, HookRefusal>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    H: ReceiverHook<S>,
{
    hook.on_snapshot_installed(scope, installed)
}
