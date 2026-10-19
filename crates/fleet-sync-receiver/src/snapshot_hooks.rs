use crate::{HookError, InstalledSnapshot, ReceiverRules};
use durable_steps::storage::TransactionScope;

/// Run inside seed publication or InstallSnapshot's ready transaction, after rows are visible.
pub fn run_snapshot_hook<'scope, S, H>(
    scope: &'scope mut S,
    hook: &'scope H,
    installed: &'scope InstalledSnapshot,
) -> impl std::future::Future<Output = Result<Vec<H::FollowUp>, HookError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    H: ReceiverRules<S>,
{
    hook.on_snapshot_installed(scope, installed)
}
