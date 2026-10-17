SELECT installs.record, typeof(installs.record) AS record_class
FROM _fleet_sync_install_current AS current
JOIN _fleet_sync_installs AS installs ON installs.install_id = current.install_id
WHERE current.target_key = ?;
