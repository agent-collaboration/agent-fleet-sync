CREATE TABLE _fleet_sync_installs (
    install_id TEXT NOT NULL PRIMARY KEY,
    target_key TEXT NOT NULL,
    record TEXT NOT NULL
);
CREATE INDEX _fleet_sync_installs_target ON _fleet_sync_installs(target_key);
CREATE TABLE _fleet_sync_install_current (
    target_key TEXT NOT NULL PRIMARY KEY,
    install_id TEXT NOT NULL REFERENCES _fleet_sync_installs(install_id)
);
