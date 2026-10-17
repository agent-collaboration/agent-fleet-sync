CREATE TABLE _fleet_sync_install(singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_install_pages(install_id TEXT NOT NULL, page INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(install_id,page));
