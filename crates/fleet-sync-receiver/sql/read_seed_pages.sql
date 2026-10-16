SELECT CAST(page AS TEXT) AS page,typeof(page) AS page_class,digest,payload FROM _fleet_sync_seed_pages WHERE seed=?1 ORDER BY _fleet_sync_seed_pages.page
