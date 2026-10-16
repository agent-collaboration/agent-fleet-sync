SELECT digest,payload FROM _fleet_sync_seed_pages WHERE seed=?1 AND page=CAST(?2 AS INTEGER)
