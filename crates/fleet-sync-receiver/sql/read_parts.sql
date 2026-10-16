SELECT CAST(part_index AS TEXT) AS part_index, typeof(part_index) AS part_index_class, payload FROM _fleet_sync_staged_parts WHERE transaction_key = ? ORDER BY _fleet_sync_staged_parts.part_index
