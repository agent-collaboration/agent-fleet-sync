SELECT CAST(tbl_name AS TEXT) AS owner, typeof(tbl_name) AS owner_class FROM sqlite_schema WHERE name=? COLLATE NOCASE
