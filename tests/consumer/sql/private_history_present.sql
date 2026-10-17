SELECT name FROM sqlite_schema WHERE name='private_history' OR instr(coalesce(sql,''),'private_history')>0
