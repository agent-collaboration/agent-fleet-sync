UPDATE text_records SET label='must roll back';
INSERT INTO text_records SELECT * FROM text_records;
