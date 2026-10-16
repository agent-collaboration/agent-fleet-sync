INSERT INTO text_records VALUES('initial',9007199254740993,'before');
INSERT INTO composite_records VALUES('account',1,x'00ff');
INSERT INTO integer_records VALUES(10,1.25,NULL);
UPDATE text_records SET label='backfilled' WHERE id='initial';
