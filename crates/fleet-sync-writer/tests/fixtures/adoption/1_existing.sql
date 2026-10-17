CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY, amount INTEGER, label TEXT);
CREATE TABLE composite_records(account TEXT NOT NULL, sequence INTEGER NOT NULL, payload BLOB, PRIMARY KEY(account,sequence));
CREATE TABLE integer_records(id INTEGER PRIMARY KEY, ratio REAL, absent TEXT);
INSERT INTO text_records VALUES('initial',7,'before');
INSERT INTO composite_records VALUES('account',1,x'00ff');
INSERT INTO integer_records VALUES(10,1.25,NULL);
