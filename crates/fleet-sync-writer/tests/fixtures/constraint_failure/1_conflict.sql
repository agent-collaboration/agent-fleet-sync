CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY,amount INTEGER,label TEXT);
INSERT INTO text_records VALUES('same',1,'first');
INSERT INTO text_records VALUES('same',2,'must roll back');
