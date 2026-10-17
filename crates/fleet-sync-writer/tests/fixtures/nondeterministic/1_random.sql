CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY,amount INTEGER,label TEXT);
INSERT INTO text_records VALUES('bad',random(),'should not commit');
