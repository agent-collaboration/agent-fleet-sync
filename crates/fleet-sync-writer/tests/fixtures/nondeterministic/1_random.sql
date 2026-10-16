CREATE TABLE text_records(id TEXT PRIMARY KEY,amount INTEGER,label TEXT);
INSERT INTO text_records VALUES('bad',random(),'should not commit');
