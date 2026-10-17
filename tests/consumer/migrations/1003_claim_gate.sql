-- The consumer owns its claim reservation and terminal settlement receipts.
CREATE TABLE claim_gate(attempt TEXT NOT NULL PRIMARY KEY, record TEXT NOT NULL);
