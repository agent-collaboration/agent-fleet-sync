ALTER TABLE samples ADD COLUMN note TEXT;
UPDATE samples SET note = 'after bootstrap';
