-- Durable Objects have no migrations tool, so the test object runs this on
-- every request; `IF NOT EXISTS` makes that a no-op after the first. The same
-- tables as the D1 test Worker's fixture.
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    age INTEGER CHECK (age >= 0),
    score REAL,
    active INTEGER NOT NULL DEFAULT 1,
    avatar BLOB
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (id),
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS numbers (
    id INTEGER PRIMARY KEY,
    value
);
