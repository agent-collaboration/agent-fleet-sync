CREATE TABLE conformance_fk_parents (id INTEGER PRIMARY KEY NOT NULL);
CREATE TABLE conformance_fk_children (
    id INTEGER PRIMARY KEY NOT NULL,
    parent_id INTEGER NOT NULL REFERENCES conformance_fk_parents(id)
);
