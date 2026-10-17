-- A slice of a Router project store: project, milestones, tasks and their dependencies, the
-- writer epochs, the request ledger and the event feed. Constraints are PK, FK, NOT NULL and
-- UNIQUE, with CHECK only for the boolean column.
CREATE TABLE project (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE milestones (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project(id),
    is_default BOOLEAN NOT NULL CHECK (is_default IN (0, 1)),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    milestone_id TEXT NOT NULL REFERENCES milestones(id),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id),
    dependency_id TEXT NOT NULL REFERENCES tasks(id),
    PRIMARY KEY (task_id, dependency_id)
);

CREATE TABLE writer_epochs (
    epoch INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project(id),
    holder TEXT NOT NULL,
    start_position INTEGER NOT NULL
);

CREATE TABLE request_ledger (
    request_id TEXT PRIMARY KEY,
    canonical_hash TEXT NOT NULL,
    decision TEXT NOT NULL
);

CREATE TABLE events (
    position INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL REFERENCES writer_epochs(epoch),
    kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE REFERENCES request_ledger(request_id),
    router_time TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE records (
    sequence INTEGER PRIMARY KEY,
    body TEXT NOT NULL
);
