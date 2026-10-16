-- The shape a real store migration uses for a same-name rebuild: build the new table, copy the
-- rows in SQL, drop the old table and rename the new one into place. task_dependencies keeps
-- its foreign keys to tasks by name. This runs only under the policy that turns foreign keys
-- off before the owned transaction and checks PRAGMA foreign_key_check before commit.
CREATE TABLE tasks_rebuilt (
    id TEXT PRIMARY KEY,
    milestone_id TEXT NOT NULL REFERENCES milestones(id),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    label TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);
INSERT INTO tasks_rebuilt (id, milestone_id, status, revision, label)
SELECT id, milestone_id, status, revision, label FROM tasks;
DROP TABLE tasks;
ALTER TABLE tasks_rebuilt RENAME TO tasks;
