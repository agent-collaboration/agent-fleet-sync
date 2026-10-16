-- Same-name rebuild: task_dependencies keeps its incoming foreign keys to tasks. This runs only
-- under the migration policy that turns foreign keys off before the owned transaction; the
-- caller reinserts the rows and checks PRAGMA foreign_key_check before commit.
DROP TABLE tasks;
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    milestone_id TEXT NOT NULL REFERENCES milestones(id),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    label TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);
