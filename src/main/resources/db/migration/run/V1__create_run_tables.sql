CREATE TABLE workflow_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    completed_successfully INTEGER DEFAULT 0,
    completed_time INTEGER
);

CREATE INDEX workflow_run_name ON workflow_run(workflow_name);

CREATE TABLE task_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    completion_status VARCHAR(20),
    completed_time INTEGER,
    executions_json TEXT
);