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
    workflow_run INTEGER NOT NULL,
    task_name TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    completed_successfully INTEGER DEFAULT 0,
    completed_time INTEGER,
    input_json TEXT NOT NULL,
    command TEXT,
    image TEXT NOT NULL,
    output_json TEXT,
    FOREIGN KEY(workflow_run) REFERENCES workflow_run(id)
);

CREATE INDEX task_run_by_inputs ON task_run(input_json, image, task_name, command);

CREATE TABLE input_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    last_modified_time INTEGER,
    workflow_run INTEGER NOT NULL,
    FOREIGN KEY(workflow_run) REFERENCES workflow_run(id)
);

CREATE INDEX input_file_by_path ON input_file(path);