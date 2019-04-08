CREATE TABLE cached_task_run_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    latest_use_workflow_run_id INTEGER NOT NULL,
    task_name TEXT NOT NULL,
    input_json TEXT NOT NULL,
    params_json TEXT,
    command TEXT,
    image TEXT NOT NULL,
    output_files TEXT
);

CREATE UNIQUE INDEX cached_exec_by_inputs ON cached_task_run_execution(input_json, params_json, image, task_name, command);