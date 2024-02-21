CREATE KEYSPACE tms WITH replication = {'class': 'SimpleStrategy'}  AND durable_writes = true;
USE tms ;

CREATE TABLE tasks ( 
    id text, 
    status int, 
    start_at timestamp, 
    recurring text,
    command text, 
    PRIMARY KEY (id) 
);

CREATE TABLE tasks_by_schedule (
    when date,
    scheduled_for timestamp,
    task_id text,
    PRIMARY KEY (when, scheduled_for, task_id)
) WITH CLUSTERING ORDER BY (scheduled_for ASC);

CREATE TABLE tasks_by_worker (
    worker_id text,
    task_id text,
    PRIMARY KEY (worker_id, task_id);
)

CREATE TABLE workers (
    id text,
    last_heartbeat timestamp,
    PRIMARY KEY (id, last_heartbeat)
) WITH CLUSTERING ORDER BY (last_heartbeat ASC);

