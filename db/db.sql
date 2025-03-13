CREATE DATABASE IF NOT EXISTS worker;

use worker;

CREATE TABLE IF NOT EXISTS worker_jobs (
    work_id VARCHAR(64) PRIMARY KEY,
    handler_id VARCHAR(64) NOT NULL,
    payload TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    priority INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status_created (status, created_at),
    INDEX idx_handler_status (handler_id, status),
    INDEX idx_priority_created (priority, created_at)
);