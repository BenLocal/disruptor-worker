CREATE DATABASE IF NOT EXISTS worker;

use worker;

CREATE TABLE IF NOT EXISTS worker_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    work_id VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    payload TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    priority INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    message TEXT,
    max_retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_at TIMESTAMP NULL,
    retry_interval_seconds INT NOT NULL DEFAULT 3600,

    INDEX idx_status_created (status, created_at),
    INDEX idx_handler_status (handler_id, status),
    INDEX idx_priority_created (priority, created_at),
    UNIQUE INDEX idx_unique_worker_id (work_id, handler_id)
);