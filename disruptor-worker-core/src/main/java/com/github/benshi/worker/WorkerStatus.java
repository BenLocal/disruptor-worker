package com.github.benshi.worker;

public enum WorkerStatus {
    // Job is waiting to be processed
    PENDING,
    // Job is currently being processed
    RUNNING,
    // Job completed successfully
    COMPLETED,
    // Job failed but may be retried
    FAILED,
    // Job is marked for retry after failure
    RETRY,
    // Job failed and will not be retried
    FAILED_PERMANENT
}
