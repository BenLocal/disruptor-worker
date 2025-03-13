package com.github.benshi.worker.store;

import java.util.List;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkerStatus;

public interface WorkerStore {
    /**
     * Save a new job to the database with PENDING status
     */
    boolean saveWorker(WorkContext ctx) throws Exception;

    /**
     * Get a list of jobs with the given status
     */
    List<WorkContext> getWorkersByStatus(WorkerStatus status, int limit) throws Exception;

    /**
     * Update the status of a job
     */
    boolean updateWorkerStatus(String jobId, WorkerStatus status,
            WorkerStatus current, String message) throws Exception;

    WorkContext getWorkerById(String id) throws Exception;
}
