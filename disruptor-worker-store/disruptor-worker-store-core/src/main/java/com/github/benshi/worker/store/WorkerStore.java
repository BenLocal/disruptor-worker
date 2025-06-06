package com.github.benshi.worker.store;

import java.util.Date;
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
    boolean updateWorkerStatus(long id, WorkerStatus status,
            WorkerStatus current, String message) throws Exception;

    WorkContext getWorkerById(long id) throws Exception;

    int deleteJobsOlderThan(Date cutoffDate, WorkerStatus excludeStatus) throws Exception;

    WorkContext getWorkerByWorkId(String workId, String handlerId) throws Exception;

    List<WorkContext> list(int offset, int limit, String filter) throws Exception;

    long count(String filter) throws Exception;
}
