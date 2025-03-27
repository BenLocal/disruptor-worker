package com.github.benshi.worker.store.mybatis.mapper;

import java.util.Date;
import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.github.benshi.worker.store.mybatis.dao.WorkerJob;

public interface WorkerJobsMapper {
    /**
     * Insert a new worker job into the database
     * 
     * @param job The job to insert
     * @return The number of rows affected
     */
    int insertWorkerJob(WorkerJob job);

    /**
     * Get jobs by status
     * 
     * @param status The status to filter by
     * @param limit  The maximum number of jobs to return
     * @return List of worker jobs
     */
    List<WorkerJob> getWorkerJobsByStatus(@Param("status") String status, @Param("limit") int limit);

    /**
     * Update the status of a job
     * 
     * @param id            The job ID
     * @param status        The new status
     * @param currentStatus The current status
     * @param message       Optional message
     * @return The number of rows affected
     */
    int updateWorkerJobStatus(@Param("id") long id,
            @Param("status") String status,
            @Param("currentStatus") String currentStatus,
            @Param("message") String message);

    /**
     * Get a worker job by ID
     * 
     * @param id The job ID
     * @return The worker job
     */
    WorkerJob getWorkerJobById(@Param("id") long id);

    /**
     * Delete jobs older than the given date and not having the excluded status
     * 
     * @param cutoffDate    The cutoff date
     * @param excludeStatus The status to exclude from deletion
     * @return The number of rows deleted
     */
    int deleteJobsOlderThan(@Param("cutoffDate") Date cutoffDate, @Param("excludeStatus") String excludeStatus);

    /**
     * Get a worker job by work ID and handler ID
     * 
     * @param workId    The work ID
     * @param handlerId The handler ID
     * @return The worker job
     */
    WorkerJob getWorkerJobByWorkIdAndHandlerId(@Param("workId") String workId,
            @Param("handlerId") String handlerId);

    /**
     * List worker jobs with pagination and filtering
     * 
     * @param filter The filter string to match against work ID
     * @param limit  The maximum number of jobs to return
     * @param offset The offset for pagination
     * @return List of worker jobs
     */
    List<WorkerJob> listWorkerJobs(@Param("filter") String filter,
            @Param("limit") int limit,
            @Param("offset") int offset);

    /**
     * Count worker jobs matching a filter
     * 
     * @param filter The filter string to match against work ID
     * @return The count of matching jobs
     */
    long countWorkerJobs(@Param("filter") String filter);
}
