package com.github.benshi.worker.store.mybatis;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.ibatis.session.SqlSession;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkerStatus;
import com.github.benshi.worker.store.WorkerStore;
import com.github.benshi.worker.store.mybatis.dao.WorkerJob;
import com.github.benshi.worker.store.mybatis.mapper.WorkerJobsMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MybatisWorkerStore implements WorkerStore {
    private final MyBatisConfig config;

    @Override
    public boolean saveWorker(WorkContext ctx) throws Exception {
        // Generate job ID if not provided
        if (ctx.getWorkId() == null || ctx.getWorkId().isEmpty()) {
            ctx.setWorkId(UUID.randomUUID().toString());
        }

        if (ctx.getHandlerId() == null || ctx.getHandlerId().isEmpty()) {
            throw new IllegalArgumentException("Handler ID is required");
        }

        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            WorkerJob job = new WorkerJob()
                    .setWorkId(ctx.getWorkId())
                    .setHandlerId(ctx.getHandlerId())
                    .setPayload(ctx.getPayload())
                    .setStatus(WorkerStatus.PENDING.name())
                    .setPriority(0);

            int rows = mapper.insertWorkerJob(job);
            return rows > 0;
        }
    }

    @Override
    public List<WorkContext> getWorkersByStatus(WorkerStatus status, int limit) throws Exception {
        List<WorkContext> contexts = new ArrayList<>();

        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            List<WorkerJob> jobs = mapper.getWorkerJobsByStatus(status.name(), limit);

            for (WorkerJob job : jobs) {
                WorkContext ctx = new WorkContext()
                        .setId(job.getId())
                        .setWorkId(job.getWorkId())
                        .setHandlerId(job.getHandlerId())
                        .setPayload(job.getPayload())
                        .setCurrentStatus(WorkerStatus.valueOf(job.getStatus()))
                        .setRetryCount(job.getRetryCount());

                contexts.add(ctx);
            }

            return contexts;
        }
    }

    @Override
    public boolean updateWorkerStatus(long id, WorkerStatus status, WorkerStatus current, String message)
            throws Exception {
        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            int rows = mapper.updateWorkerJobStatus(id, status.name(), current.name(), message);
            return rows > 0;
        }
    }

    @Override
    public WorkContext getWorkerById(long id) throws Exception {
        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            WorkerJob job = mapper.getWorkerJobById(id);

            if (job == null) {
                return null;
            }

            return new WorkContext()
                    .setId(job.getId())
                    .setWorkId(job.getWorkId())
                    .setHandlerId(job.getHandlerId())
                    .setPayload(job.getPayload())
                    .setCurrentStatus(WorkerStatus.valueOf(job.getStatus()))
                    .setRetryCount(job.getRetryCount());
        }
    }

    @Override
    public int deleteJobsOlderThan(Date cutoffDate, WorkerStatus excludeStatus) throws Exception {
        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            return mapper.deleteJobsOlderThan(cutoffDate, excludeStatus.name());
        }
    }

    @Override
    public WorkContext getWorkerByWorkId(String workId, String handlerId) throws Exception {
        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            WorkerJob job = mapper.getWorkerJobByWorkIdAndHandlerId(workId, handlerId);

            if (job == null) {
                return null;
            }

            return new WorkContext()
                    .setId(job.getId())
                    .setWorkId(job.getWorkId())
                    .setHandlerId(job.getHandlerId())
                    .setPayload(job.getPayload())
                    .setCurrentStatus(WorkerStatus.valueOf(job.getStatus()))
                    .setRetryCount(job.getRetryCount());
        }
    }

    @Override
    public List<WorkContext> list(int offset, int limit, String filter) throws Exception {
        List<WorkContext> contexts = new ArrayList<>();

        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            List<WorkerJob> jobs = mapper.listWorkerJobs(filter, limit, offset);

            for (WorkerJob job : jobs) {
                WorkContext ctx = new WorkContext()
                        .setId(job.getId())
                        .setWorkId(job.getWorkId())
                        .setHandlerId(job.getHandlerId())
                        .setPayload(job.getPayload())
                        .setCurrentStatus(WorkerStatus.valueOf(job.getStatus()))
                        .setRetryCount(job.getRetryCount());

                contexts.add(ctx);
            }

            return contexts;
        }
    }

    @Override
    public long count(String filter) throws Exception {
        try (SqlSession session = config.openSession(true)) {
            WorkerJobsMapper mapper = session.getMapper(WorkerJobsMapper.class);

            return mapper.countWorkerJobs(filter);
        }
    }

}
