package com.github.benshi.worker.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkerStatus;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MysqlWorkerStore implements WorkerStore {
    private final DataSource dataSource;

    @Override
    public boolean saveWorker(WorkContext ctx) throws Exception {
        // Generate job ID if not provided
        if (ctx.getWorkId() == null || ctx.getWorkId().isEmpty()) {
            ctx.setWorkId(UUID.randomUUID().toString());
        }

        if (ctx.getHandlerId() == null || ctx.getHandlerId().isEmpty()) {
            throw new IllegalArgumentException("Handler ID is required");
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO worker_jobs (work_id, handler_id, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW())")) {

            stmt.setString(1, ctx.getWorkId());
            stmt.setString(2, ctx.getHandlerId());
            stmt.setString(3, ctx.getPayload());
            stmt.setString(4, WorkerStatus.PENDING.name());
            int rows = stmt.executeUpdate();
            return rows > 0;
        }

    }

    @Override
    public List<WorkContext> getWorkersByStatus(WorkerStatus status, int limit) throws Exception {
        List<WorkContext> jobs = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT id, work_id, handler_id, payload, retry_count, status " +
                                "FROM worker_jobs " +
                                "WHERE status = ? " +
                                "ORDER BY priority DESC, updated_at ASC " +
                                "LIMIT ?")) {

            stmt.setString(1, status.name());
            stmt.setInt(2, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    WorkContext ctx = new WorkContext();
                    ctx.setId(rs.getLong("id"));
                    ctx.setWorkId(rs.getString("work_id"));
                    ctx.setHandlerId(rs.getString("handler_id"));
                    ctx.setPayload(rs.getString("payload"));
                    ctx.setCurrentStatus(WorkerStatus.valueOf(rs.getString("status")));
                    ctx.setCurrentRetryCount(rs.getInt("retry_count"));
                    jobs.add(ctx);
                }
            }
        }

        return jobs;
    }

    @Override
    public boolean updateWorkerStatus(long id, WorkerStatus status, WorkerStatus current, String message)
            throws Exception {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE worker_jobs " +
                                "SET status = ?, updated_at = NOW(), message = ? " +
                                "WHERE id = ? AND status = ?")) {

            stmt.setString(1, status.name());
            stmt.setString(2, message);
            stmt.setLong(3, id);

            stmt.setString(4, current.name());

            int updated = stmt.executeUpdate();
            return updated > 0;
        }
    }

    @Override
    public WorkContext getWorkerById(long id) throws Exception {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT id, work_id, handler_id, payload, retry_count, status " +
                                "FROM worker_jobs " +
                                "WHERE work_id = ? ")) {

            stmt.setLong(1, id);

            WorkContext ctx = new WorkContext();
            try (ResultSet rs = stmt.executeQuery()) {

                if (rs.next()) {
                    ctx.setId(rs.getLong("id"));
                    ctx.setWorkId(rs.getString("work_id"));
                    ctx.setHandlerId(rs.getString("handler_id"));
                    ctx.setPayload(rs.getString("payload"));
                    ctx.setCurrentStatus(WorkerStatus.valueOf(rs.getString("status")));
                    ctx.setCurrentRetryCount(rs.getInt("retry_count"));
                } else {
                    return null;
                }
            }

            return ctx;
        }

    }

}
