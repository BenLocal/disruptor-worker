package com.github.benshi.worker.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkerStatus;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MysqlWorkerStoreTest {
    private MysqlWorkerStore initStore() {
        DataSource dataSource = initDataSource();
        cleanTable(dataSource);
        MysqlWorkerStore store = new MysqlWorkerStore(dataSource);
        return store;
    };

    private void cleanTable(DataSource dataSource) {
        // clean all datas
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("delete from worker_jobs")) {
            ps.executeUpdate();
        } catch (SQLException e) {
            // ignore
        }
    }

    @Test
    public void testCount() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1"));
        store.saveWorker(new WorkContext()
                .setWorkId("2")
                .setHandlerId("3")
                .setPayload("3"));
        long count = store.count(null);
        Assert.assertEquals(count, 2);
    }

    @Test
    public void testDeleteJobsOlderThan() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1"));
        store.saveWorker(new WorkContext()
                .setWorkId("2")
                .setHandlerId("3")
                .setPayload("3"));
        long count = store.count(null);
        Assert.assertEquals(count, 2);

        Thread.sleep(3000);
        Date cutoffDate = new Date();
        store.deleteJobsOlderThan(cutoffDate, WorkerStatus.RUNNING);
        count = store.count(null);
        Assert.assertEquals(count, 0);
    }

    @Test
    public void testGetWorkerById() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1"));

        WorkContext current = store.getWorkerByWorkId("1", "1");
        if (current == null) {
            Assert.fail("current is null");
        }
        Assert.assertEquals(current.getRetryIntervalSeconds(), 0);
        Assert.assertEquals(current.getRetryCount(), 0);

        WorkContext current1 = store.getWorkerById(current.getId());
        if (current1 == null) {
            Assert.fail("current1 is null");
        }
        Assert.assertEquals(current.getId(), current1.getId());
        Assert.assertEquals(current.getMaxRetryCount(), current1.getMaxRetryCount());

    }

    @Test
    public void testGetWorkersByStatus() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1")
                .setRetryIntervalSeconds(3600));
        store.saveWorker(new WorkContext()
                .setWorkId("2")
                .setHandlerId("3")
                .setPayload("3"));
        List<WorkContext> list = store.getWorkersByStatus(WorkerStatus.RUNNING, 10);
        Assert.assertEquals(list.size(), 0);
        list = store.getWorkersByStatus(WorkerStatus.PENDING, 10);
        Assert.assertEquals(list.size(), 2);
    }

    @Test
    public void testList() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1")
                .setMaxRetryCount(20)
                .setRetryIntervalSeconds(3600));
        store.saveWorker(new WorkContext()
                .setWorkId("2")
                .setHandlerId("3")
                .setPayload("3"));

        List<WorkContext> list = store.list(0, 1, null);
        Assert.assertEquals(list.size(), 1);
        WorkContext first = list.get(0);
        Assert.assertEquals(first.getWorkId(), "1");
        Assert.assertEquals(first.getHandlerId(), "1");
        Assert.assertEquals(first.getMaxRetryCount(), 20);
        Assert.assertEquals(first.getRetryIntervalSeconds(), 3600);

        list = store.list(1, 1, null);
        Assert.assertEquals(list.size(), 1);
        first = list.get(0);
        Assert.assertEquals(first.getWorkId(), "2");
        Assert.assertEquals(first.getHandlerId(), "3");
    }

    @Test
    public void testSaveWorker() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1"));
        long count = store.count(null);
        Assert.assertEquals(count, 1);
    }

    @Test
    public void testUpdateWorkerStatus() throws Exception {
        MysqlWorkerStore store = initStore();
        store.saveWorker(new WorkContext()
                .setWorkId("1")
                .setHandlerId("1")
                .setPayload("1")
                .setMaxRetryCount(2));
        WorkContext current = store.getWorkerByWorkId("1", "1");
        if (current == null) {
            Assert.fail("current is null");
        }
        Assert.assertEquals(current.getWorkId(), "1");
        Assert.assertEquals(current.getHandlerId(), "1");
        Assert.assertEquals(current.getCurrentStatus(), WorkerStatus.PENDING);

        store.updateWorkerStatus(current.getId(), WorkerStatus.RUNNING, current.getCurrentStatus(), "test");
        WorkContext current1 = store.getWorkerByWorkId("1", "1");
        if (current1 == null) {
            Assert.fail("current1 is null");
        }
        Assert.assertEquals(current1.getCurrentStatus(), WorkerStatus.RUNNING);

        store.updateWorkerStatus(current.getId(), WorkerStatus.RETRY, current1.getCurrentStatus(), "test retry");
        WorkContext current2 = store.getWorkerByWorkId("1", "1");
        Assert.assertEquals(current2.getCurrentStatus(), WorkerStatus.RETRY);
        Assert.assertEquals(current2.getRetryCount(), 1);
    }

    private DataSource initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/worker");
        config.setUsername("root");
        config.setPassword("root");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setIdleTimeout(30000);
        config.setConnectionTimeout(10000);
        config.setAutoCommit(true);
        config.setConnectionTestQuery("SELECT 1");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }
}
