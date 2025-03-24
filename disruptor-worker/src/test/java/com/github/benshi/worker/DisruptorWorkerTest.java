package com.github.benshi.worker;

import org.junit.Test;

import lombok.Data;

public class DisruptorWorkerTest {
    @Test
    public void testStart() throws Exception {
        DataSourceManager dataSourceManager = DataSourceManager.getInstance();
        RedisManager redisManager = RedisManager.getInstance();
        DisruptorWorker worker = new DisruptorWorker(redisManager.getClient(),
                dataSourceManager.getDataSource(), "mysql-jdbc");
        worker.start();

        worker.register("TestWorkHandler", new TestWorkHandler(), 2);

        for (int i = 0; i < 100; i++) {
            worker.submit("TestWorkHandler" + i, "TestWorkHandler", "hello world " + i, false);
        }

        Thread.sleep(5000);
        worker.shutdown();
    }

    private static class TestWorkHandler implements WorkHandler {

        @Override
        public WorkHandlerResult run(WorkHandlerMessage msg) {
            System.out.println(msg.getWorkId() + " " + msg.getPayload());
            return WorkHandlerResult.success();
        }

    }

    @Data
    private static class TestWorkHandlerMessage {
        private String name;
    }
}
