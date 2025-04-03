package com.github.benshi.worker;

import org.junit.Test;

import lombok.Data;

public class DisruptorWorkerTest {
    @Test
    public void testStart() throws Exception {
        DataSourceManager dataSourceManager = DataSourceManager.getInstance();
        RedisManager redisManager = RedisManager.getInstance();
        DisruptorWorker worker = new DisruptorWorker(redisManager.getClient(),
                dataSourceManager.getDataSource(), DisruptorWorkerOptions.builder()
                        .storeName("mysql-jdbc").build());
        worker.start();

        worker.register("TestWorkHandler", new TestWorkHandler(), 2);

        for (int i = 0; i < 100; i++) {
            WorkContext ctx = new WorkContext()
                    .setWorkId("TestWorkHandler" + i)
                    .setHandlerId("TestWorkHandler")
                    .setPayload("hello world " + i)
                    .setForce(false);
            worker.submit(ctx);
        }

        Thread.sleep(5000);
        worker.shutdown();
    }

    private static class TestWorkHandler implements WorkerHandler {

        @Override
        public WorkHandlerResult run(WorkHandlerMessage msg) throws Exception {
            System.out.println(msg.getWorkId() + " " + msg.getPayload());
            return WorkHandlerResult.success();
        }

    }

    @Data
    private static class TestWorkHandlerMessage {
        private String name;
    }
}
