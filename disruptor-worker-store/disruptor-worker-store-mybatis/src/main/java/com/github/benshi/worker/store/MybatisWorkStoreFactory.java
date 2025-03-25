package com.github.benshi.worker.store;

import javax.sql.DataSource;

public class MybatisWorkStoreFactory implements WorkStoreFactory {
    @Override
    public String name() {
        return "mybatis";
    }

    @Override
    public WorkerStore create(DataSource dataSource) {
        MyBatisConfig config = new MyBatisConfig(dataSource);
        return new MybatisWorkerStore(config);
    }

}
