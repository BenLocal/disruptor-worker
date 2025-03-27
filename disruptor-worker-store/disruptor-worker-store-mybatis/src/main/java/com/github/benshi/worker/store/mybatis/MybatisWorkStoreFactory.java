package com.github.benshi.worker.store.mybatis;

import javax.sql.DataSource;

import com.github.benshi.worker.store.WorkStoreFactory;
import com.github.benshi.worker.store.WorkerStore;

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
