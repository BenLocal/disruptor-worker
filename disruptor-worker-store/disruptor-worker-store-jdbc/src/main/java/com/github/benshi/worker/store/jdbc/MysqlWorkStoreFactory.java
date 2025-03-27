package com.github.benshi.worker.store.jdbc;

import javax.sql.DataSource;

import com.github.benshi.worker.store.WorkStoreFactory;
import com.github.benshi.worker.store.WorkerStore;

public class MysqlWorkStoreFactory implements WorkStoreFactory {
    @Override
    public String name() {
        return "mysql-jdbc";
    }

    @Override
    public WorkerStore create(DataSource dataSource) {
        return new MysqlWorkerStore(dataSource);
    }

}
