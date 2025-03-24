package com.github.benshi.worker.store;

import javax.sql.DataSource;

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
