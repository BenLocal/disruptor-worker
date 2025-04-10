package com.github.benshi.worker.store.jdbc;

import java.util.Properties;

import javax.sql.DataSource;

import com.github.benshi.worker.store.WorkStoreFactory;
import com.github.benshi.worker.store.WorkerStore;

public class MysqlWorkStoreFactory implements WorkStoreFactory {
    @Override
    public String name() {
        return "mysql-jdbc";
    }

    @Override
    public WorkerStore create(Properties properties) {
        Object dataSource = (DataSource) properties.get("dataSource");
        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource is required");
        }
        if (!(dataSource instanceof DataSource)) {
            throw new IllegalArgumentException("DataSource is not valid");
        }
        return new MysqlWorkerStore((DataSource) dataSource);
    }

}
