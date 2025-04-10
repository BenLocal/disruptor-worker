package com.github.benshi.worker.store.mybatis;

import java.util.Properties;

import javax.sql.DataSource;

import com.github.benshi.worker.store.WorkStoreFactory;
import com.github.benshi.worker.store.WorkerStore;

public class MybatisWorkStoreFactory implements WorkStoreFactory {
    @Override
    public String name() {
        return "mybatis";
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
        return new MybatisWorkerStore(new MyBatisConfig((DataSource) dataSource));
    }

}
