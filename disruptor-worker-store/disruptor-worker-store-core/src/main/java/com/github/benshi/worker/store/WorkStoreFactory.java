package com.github.benshi.worker.store;

import javax.sql.DataSource;

public interface WorkStoreFactory {
    String name();

    WorkerStore create(DataSource dataSource);
}
