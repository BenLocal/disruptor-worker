package com.github.benshi.worker.store;

import java.util.Properties;

public interface WorkStoreFactory {
    String name();

    WorkerStore create(Properties properties);
}
