package com.github.benshi.worker.store;

import java.util.HashMap;
import java.util.ServiceLoader;

public class WorkerStoreProcessor {
    private static final HashMap<String, WorkStoreFactory> registers = new HashMap<>();

    static {
        synchronized (WorkerStoreProcessor.class) {
            ServiceLoader.load(WorkStoreFactory.class)
                    .iterator()
                    .forEachRemaining(register -> {
                        registers.put(register.name(), register);
                    });
        }
    }

    public static WorkStoreFactory get(String name) {
        WorkStoreFactory current = registers.get(name);
        if (current == null) {
            throw new IllegalArgumentException("No such WorkStoreFactory: " + name);
        }
        return current;
    }
}
