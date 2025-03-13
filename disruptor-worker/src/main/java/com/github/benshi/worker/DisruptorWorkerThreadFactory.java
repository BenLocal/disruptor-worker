package com.github.benshi.worker;

import java.util.concurrent.ThreadFactory;

public class DisruptorWorkerThreadFactory implements ThreadFactory {
    private final String name;

    public DisruptorWorkerThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(String.format("%s-%s", name, t.getId()));
        return t;
    }

}
