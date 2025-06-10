package com.github.benshi.worker.springboot;

import com.github.benshi.worker.CacheDisruptorWorker;
import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.JsonUtils;
import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkerHandler;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WorkerPublisher {
    private final DisruptorWorker worker;
    private final CacheDisruptorWorker cacheWorker;

    public boolean publish(Class<? extends WorkerHandler> clazz, String workerId, Object payload) {
        return publish(clazz, workerId, payload, false);
    }

    public boolean publish(Class<? extends WorkerHandler> clazz, String workerId, Object payload, boolean froce) {
        String s = null;
        if (payload != null) {
            if (payload instanceof String) {
                s = (String) payload;
            } else {
                s = JsonUtils.toJson(payload);
            }
        }

        return publish(clazz, workerId, s, froce);
    }

    public boolean publish(Class<? extends WorkerHandler> clazz, String workerId, String payload) {
        return publish(clazz, workerId, payload, false);
    }

    public boolean publish(Class<? extends WorkerHandler> clazz, WorkerPublishOptions options) {
        String handlerId = clazz.getName();
        Worker aw = clazz.getAnnotation(Worker.class);
        boolean cache = false;
        if (aw != null) {
            cache = aw.cache();
        }
        return publish(options
                .setHandlerId(handlerId)
                .setCache(cache));
    }

    public boolean publish(
            Class<? extends WorkerHandler> clazz, String workerId, String payload, boolean froce) {
        return publish(clazz, new WorkerPublishOptions()
                .setWorkId(workerId)
                .setPayload(payload)
                .setForce(froce));
    }

    public boolean publish(WorkerPublishOptions options) {
        if (options == null || options.getHandlerId() == null || options.getWorkId() == null) {
            return false;
        }
        WorkContext ctx = new WorkContext()
                .setWorkId(options.getWorkId())
                .setHandlerId(options.getHandlerId())
                .setPayload(options.getPayload())
                .setForce(options.isForce())
                .setLockStr(options.getLockStr());

        if (options.isCache()) {
            return cacheWorker.submit(ctx);
        } else {
            return worker.submit(ctx);
        }
    }
}
