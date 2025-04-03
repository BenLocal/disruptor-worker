package com.github.benshi.worker.springboot;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;

import com.github.benshi.worker.CacheDisruptorWorker;
import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.WorkerHandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class WorkerBeanPostProcessor implements BeanPostProcessor, InitializingBean, DisposableBean {
    private final DisruptorWorker worker;
    private final CacheDisruptorWorker cacheWorker;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

        if (bean != null) {
            selectWorkerAnnotation(bean);
        }
        return bean;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    private void selectWorkerAnnotation(Object bean) {
        Class<?> clazz = bean.getClass();
        Worker aw = clazz.getAnnotation(Worker.class);
        if (aw == null) {
            return;
        }

        boolean isHandler = false;
        for (Class<?> item : bean.getClass().getInterfaces()) {
            if (item.isAssignableFrom(WorkerHandler.class)) {
                isHandler = true;
                break;
            }
        }

        if (!isHandler) {
            return;
        }

        String handlerId = clazz.getName();
        int limit = aw.limit();

        if (aw.cache()) {
            cacheWorker.register(handlerId, (WorkerHandler) bean, limit);
        } else {
            this.worker.register(handlerId, (WorkerHandler) bean, limit);
        }
        log.info("Registered handler {} with limit {}", handlerId, limit);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // Start the DisruptorWorker when Spring context is initialized
        log.info("Starting DisruptorWorker...");
        try {
            this.worker.start();
        } catch (Exception e) {
            // ignore the exception
        }

        try {
            this.cacheWorker.start();
        } catch (Exception e) {
            // ignore the exception
        }

        log.info("DisruptorWorker started successfully");
    }

    @Override
    public void destroy() throws Exception {
        // Shutdown the DisruptorWorker when Spring context is closed
        log.info("Shutting down DisruptorWorker...");
        try {
            this.worker.shutdown();
        } catch (Exception e) {
            // ignore the exception
        }

        try {
            this.cacheWorker.shutdown();
        } catch (Exception e) {
            // ignore the exception
        }

        log.info("DisruptorWorker shutdown complete");
    }
}
