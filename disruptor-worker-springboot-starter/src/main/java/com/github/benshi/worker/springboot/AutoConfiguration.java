package com.github.benshi.worker.springboot;

import javax.sql.DataSource;

import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.github.benshi.worker.CacheDisruptorWorker;
import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.DisruptorWorkerOptions;

@Configuration
@EnableConfigurationProperties({ WorkerProperties.class,
        DataSourceProperties.class,
        RedisProperties.class })
@Import({ CustomRedissonAutoConfiguration.class })
@ComponentScan(basePackages = "com.iflysse.benshi.worker.springboot")
public class AutoConfiguration {
    @Bean(name = "workerDataSource")
    @ConditionalOnMissingBean(DataSource.class)
    public DataSource workDataSource(DataSourceProperties dataSourceProperties) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(dataSourceProperties.getDriverClassName());
        dataSource.setUrl(dataSourceProperties.getUrl());
        dataSource.setUsername(dataSourceProperties.getUsername());
        dataSource.setPassword(dataSourceProperties.getPassword());
        return dataSource;
    }

    @Bean
    public DisruptorWorker disruptorWorker(WorkerProperties workerProperties,
            DataSourceProperties dataSourceProperties,
            RedissonClient redissonClient) {
        int bufferSize = workerProperties.getBufferSize();
        int stayDays = workerProperties.getStayDays();

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(dataSourceProperties.getDriverClassName());
        dataSource.setUrl(dataSourceProperties.getUrl());
        dataSource.setUsername(dataSourceProperties.getUsername());
        dataSource.setPassword(dataSourceProperties.getPassword());

        DisruptorWorkerOptions options = DisruptorWorkerOptions.builder()
                .storeName(workerProperties.getStoreDirver())
                .bufferSize(bufferSize)
                .stayDays(stayDays)
                .build();

        return new DisruptorWorker(
                redissonClient,
                dataSource,
                options);
    }

    @Bean
    public CacheDisruptorWorker cacheDisruptorWorker(WorkerProperties workerProperties,
            RedissonClient redissonClient) {
        int bufferSize = workerProperties.getBufferSize() * 1024;
        if (bufferSize > 1024 * 1024) {
            bufferSize = 1024 * 1024;
        }
        int stayDays = workerProperties.getStayDays();

        DisruptorWorkerOptions options = DisruptorWorkerOptions.builder()
                .storeName(workerProperties.getStoreDirver())
                .bufferSize(bufferSize)
                .stayDays(stayDays)
                .build();

        return new CacheDisruptorWorker(
                redissonClient,
                options);
    }

    @Bean
    public WorkerBeanPostProcessor workerBeanPostProcessor(DisruptorWorker disruptorWorker,
            CacheDisruptorWorker cacheDisruptorWorker) {
        return new WorkerBeanPostProcessor(disruptorWorker, cacheDisruptorWorker);
    }

    @Bean
    public WorkerPublisher workerPublisher(DisruptorWorker disruptorWorker,
            CacheDisruptorWorker cacheDisruptorWorker) {
        return new WorkerPublisher(disruptorWorker, cacheDisruptorWorker);
    }
}
