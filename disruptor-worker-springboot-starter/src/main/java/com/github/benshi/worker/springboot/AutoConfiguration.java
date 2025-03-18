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

import com.github.benshi.worker.DisruptorWorker;

@Configuration
@EnableConfigurationProperties({ WorkerProperties.class,
        DataSourceProperties.class })
@Import({ CustomRedissonAutoConfiguration.class })
@ComponentScan(basePackages = "com.github.benshi.worker.springboot")
public class AutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(DataSource.class)
    public DataSource dataSource(DataSourceProperties dataSourceProperties) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(dataSourceProperties.getDriverClassName());
        dataSource.setUrl(dataSourceProperties.getUrl());
        dataSource.setUsername(dataSourceProperties.getUsername());
        dataSource.setPassword(dataSourceProperties.getPassword());
        return dataSource;
    }

    @Bean
    public DisruptorWorker disruptorWorker(WorkerProperties workerProperties,
            DataSource dateSource,
            RedissonClient redissonClient) {
        int bufferSize = workerProperties.getBufferSize();
        int stayDays = workerProperties.getStayDays();
        return new DisruptorWorker(
                redissonClient,
                dateSource,
                bufferSize,
                stayDays);
    }

    @Bean
    public WorkerBeanPostProcessor workerBeanPostProcessor(DisruptorWorker disruptorWorker) {
        return new WorkerBeanPostProcessor(disruptorWorker);
    }

    @Bean
    public WorkerPublisher workerPublisher(DisruptorWorker disruptorWorker) {
        return new WorkerPublisher(disruptorWorker);
    }
}
