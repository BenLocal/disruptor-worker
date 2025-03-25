package com.github.benshi.worker.store;

import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import com.github.benshi.worker.store.mapper.WorkerJobsMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyBatisConfig {
    private final SqlSessionFactory sqlSessionFactory;

    public MyBatisConfig(DataSource dataSource) {
        try {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("disruptor-worker-store-mybatis", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);

            // Register mappers
            configuration.addMapper(WorkerJobsMapper.class);

            // Other configuration settings
            configuration.setMapUnderscoreToCamelCase(true);

            this.sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
            log.info("MyBatis configuration initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize MyBatis configuration", e);
            throw new RuntimeException("Failed to initialize MyBatis configuration", e);
        }
    }

    public SqlSession openSession() {
        return sqlSessionFactory.openSession();
    }

    public SqlSession openSession(boolean autoCommit) {
        return sqlSessionFactory.openSession(autoCommit);
    }
}
