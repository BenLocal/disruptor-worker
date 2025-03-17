package com.github.benshi.worker.springboot;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@AutoConfigureBefore(RedisAutoConfiguration.class)
@EnableConfigurationProperties({ RedisProperties.class })
public class CustomRedissonAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @Primary
    public RedissonClient redissonClient(RedisProperties redisProperties) {
        Config config = new Config();
        SingleServerConfig single = config.useSingleServer();
        single.setAddress("redis://" + redisProperties.getHost() + ":" + redisProperties.getPort());

        if (redisProperties.getPassword() != null) {
            single.setPassword(redisProperties.getPassword());
        }

        single.setDatabase(0);
        log.info("Initializing RedissonClient with database configuration");
        return Redisson.create(config);
    }
}
