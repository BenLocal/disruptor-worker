package com.github.benshi.worker.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "worker.redis")
public class RedisProperties {
    /**
     * Redis server host (when using properties)
     */
    private String host = "localhost";

    /**
     * Redis server port (when using properties)
     */
    private int port = 6379;

    /**
     * Redis password (when using properties)
     */
    private String password;

}
