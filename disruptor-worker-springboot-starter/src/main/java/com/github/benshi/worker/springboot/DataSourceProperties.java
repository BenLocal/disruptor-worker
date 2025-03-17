package com.github.benshi.worker.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "worker.datasource")
public class DataSourceProperties {
    private String username;
    private String password;
    private String url;
    private String driverClassName;
}
