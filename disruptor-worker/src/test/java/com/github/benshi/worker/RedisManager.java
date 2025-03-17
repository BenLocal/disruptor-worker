package com.github.benshi.worker;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * 
 * 
 * @date 2025年3月13日
 * @time 21:19:47
 * @author tangchuanyu
 * @description
 * 
 */
public class RedisManager {
    private static RedisManager instance;
    private RedissonClient client;

    private RedisManager() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        this.client = Redisson.create(config);
    }

    public static synchronized RedisManager getInstance() {
        if (instance == null) {
            instance = new RedisManager();
        }
        return instance;
    }

    public RedissonClient getClient() {
        return client;
    }
}
