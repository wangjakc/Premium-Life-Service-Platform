package com.hmdp.limter;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * 本地限流器（不依赖Redis）
 * 使用 Guava RateLimiter（令牌桶）+ Caffeine Cache（自动过期）
 */
@Slf4j
public class LocalRateLimiter {

    private static final Cache<String, RateLimiter> limiters = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build();

    public boolean tryAcquire(String key, int limit, int windowSeconds) {
        RateLimiter limiter = limiters.get(key, k -> RateLimiter.create(limit));
        return limiter.tryAcquire();
    }
}