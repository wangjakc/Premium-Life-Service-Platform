package com.hmdp.utils;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Redis 健康检查器 + 熔断器
 *
 * 完全依赖熔断器的状态机（CLOSED → OPEN → HALF_OPEN → CLOSED）实现：
 * - CLOSED：正常，Redis 限流
 * - OPEN：Redis 不可用，请求直接降级到本地限流，不等超时
 * - HALF_OPEN：尝试3次探测，成功则恢复 CLOSED，失败则回到 OPEN
 *
 * 无需定时任务，无需主动探测，HALF_OPEN 状态本身就是探测
 */
@Slf4j
@Component
public class RedisHealthChecker {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private final CircuitBreaker circuitBreaker;

    public RedisHealthChecker(CircuitBreakerRegistry registry) {
        this.circuitBreaker = registry.circuitBreaker("redis-fallback");
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn("Redis 熔断器状态变更: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
    }

    /**
     * 检查 Redis 是否可用
     * CLOSED/HALF_OPEN → 可用（HALF_OPEN时放行让探测执行）
     * OPEN → 不可用（直接降级本地限流，ms级）
     */
    public boolean isRedisAvailable() {
        return circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }

    /**
     * 记录一次 Redis 调用失败
     * 熔断器自动统计，失败率达到阈值后自动 OPEN
     */
    public void recordSuccess() {
        circuitBreaker.recordSuccess();
    }

    /**
     * 记录一次 Redis 调用成功
     */
    public void recordFailure() {
        circuitBreaker.recordFailure();
    }
}