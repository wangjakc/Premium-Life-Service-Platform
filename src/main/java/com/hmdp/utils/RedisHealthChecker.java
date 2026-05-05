package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis 健康检查器
 * 用于检测 Redis 是否可用，决定是否需要降级
 */
@Slf4j
@Component
public class RedisHealthChecker {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // Redis 可用状态标记
    private final AtomicBoolean redisAvailable = new AtomicBoolean(true);

    // 健康检查开关
    private volatile boolean healthCheckEnabled = true;

    // 降级状态标记（用于日志去重）
    private final AtomicBoolean descendActivated = new AtomicBoolean(false);

    /**
     * 检查 Redis 是否可用
     * @return true-可用，false-不可用（需要降级）
     */
    public boolean isRedisAvailable() {
        if (!healthCheckEnabled) {
            return true; // 关闭健康检查时，默认认为 Redis 可用
        }

        if (redisAvailable.get()) {
            return true; // 快速路径：如果标记为可用，直接返回
        }

        // 标记为不可用时，进行实际探测（避免误判）
        return checkRedisHealth();
    }

    /**
     * 执行 Redis 健康检查
     */
    public boolean checkRedisHealth() {
        try {
            RedisConnection connection = stringRedisTemplate.getConnectionFactory()
                    .getConnection();
            connection.ping();
            connection.close();

            // 健康检查通过，更新状态
            if (!redisAvailable.get()) {
                log.info("Redis 健康检查通过，Redis 服务已恢复");
                redisAvailable.set(true);
                descendActivated.set(false); // 重置降级激活标记
            }
            return true;
        } catch (Exception e) {
            // 健康检查失败，更新状态
            if (redisAvailable.get()) {
                log.error("Redis 健康检查失败，将执行降级方案", e);
                redisAvailable.set(false);
            }
            return false;
        }
    }

    /**
     * 定时探测 Redis 状态（每 5 秒一次）
     * 当 Redis 不可用时，定期探测是否恢复
     */
    @Scheduled(fixedRate = 5000)
    public void scheduledHealthCheck() {
        // 只有在 Redis 被认为不可用时才进行探测
        if (!redisAvailable.get()) {
            boolean healthy = checkRedisHealth();
            if (healthy && !descendActivated.get()) {
                log.info("Redis 服务已恢复，自动切换回正常模式");
            } else if (!healthy) {
                // 降级激活标记，避免重复日志
                if (descendActivated.compareAndSet(false, true)) {
                    log.warn("Redis 服务不可用，已激活降级模式，将持续探测恢复状态");
                }
            }
        }
    }

    /**
     * 手动设置 Redis 可用状态（用于测试或手动切换）
     */
    public void setRedisAvailable(boolean available) {
        redisAvailable.set(available);
        if (available) {
            log.info("手动设置 Redis 为可用状态");
        } else {
            log.warn("手动设置 Redis 为不可用状态，将执行降级");
        }
    }

    /**
     * 启用/禁用健康检查
     */
    public void setHealthCheckEnabled(boolean enabled) {
        this.healthCheckEnabled = enabled;
    }

    /**
     * 当前是否需要降级（Redis 不可用）
     */
    public boolean needDescend() {
        return !isRedisAvailable();
    }
}
