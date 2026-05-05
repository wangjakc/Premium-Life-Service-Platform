package com.hmdp.limter.aop;

import com.hmdp.dto.UserDTO;
import com.hmdp.limter.LocalRateLimiter;
import com.hmdp.limter.annotation.LimtRate;
import com.hmdp.limter.exception.RateLimitException;
import com.hmdp.utils.RedisHealthChecker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.util.Collections;

@Aspect
@Component
@Slf4j
public class RateLimiterAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisHealthChecker redisHealthChecker;

    /**
     * 本地限流器（降级方案，Redis不可用时启用）
     */
    private final LocalRateLimiter localRateLimiter = new LocalRateLimiter();

    // Redis 限流 Lua 脚本
    private static final DefaultRedisScript<Long> LIMIT_SCRIPT;

    static {
        LIMIT_SCRIPT = new DefaultRedisScript<>();
        LIMIT_SCRIPT.setLocation(new ClassPathResource("limiter.lua"));
        LIMIT_SCRIPT.setResultType(Long.class);
    }

    /**
     * 标记是否正在使用本地限流（用于日志打印，避免重复）
     */
    private volatile boolean usingLocalFallback = false;

    @PostConstruct
    public void init() {
        log.info("RateLimiterAspect 初始化完成");
    }

    /**
     * 限流拦截
     *
     * 策略：
     * 1. 优先使用 Redis 限流（精确、可分布式协调）
     * 2. Redis 不可用时，降级到本地限流（保护数据库）
     *
     * 注意：这里只负责限流，不负责业务处理
     */
    @Before("@annotation(rateLimiter)")
    public void doBefore(JoinPoint point, LimtRate rateLimiter) {
        String key = rateLimiter.key();
        long window = rateLimiter.window();
        long limit = rateLimiter.limit();
        String fullKey = buildRateLimitKey(point, rateLimiter, key);

        // ========== 优先：Redis 限流 ==========
        if (redisHealthChecker.isRedisAvailable()) {
            try {
                Long result = executeRedisLimiter(fullKey, window, limit);
                if (result != null && result == 0) {
                    log.warn("Redis限流拦截，key: {}", fullKey);
                    throw new RateLimitException(rateLimiter.message());
                }
                // 限流通过
                if (usingLocalFallback) {
                    log.info("Redis 已恢复，切换回 Redis 限流");
                    usingLocalFallback = false;
                }
                return;
            } catch (RateLimitException e) {
                throw e;
            } catch (Exception e) {
                // Redis 限流异常，降级到本地限流
                log.error("Redis 限流异常，降级到本地限流，key: {}, error: {}", fullKey, e.getMessage());
                redisHealthChecker.setRedisAvailable(false);
            }
        }

        // ========== 降级：本地限流 ==========
        if (!usingLocalFallback) {
            log.warn("Redis 不可用，切换到本地限流降级方案");
            usingLocalFallback = true;
        }

        boolean allowed = localRateLimiter.tryAcquire(fullKey, (int) limit, (int) window);
        if (!allowed) {
            log.warn("本地限流拦截，key: {}", fullKey);
            throw new RateLimitException(rateLimiter.message());
        }
    }

    /**
     * 执行 Redis 滑动窗口限流
     */
    private Long executeRedisLimiter(String key, Long window, Long limit) {
        long now = System.currentTimeMillis();
        return stringRedisTemplate.execute(
                LIMIT_SCRIPT,
                Collections.singletonList(key),
                window.toString(), limit.toString(), Long.toString(now)
        );
    }

    /**
     * 构建限流 key
     */
    private String buildRateLimitKey(JoinPoint point, LimtRate rateLimiter, String baseKey) {
        StringBuilder keyBuilder = new StringBuilder(baseKey);

        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = signature.getMethod();

        keyBuilder.append(method.getDeclaringClass().getName())
                .append(":")
                .append(method.getName());

        switch (rateLimiter.type()) {
            case IP:
                keyBuilder.append(":ip:").append(getClientIp());
                break;
            case USER:
                keyBuilder.append(":user:").append(getCurrentUserId());
                break;
            case METHOD:
            default:
                break;
        }

        return keyBuilder.toString();
    }

    private String getClientIp() {
        try {
            javax.servlet.http.HttpServletRequest request =
                    ((org.springframework.web.context.request.ServletRequestAttributes)
                            org.springframework.web.context.request.RequestContextHolder.getRequestAttributes())
                            .getRequest();
            String ip = request.getHeader("X-Forwarded-For");
            if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("Proxy-Client-IP");
            }
            if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("WL-Proxy-Client-IP");
            }
            if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getRemoteAddr();
            }
            return ip;
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String getCurrentUserId() {
        try {
            UserDTO user = UserHolder.getUser();
            if (user != null) {
                return String.valueOf(user.getId());
            }
        } catch (Exception e) {
            // ignore
        }
        return "anonymous";
    }
}