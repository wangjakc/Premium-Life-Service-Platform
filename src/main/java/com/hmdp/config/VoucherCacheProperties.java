package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.cache.voucher-list")
public class VoucherCacheProperties {
    /**
     * 缓存模式: mysql(仅DB), redis(仅Redis+DB回源), caffeine(本地缓存+Redis+DB回源)
     */
    private String mode = "caffeine";

    /**
     * 本地缓存刷新时间, 默认为 5 seconds, 读取配置值
     */
    private Duration refreshAfterWrite = Duration.ofSeconds(5);

    /**
     *  本地缓存硬过期时间, 默认为 10 minutes, 读取配置值
     */
    private Duration  expireAfterWrite = Duration.ofMinutes(10);

    /**
     *  Redis 缓存过期时间, 默认为 10 minutes, 读取配置值
     *  注意: 当使用逻辑过期时,这个字段不生效,数据不会因为TTL而失效
     */
    private Duration redisTtl = Duration.ofMinutes(10);

    /**
     * 逻辑过期时间, 默认为 10 minutes
     * 用于解决缓存击穿问题: Redis Key永不过期,但数据本身携带逻辑过期时间
     * 当系统时间超过expireTime时,数据被认为已过期,触发异步重建
     */
    private Duration logicalExpireTime = Duration.ofMinutes(10);
}
