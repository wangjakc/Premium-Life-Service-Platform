package com.hmdp.config;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.IdUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SnowflakeConfig {

    @Bean
    public Snowflake snowflake() {
        // 获取本机 IP 的最后一段作为 workerId
        long workerId = NetUtil.ipv4ToLong(NetUtil.getLocalhostStr()) % 32;
        // 使用固定的 dataCenterId
        long dataCenterId = 1;
        
        log.info("初始化 Snowflake: workerId={}, dataCenterId={}", workerId, dataCenterId);
        return IdUtil.createSnowflake(workerId, dataCenterId);
    }
}
