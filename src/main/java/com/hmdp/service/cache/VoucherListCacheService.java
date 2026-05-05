package com.hmdp.service.cache;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.config.VoucherCacheProperties;
import com.hmdp.dto.VoucherWithExpireTime;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_VOUCHER_LIST_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_VOUCHER_LIST_KEY;

@Slf4j
@Service
public class VoucherListCacheService {

    @Resource
    private VoucherMapper voucherMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private VoucherCacheProperties cacheProperties;
    @Resource
    private RedissonClient redissonClient;

    // 【秒杀优惠券信息多级缓存改造】caffeine缓存的定义, key是shopId, value是优惠券列表的json字符串
    private LoadingCache<Long, List<Voucher>> voucherListCache;

    // 【秒杀优惠券信息多级缓存改造】caffeine缓存的初始化,会在项目启动时调用
    @PostConstruct
    public void initCache() {
        /*
            refreshAfterWrite(x, TimeUnit.MILLISECONDS)
            含义：写入后多久触发"刷新"。
            到了这个时间后，下一次访问该 key 时，会触发重新加载最新值（注意是异步线程 refresh，当前线程会先返回旧值）。
            重点：它不是立刻删除缓存，而是让缓存"该更新了" 提高数据一致性

            expireAfterWrite(y, TimeUnit.MILLISECONDS)
            含义：写入后多久"过期失效"。
            超过这个时间，缓存项会被当作不存在，下次访问会走加载逻辑（通常同步加载新值）。
            重点：这是硬过期时间，过期后旧值不再可用。
        */
        voucherListCache = Caffeine.newBuilder()
                .maximumSize(1000)  // 缓存最大容量值
                .refreshAfterWrite(cacheProperties.getRefreshAfterWrite().toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheProperties.getExpireAfterWrite().toMillis(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<Long, List<Voucher>>() {
                    @Override
                    public List<Voucher> load(Long shopId) {
                        return loadFromRedisThenDb(shopId);
                    }
                });
    }


    // 通过商户ID获取商户的秒杀优惠券列表
    public List<Voucher> getVoucherByShopId(Long shopId) {
        String mode = cacheProperties.getMode();
        switch (mode.toLowerCase(Locale.ROOT)) {
            case "mysql":
                return loadFromDb(shopId);
            case "redis":
                return loadFromRedisThenDb(shopId);
            case "caffeine":
            default:
                // 直接从本地缓存中获取,若本地缓存中不存在会执行load函数,加载最新数据再返回
                return voucherListCache.get(shopId);
        }
    }


    /**
     * 从Redis加载数据,如果数据不存在或已逻辑过期则回源到数据库
     * 使用逻辑过期时间解决缓存击穿问题
     *
     * 流程:
     * 1. 先从Redis获取数据
     * 2. 检查数据是否为空或已逻辑过期
     * 3. 如果已过期,尝试获取分布式锁
     * 4. 获取锁成功的线程开启独立线程重建缓存,本线程直接返回旧数据
     * 5. 获取锁失败的线程直接返回旧数据
     * 6. 无锁情况下如果Redis无数据,回源到MySQL
     */
    private List<Voucher> loadFromRedisThenDb(Long shopId) {
        String key = CACHE_VOUCHER_LIST_KEY + shopId;

        // 1) L2: 读Redis
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            // 2) Redis中有数据,检查是否逻辑过期
            VoucherWithExpireTime voucherWithExpireTime = JSONUtil.toBean(json, VoucherWithExpireTime.class);

            // 3) 判断是否逻辑过期 (当前时间 < 过期时间)
            if (voucherWithExpireTime.getExpireTime() > System.currentTimeMillis()) {
                // 未过期,直接返回数据
                return voucherWithExpireTime.getVouchers();
            }

            // 4) 已逻辑过期,尝试获取分布式锁
            String lockKey = LOCK_VOUCHER_LIST_KEY + shopId;
            RLock lock = redissonClient.getLock(lockKey);

            // 尝试获取锁,非阻塞,获取成功返回true
            boolean isLockAcquired = lock.tryLock();

            if (isLockAcquired) {
                try {
                    // 5) 获取锁成功,开启独立线程重建缓存
                    // 双重检查:获取锁后再次检查是否过期(防止其他线程已重建)
                    String jsonAfterLock = stringRedisTemplate.opsForValue().get(key);
                    if (StrUtil.isNotBlank(jsonAfterLock)) {
                        VoucherWithExpireTime cachedData = JSONUtil.toBean(jsonAfterLock, VoucherWithExpireTime.class);
                        if (cachedData.getExpireTime() > System.currentTimeMillis()) {
                            // 其他线程已重建,直接返回
                            return cachedData.getVouchers();
                        }
                    }

                    // 执行缓存重建 (独立线程)
                    rebuildCacheWithLogicalExpire(shopId);
                } finally {
                    // 释放锁
                    lock.unlock();
                }
            }

            // 6) 获取锁失败 或 已重建,返回Redis中的旧数据 (即使过期也返回,保证可用性)
            return voucherWithExpireTime.getVouchers();
        }

        // 7) Redis中不存在,回源到 MySQL
        List<Voucher> vouchers = loadFromDb(shopId);

        if (CollectionUtil.isNotEmpty(vouchers)) {
            // 8) MySQL中数据不为空才写回 Redis (使用逻辑过期,不设置Redis TTL)
            saveWithLogicalExpire(shopId, vouchers);
        }

        return vouchers;
    }

    /**
     * 保存优惠券列表到Redis,使用逻辑过期时间(不依赖Redis TTL)
     *
     * @param shopId 店铺ID
     * @param vouchers 优惠券列表
     */
    private void saveWithLogicalExpire(Long shopId, List<Voucher> vouchers) {
        String key = CACHE_VOUCHER_LIST_KEY + shopId;

        // 计算逻辑过期时间: 当前时间 + 逻辑过期时长
        long expireTime = System.currentTimeMillis() + cacheProperties.getLogicalExpireTime().toMillis();

        // 包装成VoucherWithExpireTime
        VoucherWithExpireTime voucherWithExpireTime = new VoucherWithExpireTime(vouchers, expireTime);

        // 写入Redis,不设置TTL (逻辑过期由应用层控制)
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(voucherWithExpireTime));
    }

    /**
     * 重建缓存 (异步执行)
     * 由获取锁成功的线程调用,开启独立线程查询数据库并更新缓存
     *
     * @param shopId 店铺ID
     */
    private void rebuildCacheWithLogicalExpire(Long shopId) {
        // 提交异步任务重建缓存
        threadPoolTaskExecutor().submit(() -> {
            log.info("开始重建优惠券缓存, shopId={}", shopId);

            // 1) 从MySQL查询最新数据
            List<Voucher> vouchers = loadFromDb(shopId);

            // 2) 写入Redis (设置新的逻辑过期时间)
            if (CollectionUtil.isNotEmpty(vouchers)) {
                saveWithLogicalExpire(shopId, vouchers);
                log.info("优惠券缓存重建完成, shopId={}, voucherCount={}", shopId, vouchers.size());
            }
        });
    }

    /**
     * 获取线程池 (用于异步重建缓存)
     * 使用Spring默认的异步任务执行器
     */
    private java.util.concurrent.ExecutorService threadPoolTaskExecutor() {
        return java.util.concurrent.Executors.newSingleThreadExecutor();
    }

    private List<Voucher> loadFromDb(Long shopId) {
        List<Voucher> vouchers = voucherMapper.queryVoucherOfShop(shopId);
        return vouchers == null ? Collections.emptyList() : vouchers;
    }
}