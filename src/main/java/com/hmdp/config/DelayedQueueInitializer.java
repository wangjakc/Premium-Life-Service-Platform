package com.hmdp.config;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
public class DelayedQueueInitializer {

    private static final String SECKILL_ORDERS_READY_QUEUE = "seckill:orders:ready";
    private static final String SECKILL_STOCK_KEY_PREFIX = "seckill:stock:";
    private static final String SECKILL_ORDER_KEY_PREFIX = "seckill:order:";

    private static final int ORDER_STATUS_UNPAID = 1;
    private static final int ORDER_STATUS_CANCELLED = 4;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @PostConstruct
    public void init() {
        RBlockingQueue<String> readyQueue = redissonClient.getBlockingQueue(SECKILL_ORDERS_READY_QUEUE);

        Thread consumerThread = new Thread(() -> {
            log.info("超时订单消费者线程启动");
            while (true) {
                String message = null;
                try {
                    // BLPOP 阻塞获取就绪队列中的过期订单
                    message = readyQueue.poll(30, TimeUnit.SECONDS);
                    if (message == null) {
                        continue;
                    }
                    log.info("收到超时订单消息: {}", message);
                    processExpiredOrder(message);
                } catch (InterruptedException e) {
                    log.warn("超时订单消费者线程被中断", e);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("处理超时订单异常，message: {}", message, e);
                }
            }
        }, "voucher-order-timeout-consumer");

        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void processExpiredOrder(String message) {
        // 解析消息：userId:voucherId:orderId
        String[] parts = message.split(":");
        Long userId = Long.parseLong(parts[0]);
        Long voucherId = Long.parseLong(parts[1]);
        Long orderId = Long.parseLong(parts[2]);

        // 1.查询订单状态
        VoucherOrder order = voucherOrderService.getById(orderId);
        if (order == null) {
            log.warn("订单不存在，orderId: {}", orderId);
            return;
        }

        // 2.只有未支付订单才需要关单
        if (order.getStatus() != ORDER_STATUS_UNPAID) {
            log.info("订单状态不是未支付，跳过，orderId: {}, status: {}", orderId, order.getStatus());
            return;
        }

        // 3.乐观锁更新订单状态为已取消（只有status=1才更新）
        boolean updated = voucherOrderService.update()
                .eq("id", orderId)
                .eq("status", ORDER_STATUS_UNPAID)
                .set("status", ORDER_STATUS_CANCELLED)
                .update();

        if (!updated) {
            log.warn("订单状态更新失败（可能被其他线程处理），orderId: {}", orderId);
            return;
        }

        log.info("订单状态已更新为已取消，orderId: {}", orderId);

        // 4.回滚Redis库存（使用incr原子增加库存）
        String stockKey = SECKILL_STOCK_KEY_PREFIX + voucherId;
        stringRedisTemplate.opsForValue().increment(stockKey);
        log.info("Redis库存已回滚，voucherId: {}, orderId: {}", voucherId, orderId);

        // 5.回滚MySQL库存（乐观锁）
        boolean stockRestored = voucherOrderService.restoreStock(voucherId);
        if (!stockRestored) {
            log.warn("MySQL库存回滚失败，voucherId: {}", voucherId);
        } else {
            log.info("MySQL库存已回滚，voucherId: {}, orderId: {}", voucherId, orderId);
        }

        // 6.删除用户下单资格
        String orderKey = SECKILL_ORDER_KEY_PREFIX + voucherId;
        stringRedisTemplate.opsForHash().delete(orderKey, userId.toString());
        log.info("用户下单资格已删除，userId: {}, voucherId: {}", userId, voucherId);
    }
}
