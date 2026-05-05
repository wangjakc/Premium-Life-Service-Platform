package com.hmdp.service.impl;

import cn.hutool.core.lang.Snowflake;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Resource
    private Snowflake snowflake;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    private static final String SECKILL_ORDER_TOPIC = "seckill-order";

    private static final long KAFKA_SEND_TIMEOUT = 5L;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.使用雪花算法生成唯一订单ID
        long orderId = snowflake.nextId();

        // 2.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 3.判断结果是否为0
        if (r != 0) {
            // 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 4.异步发送Kafka消息（acks=all + retries=3 保证不丢失）
        String message = userId + ":" + voucherId + ":" + orderId;
        kafkaTemplate.send(SECKILL_ORDER_TOPIC, message)
                .addCallback(
                        success -> log.info("Kafka消息发送成功，orderId: {}", orderId),
                        failure -> log.error("Kafka消息发送失败，orderId: {}, message: {}", orderId, message, failure)
                );

        // 5.返回订单id（异步发送，不阻塞用户）
        return Result.ok(orderId);
    }

    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(Long userId, Long voucherId, Long orderId) {
        try {
            // 1.创建订单（订单ID为主键，天然幂等，重复插入会失败）
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 2.扣减MySQL库存（乐观锁）
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                throw new RuntimeException("库存扣减失败，库存不足或已售罄，voucherId: " + voucherId);
            }
            log.info("订单创建成功，orderId: {}", orderId);
        } catch (Exception e) {
            log.error("处理订单异常，orderId: {}, voucherId: {}, userId: {}", orderId, voucherId, userId, e);
            throw e;
        }
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = SECKILL_ORDER_TOPIC, groupId = "seckill-order-group")
    public void handleSeckillOrder(String message, Acknowledgment acknowledgment) {
        // 解析消息：userId:voucherId:orderId
        String[] parts = message.split(":");
        Long userId = Long.parseLong(parts[0]);
        Long voucherId = Long.parseLong(parts[1]);
        Long orderId = Long.parseLong(parts[2]);

        log.info("收到秒杀订单消息，userId: {}, voucherId: {}, orderId: {}", userId, voucherId, orderId);
        try {
            // 创建订单
            createVoucherOrder(userId, voucherId, orderId);
            // 手动提交offset（确保任务完成后才提交）
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("处理订单异常，不提交offset，message: {}", message, e);
            // 不调用acknowledge，offset不会提交，消息会被重新消费
        }
    }

}