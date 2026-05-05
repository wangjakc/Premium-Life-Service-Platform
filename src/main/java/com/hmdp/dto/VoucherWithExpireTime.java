package com.hmdp.dto;

import com.hmdp.entity.Voucher;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 优惠券列表包装类(包含逻辑过期时间)
 * 用于实现逻辑过期而不是Redis TTL过期,解决缓存击穿问题
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VoucherWithExpireTime {

    /**
     * 优惠券列表
     */
    private List<Voucher> vouchers;

    /**
     * 逻辑过期时间戳(毫秒)
     * 当系统时间超过这个时间时,认为数据已过期
     * 区别于Redis的TTL,这是应用层的逻辑过期判定
     */
    private Long expireTime;
}