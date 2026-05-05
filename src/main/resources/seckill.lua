-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.一人一单hash key: seckill:order:{voucherId}
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1.判断库存是否充足 get stockKey
if(tonumber(redis.call('get', stockKey)) <= 0) then
    -- 库存不足，返回1
    return 1
end

-- 3.2.判断用户是否下单（Hash结构）
if(redis.call('hexists', orderKey, userId) == 1) then
    -- 存在，说明是重复下单，返回2
    return 2
end

-- 3.3.扣库存
redis.call('incrby', stockKey, -1)

-- 3.4.保存一人一单（Hash结构，field为userId，value为orderId）
redis.call('hset', orderKey, userId, orderId)

return 0