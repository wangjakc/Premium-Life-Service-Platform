# 优选生活综合服务平台

基于 Spring Boot + MyBatis-Plus + Redis 的本地生活服务电商平台，仿大众点评/美团功能。

## 技术栈

| 分类 | 技术 |
|------|------|
| 框架 | Spring Boot 2.3.12、MyBatis-Plus 3.4.3 |
| 数据库 | MySQL 5.1.47 |
| 缓存 | Redis + Lettuce + Caffeine（多级缓存） |
| 消息队列 | Apache Kafka（秒杀订单异步处理） |
| 分布式 | Redisson（分布式锁、延迟队列） |
| 熔断降级 | Resilience4j |
| 工具库 | Hutool、Guava、Lombok |

---

## 功能模块

### 1. 用户模块（User）

- **手机验证码登录**：验证码存入 Redis（key: `login:code:{phone}`），验证通过后生成 UUID token 存入 Redis
- **签到功能**：基于 Redis BitMap 实现月度签到统计（key: `user:sign:{userId}:{yyyyMM}`）
- **Token 刷新**：登录拦截器自动刷新 Token 有效期

### 2. 商铺模块（Shop）

- **商铺 CRUD**：增删改查商铺信息
- **商铺分类查询**：按类型分页查询，支持按距离排序
- **附近商铺**：基于 Redis GEO 实现 `GEOSEARCH` 附近 5km 商铺查询
- **多级缓存策略**：
  - 缓存穿透：存储空值防止穿透
  - 缓存击穿：逻辑过期 + 互斥锁双重方案
  - 缓存雪崩：随机 TTL

### 3. 探店笔记模块（Blog）

- **发布笔记**：发布探店内容，关联商铺和用户
- **笔记点赞**：基于 ZSet 实现，score 为时间戳支持排序（key: `blog:liked:{blogId}`）
- **粉丝推送**：基于 ZSet 实现滚动分页（key: `feed:{userId}`），发布笔记时推送给所有粉丝
- **热点博客查询**：按点赞数降序查询

### 4. 关注模块（Follow）

- **关注/取关用户**：同时更新 MySQL 和 Redis Set
- **共同关注查询**：基于 Redis Set 交集 `SINTER` 实现
- **关注状态判断**

### 5. 优惠券模块（Voucher）

- **普通优惠券**：直接发放给用户
- **秒杀优惠券**：创建秒杀活动（设置库存、时间），关联特定商铺

### 6. 秒杀模块（Seckill）【核心亮点】

- **Redis Lua 脚本**：保证库存检查、库存扣减、订单记录操作的原子性
- **一人一单限制**：基于 Redis Hash 结构防止重复下单
- **异步订单处理**：通过 Kafka 消息队列异步创建订单、扣减 MySQL 库存
- **订单超时处理**：基于 Redisson 延迟队列实现 15 分钟超时自动释放库存
- **雪花算法**：生成分布式唯一订单 ID
- **熔断器降级**：Resilience4j 配置 Redis 故障时降级到本地限流

### 7. 限流模块（Rate Limiter）

- **AOP 限流**：基于注解 `@LimtRate` 拦截
- **Redis 滑动窗口限流**：Lua 脚本实现精确限流
- **本地限流降级**：Redis 不可用时自动切换到 Caffeine 本地限流
- **熔断器**：Resilience4j 统计失败率，自动切换降级策略

### 8. 商铺类型模块（ShopType）

- 查询所有商铺类型（按 sort 排序）

---

## 核心业务流程

### 用户登录流程
```
手机号 → 发送验证码到 Redis → 用户提交验证码 → 验证通过
→ 查询/创建用户 → 生成 Token 存入 Redis → 返回 Token
```

### 秒杀订单流程
```
用户发起秒杀 → Lua 脚本检查库存和一人一单 → 扣减 Redis 库存
→ 生成订单 ID → 发送 Kafka 消息 → 返回订单 ID → 异步创建订单
```

### 探店笔记发布流程
```
发布笔记 → 保存 MySQL → 查询作者所有粉丝 → 推送笔记到粉丝 feed
→ 粉丝在关注页滚动分页查看
```


## 项目配置

启动前需配置 `application.yaml`：

```yaml
spring:
  datasource:
    url: 
    username:
    password: 
  redis:
    host: 
    port: 
    password: 
  kafka:
    bootstrap-servers: 
```

---

## 技术亮点

1. **多级缓存**：Caffeine（本地） + Redis（分布式），解决缓存击穿、穿透、雪崩
2. **秒杀系统**：Lua 脚本保证原子性 + Kafka 异步解耦 + 延迟队列处理超时
3. **限流降级**：Redis 滑动窗口限流 + 本地限流降级 + Resilience4j 熔断器
4. **Feed 流**：ZSet 实现粉丝推送和滚动分页
5. **附近查询**：Redis GEO 实现高效地理位置查询