# Java 后台事务ID幂等方案汇总

## 1. 背景

移动端请求后台接口时，可能由于以下原因导致重复请求：

- 用户连续点击
- 网络超时重试
- App 自动重发
- Gateway 重试
- MQ 重复消费

如果后台不进行幂等控制，可能会导致：

- 重复扣款
- 重复创建订单
- 数据状态异常
- 库存错误
- 重复发送消息

因此需要：

> 相同 transactionId 只能成功处理一次。

---

# 2. 常见幂等方案总览

| 方案 | 推荐度 | 适合场景 | 特点 |
|---|---|---|---|
| Redis SETNX | ★★★★★ | Web API / 微服务 | 性能高 |
| 数据库唯一索引 | ★★★★★ | 支付/订单 | 最可靠 |
| Token机制 | ★★★★ | 防重复提交 | 前端友好 |
| 状态机控制 | ★★★★ | 复杂订单流转 | 强业务控制 |
| 分布式锁 | ★★★ | 高并发资源竞争 | 较重 |
| MQ 幂等消费 | ★★★★★ | Kafka/RabbitMQ | 消息系统 |
| Spring Cache | ★★ | 查询缓存 | 不适合严格幂等 |
| 乐观锁 | ★★★ | 数据竞争更新 | DB方案 |
| 去重表 | ★★★★ | 审计场景 | 易追踪 |
| 网关层防重 | ★★★ | API Gateway | 系统统一治理 |

---

# 3. Redis SETNX 方案（推荐）

## 原理

```text
请求到达
    ↓
读取 transactionId
    ↓
Redis SETNX
    ↓
已存在 → 返回错误
不存在 → 执行业务
```

---

## Java 示例

```java
String key = "tx:" + transactionId;

Boolean success = redisTemplate.opsForValue()
    .setIfAbsent(key, "1", 10, TimeUnit.MINUTES);

if (Boolean.FALSE.equals(success)) {
    throw new RuntimeException("重复事务请求");
}
```

---

## 优点

- 高性能
- 支持分布式
- 并发安全
- 实现简单

---

## 缺点

- 依赖 Redis
- Redis 故障时需额外保护

---

# 4. 数据库唯一索引方案

## 表结构

```sql
CREATE TABLE payment (
    id BIGINT PRIMARY KEY,
    transaction_id VARCHAR(64) UNIQUE
);
```

---

## Java 示例

```java
try {
    paymentMapper.insert(order);
} catch (DuplicateKeyException e) {
    throw new RuntimeException("重复事务");
}
```

---

## 优点

- 强一致性
- 数据可靠
- 金融系统常用

---

## 缺点

- DB压力较大
- 高并发性能不如Redis

---

# 5. Token 防重复提交方案

## 流程

```text
页面获取 token
    ↓
提交时携带 token
    ↓
后台消费 token
    ↓
已消费 → 拒绝
```

---

## Java 示例

```java
Boolean success = redisTemplate.delete("submit:" + token);

if (!success) {
    throw new RuntimeException("重复提交");
}
```

---

## 适合场景

- 防按钮连点
- Web表单提交
- 电商订单页面

---

# 6. 状态机方案

## 订单状态

```text
INIT
PROCESSING
SUCCESS
FAIL
```

---

## 更新控制

```sql
update order_table
set status='PROCESSING'
where id=?
and status='INIT';
```

---

## 特点

只有一个请求可以更新成功。

---

## 优点

- 与业务状态融合
- 容易追踪
- 不依赖 Redis

---

## 缺点

- 代码复杂
- 需要完整状态设计

---

# 7. 乐观锁方案

## 表结构

```sql
version INT
```

---

## 更新语句

```sql
update order_table
set version = version + 1
where id = ?
and version = 1;
```

---

## MyBatis Plus 示例

```java
@Version
private Integer version;
```

---

# 8. 分布式锁方案

## Redisson 示例

```java
RLock lock = redisson.getLock("tx:" + txId);

if(lock.tryLock()) {
    try {
        doBusiness();
    } finally {
        lock.unlock();
    }
}
```

---

## 优点

- 强并发控制
- 支持资源锁定

---

## 缺点

- 实现复杂
- 存在死锁风险
- 性能低于SETNX

---

# 9. MQ 幂等消费方案

## 流程

```text
messageId
    ↓
Redis / DB
    ↓
是否已消费？
```

---

## 常见MQ

- Kafka
- RabbitMQ
- RocketMQ

---

## 适合场景

- 消息重复消费控制
- 异步系统
- 事件驱动架构

---

# 10. 去重表方案

## 表结构

```sql
CREATE TABLE idempotent_log (
    transaction_id VARCHAR(64) PRIMARY KEY,
    created_at TIMESTAMP
);
```

---

## 特点

适合：

- 审计
- 对账
- 长期追踪
- 问题排查

---

# 11. 网关层防重方案

## 原理

```text
API Gateway
    ↓
transactionId
    ↓
Redis
    ↓
拒绝重复请求
```

---

## 常见网关

- Spring Cloud Gateway
- Kong
- APISIX

---

## 优点

- 统一治理
- 业务系统无感知

---

## 缺点

- 网关逻辑复杂
- 业务状态难处理

---

# 12. Spring 官方相关方案

## Spring Cache

特点：

- 适合缓存
- 不适合严格幂等

---

## Spring Retry

特点：

- 适合失败自动重试
- 不是真正幂等

---

## Spring Integration

特点：

- 官方 Idempotent Receiver
- 更适合 MQ / Integration 场景

---

# 13. 推荐生产方案

## 普通互联网系统

```text
Spring AOP
    +
Redis SETNX
    +
MySQL UNIQUE
```

---

## 金融支付系统

```text
状态机
    +
唯一索引
    +
MQ 幂等
```

---

## 电商系统

```text
Token
    +
Redis
```

---

# 14. 最终推荐

对于：

```text
手机App
    ↓
Java Spring Boot API
    ↓
transactionId
```

推荐：

```text
Spring AOP
    +
Redis SETNX
    +
数据库唯一索引
```

原因：

- 实现简单
- 性能高
- 支持分布式
- 并发安全
- 生产成熟
- 适合支付/订单系统
