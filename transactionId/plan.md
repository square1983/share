# Spring Boot 事务ID幂等校验方案

## 1. 背景

移动端请求后台接口时，可能因为以下原因导致相同请求被重复发送：

- 用户重复点击
- 网络超时后的自动重试
- App 重发机制
- 网关重试
- MQ 重复消费

如果后台不做处理，可能导致：

- 重复扣款
- 重复下单
- 重复写入数据
- 数据状态异常

因此需要实现：

> 相同 transactionId 的请求只能成功处理一次。

---

# 2. 推荐方案

推荐使用：

- Spring Boot
- Spring AOP
- Redis
- transactionId

实现接口幂等控制。

核心原理：

```text
请求到达
    ↓
读取 transactionId
    ↓
Redis SETNX
    ↓
已存在 → 返回重复请求错误
不存在 → 正常执行业务
```

---

# 3. 技术方案对比

| 方案 | 是否推荐 | 特点 |
|---|---|---|
| Redis SETNX | 推荐 | 高性能，适合分布式 |
| 数据库唯一索引 | 推荐 | 强一致性 |
| Spring Cache | 一般 | 返回缓存结果，不适合报错 |
| Spring Integration | 特殊场景 | 偏消息系统 |
| synchronized | 不推荐 | 单机有效 |

---

# 4. Redis 幂等方案

## 4.1 Maven 依赖

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

---

# 5. 自定义注解

## Idempotent.java

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Idempotent {
}
```

---

# 6. AOP 实现

## IdempotentAspect.java

```java
@Aspect
@Component
public class IdempotentAspect {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Around("@annotation(Idempotent)")
    public Object around(ProceedingJoinPoint point) throws Throwable {

        HttpServletRequest request =
            ((ServletRequestAttributes)
                RequestContextHolder.getRequestAttributes())
                .getRequest();

        String transactionId =
            request.getHeader("transactionId");

        if (transactionId == null || transactionId.isBlank()) {
            throw new RuntimeException("transactionId不能为空");
        }

        String key = "tx:" + transactionId;

        Boolean success =
            redisTemplate.opsForValue()
                .setIfAbsent(
                    key,
                    "PROCESSING",
                    10,
                    TimeUnit.MINUTES
                );

        if (Boolean.FALSE.equals(success)) {
            throw new RuntimeException("重复事务请求");
        }

        try {

            Object result = point.proceed();

            redisTemplate.opsForValue().set(
                key,
                "SUCCESS",
                24,
                TimeUnit.HOURS
            );

            return result;

        } catch (Exception e) {

            // 失败后允许重试
            redisTemplate.delete(key);

            throw e;
        }
    }
}
```

---

# 7. Controller 使用方式

```java
@RestController
@RequestMapping("/pay")
public class PayController {

    @Idempotent
    @PostMapping("/submit")
    public String submit() {

        return "success";
    }
}
```

---

# 8. 请求示例

## HTTP Header

```http
transactionId: 202605210001
```

---

# 9. Redis Key 示例

```text
tx:202605210001
```

---

# 10. 并发安全说明

Redis 的：

```text
SETNX
```

是原子操作。

即使多个请求同时到达：

- 只有一个请求会成功
- 其它请求会立即失败

因此适合：

- 多实例部署
- Kubernetes
- ECS
- 微服务

---

# 11. 失败重试策略

当前方案：

```text
业务失败
    ↓
删除Redis Key
    ↓
允许重新提交
```

适合：

- 支付失败
- 网络异常
- 第三方调用失败

---

# 12. 可扩展优化

## 12.1 返回之前结果

可将响应结果缓存：

```text
transactionId
    ↓
Redis
    ↓
response
```

重复请求时直接返回之前结果。

---

## 12.2 数据库最终校验

推荐最终增加数据库唯一索引：

```sql
ALTER TABLE order_table
ADD CONSTRAINT uk_transaction_id
UNIQUE (transaction_id);
```

防止：

- Redis 故障
- 数据不一致
- 极端并发问题

---

# 13. 推荐生产架构

```text
手机App
    ↓
Spring Boot API
    ↓
Redis 幂等校验
    ↓
业务处理
    ↓
MySQL 唯一索引最终保护
```

---

# 14. 总结

推荐生产实现：

- Spring Boot
- Spring AOP
- Redis SETNX
- 数据库唯一索引

特点：

- 实现简单
- 性能高
- 支持分布式
- 并发安全
- 适合支付/订单系统
