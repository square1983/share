# transactionId 幂等校验 — 最终方案

## 1. 客户需求确认

| 需求项 | 确认内容 |
|---|---|
| Redis 基础设施 | 已有，集群模式 |
| 重复请求处理 | 报错拒绝 |
| 失败后重试 | 不允许 |
| TTL | 待最终确认（本方案默认 7 天） |
| transactionId 来源 | 客户端生成，已实现 |

---

## 2. 选型结论

**推荐方案：Redis Cluster SETNX + Spring AOP + 数据库唯一索引**

| 组件 | 作用 |
|---|---|
| Redis Cluster SETNX | 主校验层，原子防重，高性能 |
| Spring AOP 注解 | 业务无侵入，统一切面拦截 |
| 数据库唯一索引 | 兜底层，Redis 故障时防止数据异常 |

---

## 3. transactionId 状态机

```
请求到达
    ↓
Key 是否存在于 Redis？
    ↙ 是                  ↘ 否
读取状态                  SETNX → PROCESSING（30 分钟）
  ↓                              ↓
任意状态 → 拒绝            执行业务逻辑
（PROCESSING/SUCCESS/FAILED）   ↙          ↘
                           成功           失败
                            ↓              ↓
                      → SUCCESS        → FAILED
                       （7 天）         （7 天）
```

**关键规则：SUCCESS 和 FAILED 均不可重入，失败后永久封闭该 transactionId。**

---

## 4. Redis Key 设计

| 属性 | 值 |
|---|---|
| Key 格式 | `tx:{transactionId}` |
| PROCESSING TTL | 30 分钟 |
| SUCCESS TTL | 7 天 |
| FAILED TTL | 7 天 |

> 如有合规/审计要求，SUCCESS 和 FAILED 的 TTL 可延长至 30 天。

---

## 5. 请求处理流程

### 5.1 正常请求

```
App
 ↓ Header: transactionId
AOP 拦截
 ↓ SETNX("tx:{id}", PROCESSING, 30min) → 写入成功
业务逻辑执行
 ↓ 执行成功
Redis 写入 SUCCESS（TTL 7天）
 ↓
返回正常响应
```

### 5.2 重复请求

```
App
 ↓ 携带相同 transactionId
AOP 拦截
 ↓ Redis Key 已存在（任意状态）
立即拒绝，不执行业务
 ↓
返回错误：{"code": "DUPLICATE_REQUEST"}
```

### 5.3 业务失败

```
App
 ↓ 携带 transactionId
AOP 拦截
 ↓ SETNX 成功
业务逻辑执行
 ↓ 执行失败（异常）
Redis 写入 FAILED（TTL 7天）
 ↓
返回业务错误
※ 此 transactionId 永久封闭，无法重试
```

---

## 6. 错误码规范

| 场景 | HTTP 状态码 | 错误码 | 错误信息 |
|---|---|---|---|
| transactionId 缺失 | 400 | MISSING_TRANSACTION_ID | transactionId 不能为空 |
| transactionId 格式非法 | 400 | INVALID_TRANSACTION_ID | transactionId 格式不合法 |
| 处理中重复请求 | 409 | REQUEST_IN_PROGRESS | 请求处理中，请勿重复提交 |
| 已成功的重复请求 | 409 | DUPLICATE_REQUEST | 该事务已处理完成 |
| 已失败的重复请求 | 409 | DUPLICATE_REQUEST | 该事务已处理完成 |

---

## 7. 安全校验

| 校验项 | 规则 |
|---|---|
| transactionId 长度 | 1 ～ 64 字符 |
| 字符白名单 | 字母、数字、连字符、下划线 |
| 空值处理 | 直接返回 400 |

---

## 8. Redis Cluster 兼容性

- SETNX（setIfAbsent）为单 Key 原子操作，完全兼容 Redis Cluster
- 每个 transactionId 对应独立 Key，不涉及跨 Slot 操作
- 无需额外改造

---

## 9. 数据库兜底

在支付/订单表中增加：

```
字段：transaction_id VARCHAR(64)
约束：UNIQUE INDEX
```

作用：Redis 故障或极端并发场景下，数据库层拦截重复写入，保障数据最终一致性。

---

## 10. 生产架构

```
手机 App
    ↓ Header: transactionId
Spring Boot API
    ↓
AOP 幂等拦截（@Idempotent 注解）
    ↓
Redis Cluster（SETNX 原子校验）
    ↓
业务逻辑层
    ↓
MySQL（UNIQUE 索引兜底）
```

---

## 11. 待确认事项

| 编号 | 问题 | 影响范围 | 默认值 |
|---|---|---|---|
| Q1 | SUCCESS / FAILED 的 TTL 是否满足合规或审计要求？ | Redis Key 保留时长 | 7 天 |
| Q2 | transactionId 的字符格式规范？（UUID / 纯数字 / 其他） | 服务端格式校验规则 | 长度 ≤ 64，字母数字 |
