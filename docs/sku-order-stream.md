# 秒杀订单：Redis Stream 到 RocketMQ

## 请求与消息链路

`POST /sku/buy` 的参数和返回类型保持不变。返回订单 ID 表示请求已受理，不表示 MySQL 已完成创建；返回 `null` 表示明确拒绝（无库存、旧购买标记或待转发积压达到上限）。Redis 异常继续向上抛出，不能把超时当成确定失败并直接加回库存。

```text
SkuServiceImpl.buy
  └─ reserve-order.lua
       检查购买资格和积压 → 预扣库存 → 记录预占 → XADD
         └─ OrderStreamRelay
              消费组读取 / 接管 PEL → RocketMQ SEND_OK → XACK + XDEL
                └─ OrderConsumer
                     OrderPersistenceService 的 MySQL 事务
                       订单 + 条件扣库存 + Outbox + order_outcome
                     → complete-order.lua 确认或释放预占
                     → 标记 redis_applied → 正常返回，确认 MQ 消费
```

两段 ACK 分别表示消息已交给 RocketMQ、业务最终结果已处理。转发成功但 Stream ACK 失败会重复发送，消费者按业务订单 ID 幂等，不能按 RocketMQ 生成的消息 ID 去重。

## Redis 数据与部署范围

本实现沿用配置中的**单实例 Redis**和既有库存/购买集合键名，要求 Redis 6.2 或更新版本（使用排他范围分页扫描 PEL）。它没有使用能够直接运行于 Redis Cluster 的 key 布局；不能原样迁移到 Cluster。若需要分片，必须同时迁移库存、集合、预占及 Stream 的 key，并使同一脚本的全部 key 位于同一槽。

| Key | 含义 |
| --- | --- |
| `stole:<skuId>` | 沿用原有库存 key（保留原拼写）；非负整数字符串 |
| `set:<skuId>` | 沿用原有已购买/已预占用户集合 |
| `sku:order:buyer:<skuId>:<userId>` | 当前购买对应的订单 ID，重复请求返回此 ID |
| `sku:order:reservation:<orderId>` | `skuId`、`userId`、`state`；状态为 PENDING、CONFIRMED 或 CANCELLED |
| `sku:orders:stream` | 待转交 RocketMQ 的订单事件，字段为 orderId、skuId、userId |

Stream 仅供 `order-rocketmq-relay` 这一个逻辑消费组使用。多应用实例可以加入此组。消费组从 `0-0` 创建，确保启动前写入的事件也会发送；通过 `XPENDING` 分页与 `XCLAIM` 接管失联消费者和自身失败留下的消息。未确认消息不会用 `MAXLEN` 裁剪；仅在收到 MQ `SEND_OK` 后用 Lua 同时 ACK 和删除。

预占和订单 ID 均不设自动过期。取消成功后保留 CANCELLED 记录，以便迟到消息和重复补偿仍能被识别。上线需给这些数据做容量规划；后续归档必须结合 MQ 重放窗口和 MySQL 终态，不能单独给预占 hash 设置 TTL。

## MySQL 结果与补偿

`order_outcome` 是数据库的订单最终决定，以及 Redis 尚待完成任务的记录。它与订单和库存写入处于同一个事务：

- 成功：创建订单、扣一次库存、写一条原有 `Order` Outbox 事件，记录 CONFIRMED。
- 缺货或 SKU 不存在：撤销本次事务中的订单插入，记录 CANCELLED，随后仅归还这一笔 Redis 预占。
- 同一用户商品已有其他订单：取消新预占，归还一次库存，同时保留购买标记。
- 相同订单 ID 的重复消息：校验 SKU/用户一致后返回已提交的终态，不再次扣库存。
- 数据库故障、Outbox 冲突、序列化错误：回滚整个事务，异常交给 MQ 重试，不擅自取消订单。

消费者在数据库事务已提交后才调用 Redis 完成脚本。如果 Redis 故障，数据库结果不回滚，MQ 消费也不确认。`OrderOutcomeRecoveryJob` 还会按 `redis_applied=0` 独立重试已提交结果；因此 Redis 故障导致 MQ 重试耗尽后，已提交的结果仍可继续补偿。失败记录延后处理，避免占满每一批恢复任务。

取消终态不可重新变为成功：迟到 MQ 消息会读到数据库 CANCELLED，随后再次幂等释放。不能用“目前查不到订单”或预占时间过长直接释放库存。

**尚无数据库终态的消息**（例如 MySQL 长期不可用或损坏的消息）仍依靠 RocketMQ 重试和死信处理。运维需监控 `%DLQ%order_consumer_group`，修复原因后沿用原订单 ID 重放。恢复任务不把这些未知结果自动认定为取消。

## 上线步骤

1. 暂停旧版购买入口，使用旧版消费者处理完原 `order` 主题中的正常、重试及需恢复的死信消息，再切换新版。旧消息没有新预占 hash，新版会严格拒绝凭空完成/释放预占。
2. 应用启动时默认自动执行 `src/main/resources/db/order-outcome.sql`，使用 `CREATE TABLE IF NOT EXISTS` 创建结果表并保留已有数据，建表失败会阻止启动。应用数据库账号需要 `CREATE` 权限；若由运维预先执行该脚本，可设置 `MYSQL_SCHEMA_INIT_MODE=never` 关闭自动建表。另行审阅 `src/main/resources/db/order-unique-index.sql`：检查已有重复购买，必要时手动增加 `(sku_id, user_id)` 唯一索引；已有等价索引时跳过 `ALTER`。唯一索引脚本不会自动执行。需要 MySQL/InnoDB 本地事务。
3. 确认原有 `stole:` 库存已正确初始化、原 `set:` 集合保留。不要在有在途订单时用 MySQL 当前库存直接覆盖 Redis。
4. 为每个同时运行的实例分配不同 `SKU_WORKER_ID`（0–1023，默认 0 仅适合单实例）；重启时避免时钟回拨或在同一毫秒复用同一 worker 的序列。订单 ID 冲突会拒绝处理，不会吞成另一个用户的重复订单。
5. 配置 Redis 的持久化、复制与内存策略，禁止淘汰订单 Stream 和预占数据。Redis 接受写入并不等于所有故障下零数据丢失，AOF 与主从切换的恢复点目标需单独验证。MQ 的刷盘/复制、保留期和消费组也需按可靠性要求配置。
6. 启动新版，观察 Stream 长度、PEL 最长等待时间、MQ 积压/死信，以及 `order_outcome` 未完成数量。入口依然需要网关限流/故障熔断。

## 配置

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `MYSQL_SCHEMA_INIT_MODE` | always | 启动时执行幂等结果表建表脚本；预先手动建表后可设为 never |
| `SKU_WORKER_ID` | 0 | 每实例唯一的雪花 ID worker |
| `SKU_ORDER_STREAM_ENABLED` | true | 是否启动后台转发器；关闭后入口仍可预占并排队至积压上限 |
| `SKU_ORDER_STREAM_MAX_BACKLOG` | 100000 | Stream 尚未转交的事件上限，达到上限拒绝新预占 |
| `SKU_ORDER_STREAM_BATCH_SIZE` | 100 | 每次读取/扫描的记录数 |
| `SKU_ORDER_STREAM_POLL_MILLIS` | 1000 | 新消息阻塞读取时长 |
| `SKU_ORDER_STREAM_CLAIM_IDLE_MILLIS` | 60000 | 未确认消息接管门槛，必须大于发送超时 |
| `SKU_ORDER_STREAM_RETRY_MILLIS` | 2000 | 转发基础设施异常后的退避 |
| `SKU_ORDER_STREAM_SEND_TIMEOUT_MILLIS` | 3000 | MQ 同步发送超时 |
| `SKU_ORDER_OUTCOME_RECOVERY_BATCH_SIZE` | 100 | 每次扫描未完成 Redis 结果数量 |
| `SKU_ORDER_OUTCOME_RETRY_DELAY_SECONDS` | 30 | 单个 Redis 完成失败结果的延期 |
| `SKU_ORDER_OUTCOME_RECOVERY_INTERVAL_MS` | 5000 | 结果恢复任务执行间隔 |

MQ 故障会把积压留在 Redis，积压限制只约束未转交的事件数，不包含历史预占和 Broker 中在途消息的空间。

## 验证与限制

通常可执行 `mvn test`。新增测试覆盖 Lua 参数/类型校验、长整数 ID 精度、重复预占、积压拒绝、XADD 失败撤销、补偿一次性、迟到补偿不影响新预占、Stream 接管与 ACK 顺序、消费者异常传播、恢复任务，以及数据库事务回滚和并发库存竞争。

Lua 测试在 LuaJ 中执行真实资源脚本，Redis 命令由故障注入模型模拟；事务测试在 H2 的 MySQL 模式中使用真实 Spring 事务代理。这些测试不能替代原生 Redis/MySQL/RocketMQ 的故障联调，尤其是断电恢复、主从切换和刷盘确认。脚本对可捕获的命令错误做撤销，但 Redis 崩溃、持久化丢失或撤销自身不可执行，仍需恢复与对账处理。
