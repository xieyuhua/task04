# Later

基于 Redis 或 RabbitMQ 的延迟队列服务，支持 ACK 确认机制与高并发优化。

## 使用方式

Go 版本：1.7+

```
go build
./later -h
参数说明：
  -address string
    	服务监听地址（默认 ":8080"）
  -backend string
    	存储后端：redis 或 rabbitmq（默认 "redis"）
  -redis string
    	Redis 地址（默认 "redis://127.0.0.1:6379/0"）
  -rabbitmq string
    	RabbitMQ 地址（默认 "amqp://guest:guest@127.0.0.1:5672/"）
```

**使用 Redis 后端（默认）：**

```
./later -backend redis -redis redis://127.0.0.1:6379/0
```

**使用 RabbitMQ 后端：**

```
./later -backend rabbitmq -rabbitmq amqp://guest:guest@127.0.0.1:5672/
```

## 特性

- 延迟投递消息到目标地址
- 至少一次投递保证
- **ACK 确认机制**：回调成功后需显式确认，超时未确认自动重入队列
- 失败自动重试，支持**固定间隔**和**指数退避**两种重试策略
- **高并发优化**：Worker 协程池、Redis Pipeline、sync.Map、批量拉取
- 双后端支持：Redis（有序集合轮询）或 RabbitMQ（死信交换推送）

## ACK 确认机制

传统模式下，回调成功后队列立即删除消息。但这存在问题：回调 HTTP 成功仅代表请求送达，不代表业务处理完成。如果业务方处理失败，消息已丢失无法重试。

**ACK 机制流程：**

1. 任务到期后，队列回调目标地址
2. 回调 HTTP 成功（code=100）→ 任务进入**等待确认**状态，设置 ACK 超时（默认 30s）
3. 业务方处理完成后，调用 `/ack` 接口显式确认 → 队列删除消息
4. 超时未确认 → 自动重入延迟队列，按重试策略重新投递
5. 回调 HTTP 失败 → 直接进入错误桶，按重试策略重新投递

**状态流转：**

```
创建 → 延迟桶 → [到期] → 回调 → HTTP成功 → 等待ACK(30s) → /ack确认 → 删除
                                                   ↓ 超时
                                              重入延迟桶 → 重新投递
         回调 → HTTP失败 → 错误桶 → [重试时间到] → 延迟桶 → 重新投递
```

## 存储后端对比

|| | Redis | RabbitMQ |
||---|---|---|
|| 延迟机制 | 有序集合 + Worker 轮询 | 死信交换（DLX）+ 消息级 TTL |
|| 任务存储 | Redis 键值对（GET/SET） | 进程内 sync.Map（可扩展为外部存储） |
|| 消费模式 | 拉取：Worker 定时 ZRANGEBYSCORE | 推送：Consumer 自动接收到期消息 |
|| ACK 跟踪 | 独立有序集合 `later_ack_deadline` | sync.Map 跟踪 ACK 截止时间 |
|| 持久化 | Redis AOF/RDB | RabbitMQ 持久化队列 + 持久消息 |
|| 水平扩展 | 多实例竞争 ZADD 去重 | 多 Consumer 共享队列 |
|| 适用场景 | 高吞吐、已有 Redis 基础设施 | 需要消息队列语义、已有 RabbitMQ 基础设施 |

## 前端 API

响应状态码：**200** 成功，**400** 请求无效，**404** 任务不存在，**409** 状态冲突，**500** 内部错误

### 创建任务

```
请求：
POST /create
{
	"topic":"order",
	"delay":15,               // 延迟秒数
	"retry":3,                // 最大重试次数
	"retry_strategy":"exponential", // "fixed"（固定间隔）或 "exponential"（指数退避），默认 "exponential"
	"retry_interval":10,      // 基础重试间隔（秒），默认 10
	"callback":"http://127.0.0.1:8888/",  // 回调地址
	"content":"hello"         // 投递内容
}
响应：
{
    "id": "35adbde5-77c4-4d65-adac-0082d91f2554"
}
```

**重试策略说明：**

| 策略 | retry_interval=10、retry=3 时的延迟序列 | 说明 |
|---|---|---|
| `fixed`（固定间隔） | 10s, 10s, 10s | 每次重试间隔固定不变 |
| `exponential`（指数退避） | 20s, 40s, 80s | 每次重试间隔翻倍（2^n × interval） |

### 确认任务（ACK）

回调成功后，业务方必须调用此接口确认任务。超时未确认的任务将被自动重新投递。

```
请求：
POST /ack
{
	"id":"35adbde5-77c4-4d65-adac-0082d91f2554"
}
响应：
200 OK  — 确认成功，任务已删除
404     — 任务不存在
409     — 任务状态不允许确认（已确认/已超时）
```

> 注意：只有 `ack_status=0`（等待确认）的任务才能被确认。重复确认或已超时重入队列的任务会返回 409。

### 删除任务

```
请求：
POST /delete
{
	"id":"35adbde5-77c4-4d65-adac-0082d91f2554"
}
```

### 查询任务

```
请求：
POST /query
{
	"id":"35adbde5-77c4-4d65-adac-0082d91f2554"
}
响应：
{
    "id": "cb9aefdd-5bd1-4bf3-8c94-1ed5c2ea638e",
    "topic": "order",
    "execute_time": 1504934230,
    "max_retry": 1,
    "has_retry": 0,
    "retry_strategy": 1,
    "retry_interval": 10,
    "callback": "http://127.0.0.1:8888/success",
    "content": "hello",
    "creat_time": 1504934220,
    "ack_status": 0,
    "ack_deadline": 1504934260
}
```

字段说明：
- `retry_strategy`：0=固定间隔，1=指数退避
- `ack_status`：0=等待确认，1=已确认成功，2=确认超时需重试
- `ack_deadline`：ACK 确认截止时间（Unix 时间戳）

## 回调 API

队列到达执行时间后，会向任务指定的 callback 地址发送 HTTP POST 请求：

```
请求：
POST /?
{
  "id": "57e177ff-454c-42d6-93ab-65895b950dbf",
  "topic": "order",
  "content": "hello"
}
响应：
{
    "code":100   // 100: 成功，101: 请求过多，其他: 失败
}
```

> 至少一次投递，可能重复投递。回调接口应保证幂等，且始终返回响应。

## 内部实现

### Backend 接口

所有存储操作通过 `Backend` 接口抽象，Redis 和 RabbitMQ 分别实现：

```go
type Backend interface {
    Init() error
    Close()
    CreateTask(task *Task) error
    GetTask(id string) (*Task, error)
    UpdateTask(task *Task) error
    DeleteTask(id string) error
    GetReadyIDs(bucket string, begin, end int64) ([]string, error)
    DelayToUnack(id string, score int64) (bool, error)
    UnackToDelay(id string, score int64) (bool, error)
    ErrorToDelay(id string, score int64) (bool, error)
    UnackToError(id string, score int64) error
    AckTask(id string) error
    GetAckTimeoutIDs(now int64) ([]string, error)
    SetAckDeadline(id string, deadline int64) error
}
```

### Redis 后端

**五个存储分区：**

- **任务池**：键值对，存储完整任务数据
- **延迟桶（Delay Bucket）**：有序集合，存储待执行的任务 ID 及执行时间
- **未确认桶（Unack Bucket）**：有序集合，存储已回调、等待 ACK 的任务
- **错误桶（Error Bucket）**：有序集合，存储回调失败的任务
- **ACK 截止桶（Ack Deadline Bucket）**：有序集合 `later_ack_deadline`，存储任务 ACK 截止时间，用于超时检测

**四个 Worker 定时拉取任务：**

- **延迟 Worker**：获取到期任务，从延迟桶移至未确认桶并触发回调
- **未确认 Worker**：将超时未确认的任务从未确认桶移回延迟桶
- **错误 Worker**：将到重试时间的任务从错误桶移回延迟桶
- **ACK 检查 Worker**：扫描 ACK 截止桶，将超时未确认的任务重入延迟队列

**并发安全：**

多实例部署时，多个 Worker 可能获取同一任务。通过从延迟桶移至未确认桶时判断 `ZADD` 返回值解决：返回 1 则该 Worker 继续处理，返回 0 说明已被其他实例抢占，直接跳过。

### RabbitMQ 后端

利用 RabbitMQ 的**死信交换（DLX）** + **消息级 TTL** 实现延迟投递：

1. 创建任务时，消息发送到延迟队列，设置 `Expiration`（TTL = 延迟秒数）
2. 消息在延迟队列中过期后，自动通过死信交换转投到未确认队列
3. 延迟 Worker 作为消费者从未确认队列消费，触发回调
4. 回调失败时，消息发送到错误队列（设置指数退避 TTL），过期后再次通过死信交换回到延迟队列

**优势：**

- 推送模式，无需轮询，延迟更精确
- 天然支持多消费者负载均衡
- 利用 RabbitMQ 原生的消息确认/重入队机制

## 高并发优化

### 1. Worker 协程池

传统方式每来一个任务就 `go callback(id)` 启动一个协程，当积压任务数万时会瞬间创建大量协程，导致内存溢出或打爆下游服务。

**优化方案：** 使用固定大小的 Worker 协程池 + 缓冲通道：

```go
taskChan = make(chan string, WorkerPoolSize*2)  // 缓冲通道
for i := 0; i < WorkerPoolSize; i++ {
    go worker()  // 固定 64 个 worker
}
```

- Worker 池大小默认 64，可通过 `WorkerPoolSize` 配置
- 缓冲通道满时丢弃新任务（下一轮再拉取），避免背压
- RabbitMQ 消费者在通道满时 `Nack(requeue=true)` 重新入队

### 2. Redis Pipeline

当前每个 Redis 命令都是一次独立网络往返。已将以下操作改为 Pipeline 合并：

| 操作 | 原始命令数 | Pipeline 后 |
|------|-----------|------------|
| CreateTask | 2 次（SET + ZADD） | 1 次往返 |
| DeleteTask | 4 次（DEL + 3×ZREM） | 1 次往返 |
| UnackToError | 3 次（ZADD + ZREM + ZREM） | 1 次往返 |
| AckTask | 5 次（DEL + 4×ZREM） | 1 次往返 |

### 3. sync.Map 替代 map+Mutex（RabbitMQ 后端）

RabbitMQ 后端的任务存储从 `map + sync.Mutex` 改为 `sync.Map`：

- 读多写少场景下性能更优，无需加锁
- `Range` 方法天然支持遍历，无需额外同步
- 消除了全局锁竞争瓶颈

### 4. 批量拉取优化

- `ZrangeCount` 从 20 提升到 100，减少轮询次数
- 可配置 `BatchSize` 控制每次拉取量
- Worker 池满时跳过本轮，避免阻塞调度器

### 5. Redis 层深度优化

**分片桶**

当数据量达到百万级，单 key 的 ZRANGEBYSCORE 会成为热点。可将桶按 topic 或任务 ID 哈希分片为多个 key（如 `later_delay_0` ~ `later_delay_15`），Worker 轮询所有分片，将单 key 压力分散。

**Lua 脚本保证原子性**

桶迁移中先 ZADD 再 ZREM 非原子操作，多实例并发时可能出现重复投递。用 Lua 脚本将 ZADD + ZREM 合并为一个原子操作：

```lua
local added = redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
if added == 1 then
    redis.call('ZREM', KEYS[1], ARGV[2])
    return 1
end
return 0
```

**连接池调优**

高并发下可适当增大 `MaxIdle` 和 `MaxActive`，并启用 `Wait=true` 避免连接池耗尽时直接报错。同时将读取超时适当放宽，防止大 key 操作超时。

### 6. 动态调度

Worker 用固定定时器（100ms / 1s），可在空闲时拉长间隔节省资源，积压时缩短间隔加速消费：

```go
interval := baseInterval
if len(ids) == 0 {
    interval = min(interval*2, maxInterval) // 空闲退避
} else {
    interval = baseInterval // 有任务立即恢复
}
```

### 7. 断路器

当下游服务持续失败时，引入断路器（如 `sony/gobreaker`），在失败率超过阈值时熔断，避免无效请求消耗资源，半开状态下探测恢复后自动闭合。

### 8. 架构级优化

**多实例水平扩展**

本服务本身无状态，所有状态在后端存储中，可直接水平扩展部署多实例。注意 Worker 数量增多会加剧桶竞争，需配合 Lua 脚本原子操作保证正确性。

**Redis 集群**

单机 Redis 内存和带宽有上限。迁移到 Redis Cluster，将不同分片桶分布到不同节点，突破单机瓶颈。

**消息队列解耦回调**

高并发下直接 HTTP 回调存在背压问题。可将回调动作改为写入消息队列（Kafka / RabbitMQ），由独立的消费服务消费并调用下游，实现削峰填谷和更好的故障隔离。

### 9. 监控与告警

| 监控指标 | 说明 |
|---------|------|
| 桶大小 | 各桶的 ZCARD 值，监控积压 |
| 回调延迟 | 回调响应时间分布，P99 是否超时 |
| 回调成功率 | 低于阈值告警 |
| ACK 超时率 | 超时未确认的任务占比，反映业务方处理能力 |
| 错误桶增速 | 反映下游服务健康度 |
| 连接池状态 | 等待数、命中数，判断池是否够用 |
| 协程数量 | 实例协程数，防止泄漏 |
