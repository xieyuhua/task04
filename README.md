# Later

基于 Redis 或 RabbitMQ 的延迟任务队列服务，支持 ACK 确认、按天日志、命令行参数调优。

**Go 版本要求：** Go 1.20+（使用 `unsafe.String` / `unsafe.Slice` 替代废弃的 `reflect.SliceHeader`）

## 快速开始

```bash
go build -o later.exe .
./later -h
```

### 命令行参数

**核心参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-address` | `:2345` | 服务监听地址 |
| `-backend` | `redis` | 存储后端：`redis` 或 `rabbitmq` |
| `-redis` | `redis://:147258369@127.0.0.1:6379/3` | Redis 连接地址 |
| `-rabbitmq` | `amqp://guest:guest@127.0.0.1:5672/` | RabbitMQ 连接地址 |

**日志参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-logdir` | `""`（空） | 日志文件目录，为空仅输出控制台 |
| `-logdays` | `7` | 日志文件保留天数，自动清理 |

**调优参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-pool` | `64` | 并发 worker 协程池大小 |
| `-batch` | `50` | 每轮轮询拉取任务数上限 |
| `-ctt` | `3` | 回调 HTTP 超时（秒） |
| `-acktimeout` | `30` | ACK 确认超时（秒），超时未确认重入队列 |
| `-maxretrycap` | `86400` | 指数退避重试延迟上限（秒），默认 86400（24 小时） |

**示例：**

```bash
# 默认配置（Redis 后端，仅控制台日志）
./later.exe

# 指定 Redis + 文件日志
./later.exe -redis redis://:pass@10.0.0.1:6379/3 -logdir ./logs -logdays 30

# 高吞吐调优
./later.exe -pool 128 -batch 200 -ctt 1 -acktimeout 15

# 自定义重试上限（12 小时）
./later.exe -maxretrycap 43200

# RabbitMQ 后端
./later.exe -backend rabbitmq -rabbitmq amqp://guest:guest@10.0.0.2:5672/
```

## 特性

- 延迟投递消息到目标地址
- 至少一次投递保证
- **智能 ACK 确认机制**：回调成功（code=100/200/0）自动确认；返回其他 code 则进入重试
- 失败自动重试，支持**固定间隔**和**指数退避**两种重试策略
- **可配置重试上限**：指数退避延迟上限 `MaxRetryDelayCap` 默认 24h，通过 `-maxretrycap` 命令行调整
- **积压保护**：Worker 轮询不设时间下界，消费不及时的任务不会因"等太久"被丢弃
- **高并发优化**：Worker 协程池、Redis Pipeline、sync.Map、批量拉取
- 双后端支持：Redis（有序集合轮询）或 RabbitMQ（死信交换推送）
- **按天日志**：日志文件按日期分割（`later-2026-06-16.log`），文件输出 JSON 格式，控制台保持 Text 可读
- **Lua 原子化桶操作**：桶间迁移由 Lua 脚本保证原子性，消除多实例竞态
- **管道响应验证**：Redis Pipeline 操作均接收服务端响应，Redis OOM/类型错误等不再被静默吞掉
- **优雅关闭增强**：通过 `workerStopCh` + `sync.Once` 精准控制所有后台协程退出，`defer ticker.Stop()` 防止定时器泄漏
- **HTTP 超时保护**：配置 ReadTimeout/WriteTimeout/IdleTimeout，防止慢客户端 goroutine 泄漏
- **ACK 竞态消除**：Lua 脚本将 ACK deadline 注册纳入原子操作，进程崩溃不再丢任务
- **Go 1.20+ 兼容**：`hack.go` 零拷贝字符串转换使用 `unsafe.String`/`unsafe.Slice`，移除废弃的 `reflect.SliceHeader`
- **健康检查 & 监控**：`/health` 检查连通性，`/metrics` 返回各桶积压数量
- **参数校验**：创建任务时校验 delay 范围、retry 上限、content 大小等
- **桶泄漏清理**：定时清理过期桶数据（超过 TaskTTL 的僵尸条目），防止桶无限膨胀
- **回调安全**：响应体限制 64KB 防 OOM；请求体限制 1MB

## 日志系统

日志同时输出到两个目标：

| 目标 | 格式 | 说明 |
|------|------|------|
| 控制台（stdout） | Text | 人类可读，带完整时间戳 |
| 日志文件 | JSON | 结构化日志，按天分割，一行一条 |

**文件命名**：`later-YYYY-MM-DD.log`（如 `later-2026-06-16.log`），跨天自动创建新文件。

**JSON 日志示例**：

```json
{"level":"info","msg":"server listen on :2345 [backend=redis]","time":"2026-06-16 11:08:05"}
{"level":"info","msg":"auto ack success, code=100","time":"2026-06-16 11:08:06","id":"abc123"}
{"level":"error","msg":"http post fail","time":"2026-06-16 11:08:07","error":"connection refused"}
```

**保留策略**：启动时自动清理超过 `-logdays` 天的旧文件，默认 7 天。

## ACK 确认机制

**两种确认模式：**

### 1. 自动 ACK（推荐）
回调 HTTP 返回 `code = 100`、`200` 或 `0` → 队列**自动确认并删除任务**，无需业务方额外调用 `/ack`。

### 2. 手动 ACK（兼容保留）
回调 HTTP 返回其他 code（如 `101` "请求过多"）→ 任务进入**等待确认**状态，设置 ACK 超时（默认 30s）：
- 业务方调用 `/ack` 显式确认 → 队列删除任务
- 超时未确认 → 自动重入延迟队列，按重试策略重新投递

> 注意：原版设计中 code=100 仅进入等待确认需要手动 ACK，现版本对 100/200/0 统一改为自动确认。如需保留手动确认语义，请返回其他成功码。

**状态流转：**

```
创建 → 延迟桶 → [到期] → 回调 → code=100/200/0 → 自动ACK → 删除
                                 code=其他 → 等待ACK(30s) → /ack确认 → 删除
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
|| ACK 跟踪 | 独立有序集合 `later:bucket:ack_dl` | sync.Map 跟踪 ACK 截止时间 |
|| 持久化 | Redis AOF/RDB | RabbitMQ 持久化队列 + 持久消息 |
|| 水平扩展 | 多实例竞争 ZADD 去重 | 多 Consumer 共享队列 |
|| 适用场景 | 高吞吐、已有 Redis 基础设施 | 需要消息队列语义、已有 RabbitMQ 基础设施 |

## 前端 API

响应状态码：**200** 成功，**400** 请求无效，**404** 任务不存在，**409** 状态冲突，**500** 内部错误

### 健康检查

```
GET /health
响应 200：
{"status":"ok","shutting_down":"false"}
响应 503：
{"status":"unhealthy","reason":"connection refused"}
```

### 监控指标

```
GET /metrics
响应 200：
{"delay":156,"unack":3,"error":0,"ack_deadline":3}
```

| 字段 | 说明 |
|------|------|
| `delay` | 延迟桶中等待执行的任务数 |
| `unack` | 等待手动 ACK 的任务数 |
| `error` | 回调失败等待重试的任务数 |
| `ack_deadline` | ACK 截止跟踪中的任务数 |

### 创建任务

```
请求：
POST /create
{
	"topic":"order",
	"delay":15,               // 延迟秒数，0~2592000（30天）
	"retry":3,                // 最大重试次数，0~100
	"retry_strategy":"exponential", // "fixed"（固定间隔）或 "exponential"（指数退避），默认 "exponential"
	"retry_interval":10,      // 基础重试间隔（秒），默认 10
	"callback":"http://127.0.0.1:8888/",  // 回调地址（必填）
	"content":"hello"         // 投递内容，最大 1MB
}
响应：
{
    "id": "35adbde5-77c4-4d65-adac-0082d91f2554"
}

请求无效时返回 400，附带错误说明（如 "callback URL is required"、"delay too large" 等）。
```

**重试策略说明：**

| 策略 | retry_interval=10、retry=3 时的延迟序列 | 说明 |
|---|---|---|
| `fixed`（固定间隔） | 10s, 10s, 10s | 每次重试间隔固定不变 |
| `exponential`（指数退避） | 20s, 40s, 80s | 每次重试间隔翻倍（2^n × interval），上限 24 小时 |

> 指数退避上限为 `MaxRetryDelayCap`（默认 86400 秒 = 24 小时），可通过 `-maxretrycap` 命令行参数调整。

### 确认任务（ACK）

当回调返回非 100/200/0 的 code 时，任务进入等待确认状态，业务方需调用此接口手动确认。超时未确认的任务将被自动重新投递。

> 回调返回 code=100/200/0 时会自动 ACK，无需调用此接口。详见上方"回调 API"章节。

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
    "code":100   // 100/200/0: 成功（自动ACK），101: 请求过多（需手动ACK），其他: 失败（重试）
}
```

**code 语义说明：**

| code | 行为 | 说明 |
|------|------|------|
| 100 | 自动 ACK | 回调成功，队列自动确认并删除任务 |
| 200 | 自动 ACK | 同上，兼容标准 HTTP 成功码 |
| 0 | 自动 ACK | 同上，兼容常见 API 返回值 |
| 其他 | 等待手动 ACK | 任务进入 unack 桶，30s 内需调 `/ack`，超时则重试 |

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
    DelayToUnackWithData(id string, score int64, ackDeadline int64) (*Task, error)  // Redis 专用：Lua 原子获取+迁移+ACK deadline
}
```

### Redis 后端

**Redis Key 命名规范：`later:{category}:{name}`**

采用统一命名空间前缀，避免 Key 散落污染 Redis，使用 `:` 分隔符支持 Redis CLI 工具树形分组：

| Key 模式 | 类型 | 说明 |
|----------|------|------|
| `later:task:{id}` | String | 任务完整数据 JSON |
| `later:bucket:delay` | ZSet | 延迟桶，score=执行时间 |
| `later:bucket:unack` | ZSet | 未确认桶，score=入桶时间 |
| `later:bucket:error` | ZSet | 错误桶，score=重试时间 |
| `later:bucket:ack_dl` | ZSet | ACK 截止桶，score=ACK 截止时间戳 |

> 所有 key 集中在 `later:` 命名空间下，可通过 `KEYS later:*` 或 `SCAN later:*` 快速定位，`FLUSHDB` 时也便于区分。

**五个存储分区：**

- **任务池**：String 键值对，key=`later:task:{id}`，存储完整任务 JSON
- **延迟桶（Delay Bucket）**：ZSet `later:bucket:delay`，存储待执行任务，score=执行时间
- **未确认桶（Unack Bucket）**：ZSet `later:bucket:unack`，存储等待手动 ACK 的任务
- **错误桶（Error Bucket）**：ZSet `later:bucket:error`，存储回调失败等待重试的任务
- **ACK 截止桶（Ack Deadline Bucket）**：ZSet `later:bucket:ack_dl`，score=ACK 截止时间戳，ackCheckWorker 轮询此桶检测超时

**四个 Worker 定时拉取任务：**

- **延迟 Worker**（100ms 间隔）：获取到期任务，从延迟桶移至未确认桶并触发回调
- **未确认 Worker**（1s 间隔）：将超时未确认的任务从未确认桶移回延迟桶
- **错误 Worker**（1s 间隔）：将到重试时间的任务从错误桶移回延迟桶
- **ACK 检查 Worker**（500ms 间隔）：扫描 ACK 截止桶，将超时未确认的任务重入延迟队列

> **积压保护**：三个 Worker（delay / unack / error）的轮询查询下限均为 `-inf`（即不设时间下界），确保积压再久的任务都不会被跳过。即使系统消费不及时，任务数据 key 也仍在 TaskTTL（24h）内有效，会被正常取出执行。超出 TaskTTL 的僵尸条目（数据 key 已过期）由 `CleanExpiredBuckets` 每 10 分钟定期清理，同时 `callback` 中遇到 `ErrTaskNotFound` 也会主动清理引用。

**并发安全（已通过 Lua 脚本解决）：**

多实例部署时，桶间迁移全部由 Lua 脚本原子执行，`ZADD` 返回 1 表示抢占成功并自动 `ZREM` 原桶，返回 0 表示已被其他实例抢占，直接跳过。

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

- Worker 池大小默认 64，通过 `-pool` 参数配置
- 缓冲通道满时丢弃新任务（下一轮再拉取），避免背压
- RabbitMQ 消费者在通道满时 `Nack(requeue=true)` 重新入队

### 2. Redis Pipeline

当前每个 Redis 命令都是一次独立网络往返。已将以下操作改为 Pipeline 合并，且每条 Pipeline 发送后均接收服务端响应，确保 Redis OOM、类型错误等不被静默忽略：

| 操作 | 原始命令数 | Pipeline 后 | 响应处理 |
|------|-----------|------------|----------|
| CreateTask | 2 次（SET + ZADD） | 1 次往返 | 接收 2 条响应 |
| DeleteTask | 5 次（DEL + 4×ZREM） | 1 次往返 | 接收 5 条响应 |
| UnackToError | 3 次（ZADD + ZREM + ZREM） | 1 次往返 | 接收 3 条响应 |
| AckTask | 5 次（DEL + 4×ZREM） | 1 次往返 | 接收 5 条响应 |

### 3. sync.Map 替代 map+Mutex（RabbitMQ 后端）

RabbitMQ 后端的任务存储从 `map + sync.Mutex` 改为 `sync.Map`：

- 读多写少场景下性能更优，无需加锁
- `Range` 方法天然支持遍历，无需额外同步
- 消除了全局锁竞争瓶颈

### 4. 批量拉取优化

- `ZrangeCount` 批量拉取上限，通过 `-batch` 参数配置
- Worker 池满时跳过本轮，避免阻塞调度器

### 5. HTTP Server 超时保护

服务端配置了读写超时，防止慢客户端导致 goroutine 泄漏：

| 超时类型 | 值 | 说明 |
|---------|-----|------|
| `ReadTimeout` | 10s | 读取请求体最大等待时间 |
| `WriteTimeout` | 30s | 写入响应最大等待时间 |
| `IdleTimeout` | 120s | Keep-Alive 空闲连接最大保持时间 |

### 6. 优雅关闭增强

关闭流程通过 `workerStopCh` 精准控制所有后台协程退出：

```
SIGINT/SIGTERM → SetShuttingDown() → close(workerStopCh)
    ├── 4 个后台 Worker 收到信号，停止 ticker 并 return
    ├── close(taskChan) → Worker 池消费完缓冲后自动退出
    └── 等待排空 → 关闭后端连接 → 退出进程
```

- **Ticker 生命周期管理：** 所有 `time.NewTicker` 均使用 `defer ticker.Stop()`，协程退出时自动释放定时器资源
- **线程安全关闭：** `workerStopCh` 通过 `sync.Once` 确保只关闭一次，避免 panic
- 等待时间 = `max(callbackTTR × 2, 10s)`，确保进行中的回调有足够时间完成

### 7. 底层内存优化（hack.go）

字符串与 `[]byte` 互转从废弃的 `reflect.SliceHeader` 升级为 Go 1.20+ 标准库 API：

```go
// 旧（Go 1.20 起废弃）
pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))

// 新（Go 1.20+，零拷贝 + 类型安全）
s := unsafe.String(unsafe.SliceData(b), len(b))
b := unsafe.Slice(unsafe.StringData(s), len(s))
```

### 8. Redis 层深度优化

**分片桶（规划中）**

当数据量达到百万级，单 key 的 ZRANGEBYSCORE 会成为热点。可将桶按 topic 或任务 ID 哈希分片为多个 key（如 `later:bucket:delay:0` ~ `later:bucket:delay:15`），Worker 轮询所有分片，将单 key 压力分散。

**Lua 脚本保证原子性（已实现）**

桶迁移操作已全部由 Lua 脚本实现，ZADD + ZREM 合并为一个原子操作，消除多实例并发时的重复投递风险：

- `bucketTransfer`：ZADD to + ZREM from
- `bucketTransferWithAckCleanup`：ZADD to + ZREM from + ZREM ack_dl
- `unackToError`：ZADD error + ZREM unack + ZREM ack_dl
- `getTaskAndDelayToUnack`：GET task + ZADD unack + ZREM delay + ZADD ack_deadline（**一次网络往返完成四项操作**）

> **ACK 竞态窗口消除：** `getTaskAndDelayToUnack` 将 ACK deadline ZADD 纳入 Lua 原子操作，消除了旧版"桶迁移成功、SetAckDeadline 之前崩溃导致任务丢失"的竞态窗口。进程崩溃不再丢任务。

**连接池调优**

高并发下可适当增大 `MaxIdle` 和 `MaxActive`，并启用 `Wait=true` 避免连接池耗尽时直接报错。同时将读取超时适当放宽，防止大 key 操作超时。

### 9. 动态调度（规划中）

Worker 用固定定时器（100ms / 1s），可在空闲时拉长间隔节省资源，积压时缩短间隔加速消费：

```go
interval := baseInterval
if len(ids) == 0 {
    interval = min(interval*2, maxInterval) // 空闲退避
} else {
    interval = baseInterval // 有任务立即恢复
}
```

### 10. 断路器（规划中）

当下游服务持续失败时，引入断路器（如 `sony/gobreaker`），在失败率超过阈值时熔断，避免无效请求消耗资源，半开状态下探测恢复后自动闭合。

### 11. 架构级优化

**多实例水平扩展**

本服务本身无状态，所有状态在后端存储中，可直接水平扩展部署多实例。注意 Worker 数量增多会加剧桶竞争，需配合 Lua 脚本原子操作保证正确性。

**Redis 集群**

单机 Redis 内存和带宽有上限。迁移到 Redis Cluster，将不同分片桶分布到不同节点，突破单机瓶颈。

**消息队列解耦回调**

高并发下直接 HTTP 回调存在背压问题。可将回调动作改为写入消息队列（Kafka / RabbitMQ），由独立的消费服务消费并调用下游，实现削峰填谷和更好的故障隔离。

### 12. 监控与告警

| 监控指标 | 说明 |
|---------|------|
| 桶大小 | 各桶的 ZCARD 值，监控积压 |
| 回调延迟 | 回调响应时间分布，P99 是否超时 |
| 回调成功率 | 低于阈值告警 |
| ACK 超时率 | 超时未确认的任务占比，反映业务方处理能力 |
| 错误桶增速 | 反映下游服务健康度 |
| 连接池状态 | 等待数、命中数，判断池是否够用 |
| 协程数量 | 实例协程数，防止泄漏 |
