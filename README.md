## LATER ![](https://travis-ci.org/btfak/later.svg?branch=master)
Later is a redis base delay queue

### Usege
golang version: 1.7+
```
go build github.com/btfak/later
$: ./later -h
Usage of ./later:
  -address string
    	serve listen address (default ":8080")
  -redis string
    	redis address (default "redis://127.0.0.1:6379/0")
```

### Feature

- Delay push message to target
- At-lease-once delivery
- Fail and retry
- Reliable
- Performance


### Frontend API

Response http code: **200** success, **400** request invalid, **404** task not found, **500** internal error

- Create Task

  ```
  Request:
  POST /create
  {
  	"topic":"order",
  	"delay":15, // second
  	"retry":3,  // max retry 3 times, interval 10,20,40... seconds
  	"callback":"http://127.0.0.1:8888/", // http post to target url
  	"content":"hello" // content to post
  }
  Response:
  {
      "id": "35adbde5-77c4-4d65-adac-0082d91f2554"
  }
  ```

- Delete Task

  ```
  Request:
  POST /delete
  {
  	"id":"35adbde5-77c4-4d65-adac-0082d91f2554"
  }
  ```

- Query Task

  ```
  Request:
  POST /query
  {
  	"id":"35adbde5-77c4-4d65-adac-0082d91f2554"
  }
  Response:
  {
      "id": "cb9aefdd-5bd1-4bf3-8c94-1ed5c2ea638e",
      "topic": "order",
      "execute_time": 1504934230,
      "max_retry": 1,
      "has_retry": 0,
      "callback": "http://127.0.0.1:8888/success",
      "content": "hello",
      "creat_time": 1504934220
  }
  ```

## Backend API

- Callback

  ```
  Request:
  POST /?
  {
    "id": "57e177ff-454c-42d6-93ab-65895b950dbf",
    "topic": "order",
    "content": "hello"
  }
  Response:
  {
      "code":100 // 100: success,101: too many request,other: fail
  }
  ```

  At-lease-once delivery, may repeat delivery.  Backend api should idempotent and always return response.

## Inside Later

**Later has  four storage part**

* Task Pool: kv pairs hold fully task data
* Delay Bucket: a sorted set store task id and execute time, which waiting to execute by worker
* Unack Bucket: a sorted set store task which has called backend server and waiting response
* Error Bucket: a sorted set store task which call backend server fail

**Three worker fetch tasks with time ticker**

* Delay Worker: get tasks which reach execute time and move tasks from delay bucket to unack bucket, if call backend server success, delete all task data. Otherwise, move tasks from delay bucket to error bucket
* Unack Worker: move tasks from unack bucket to delay bucket
* Error Worker: move tasks from error bucket to delay bucket

**Concurrence problem**

In general, we will deploy multi instance, workers will get same task, but we judge result when move task from delay bucket to unack bucket, if `ZADD` return 1, worker move on, otherwise worker return immediately.

## 高并发与大数据量优化

当任务量急剧增长时，现有架构会面临多个瓶颈。以下从 Redis、Worker、HTTP 回调、部署等维度给出优化方案。

### 1. Redis 层优化

**分片 Bucket**

当前所有任务共用一个 `later_delay` / `later_unack` / `later_error` Sorted Set。当数据量达到百万级，单 key 的 ZRANGEBYSCORE 会成为热点。可将 Bucket 按 topic 或 task ID 哈希分片为多个 key（如 `later_delay_0` ~ `later_delay_15`），Worker 轮询所有分片，将单 key 压力分散。

**Pipeline 批量操作**

当前每个 Redis 命令都是一次独立网络往返。`createTask` 中 SET + ZADD、`deleteTask` 中 DEL + 3 次 ZREM，都可以用 Pipeline 合并为一次往返，降低网络延迟。`getTasks` 获取 ID 列表后逐个 GET 任务数据，也可用 Pipeline 一次性批量获取。

**Lua 脚本保证原子性**

`bucketTransfer` 中先 ZADD 再 ZREM 非原子操作，多实例并发时可能出现重复投递。用 Lua 脚本将 ZADD + ZREM 合并为一个原子操作：

```lua
local added = redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
if added == 1 then
    redis.call('ZREM', KEYS[1], ARGV[2])
    return 1
end
return 0
```

**连接池调优**

当前 `MaxIdle=200`、`MaxActive=400`，高并发下可适当增大，并启用 `Wait=true` 避免连接池耗尽时直接报错。同时将 `RedisReadTimeout` 从 50ms 适当放宽（如 200ms），防止大 key 操作超时。

**Task TTL 与过期清理**

当前 Task 数据用 `SET EX TTL` 存储在 Redis 中，但没有对 Sorted Set 中的过期 ID 做清理。长时间运行后 Bucket 中可能积累大量已过期但未删除的 ID，ZRANGEBYSCORE 返回后 GET 不到数据造成空转。可增加定时清理逻辑或依赖 Redis Keyspace Notification 自动清理。

### 2. Worker 层优化

**批量拉取与限流**

当前 `ZrangeCount=20`，每次只拉 20 条。高并发时可动态调整，或采用"循环拉取直到为空"的策略提高吞吐。同时应增加限流机制，避免瞬时大量回调压垮下游服务。

**Worker 分离部署**

Delay Worker、Unack Worker、Error Worker 职责不同、负载不同。Delay Worker 是主要吞吐路径，Error Worker 负载较低。可将三者拆分为独立进程/服务，按需扩缩容。

**动态 Interval**

当前 Worker 用固定 ticker（100ms / 1s）。可在空闲时拉长间隔节省资源，积压时缩短间隔加速消费，实现弹性调度：

```go
interval := baseInterval
if len(ids) == 0 {
    interval = min(interval*2, maxInterval) // 空闲退避
} else {
    interval = baseInterval // 有任务立即恢复
}
```

**回调并发控制**

当前 `callback(id)` 用 `go` 无限制启动 goroutine，若积压任务数万，会瞬间创建大量 goroutine 并发请求下游，可能导致 OOM 或打爆下游服务。应使用 worker pool 或 semaphore 限制并发数：

```go
sem := make(chan struct{}, maxConcurrentCallbacks)
sem <- struct{}{}
go func(id string) {
    defer func() { <-sem }()
    callback(id)
}(id)
```

### 3. HTTP 回调优化

**连接池调优**

当前 `MaxIdleConnsPerHost=10`，若下游服务只有少数实例，可适当增大以复用连接。对多下游场景，确保每个 host 都有足够的空闲连接。

**超时与重试策略**

当前 `CallbackTTR=3s`，对于慢接口可能不够。建议按 topic 粒度配置不同的超时时间。HTTP 层面可加入指数退避重试（当前仅靠 Error Bucket 间接重试，延迟较大）。

**断路器**

当下游服务持续失败时，不应继续发送请求。引入断路器（如 `sony/gobreaker`），在失败率超过阈值时熔断，避免无效请求消耗资源，半开状态下探测恢复后自动闭合。

### 4. 架构级优化

**多实例水平扩展**

Later 本身无状态，所有状态在 Redis 中，可直接水平扩展部署多实例。注意 Worker 数量增多会加剧 Bucket 竞争，需配合 Lua 脚本原子操作保证正确性。

**Redis Cluster**

单机 Redis 内存和带宽有上限。迁移到 Redis Cluster，将不同分片 Bucket 分布到不同节点，突破单机瓶颈。

**读写分离**

若查询接口（/query）压力较大，可使用 Redis 从库分担读流量，写操作走主库。

**消息队列解耦回调**

高并发下直接 HTTP 回调存在背压问题。可将回调动作改为写入消息队列（Kafka / RabbitMQ），由独立的 Consumer 服务消费并调用下游，实现削峰填谷和更好的故障隔离。

### 5. 监控与告警

| 监控指标 | 说明 |
|---------|------|
| Bucket Size | 各 Bucket 的 ZCARD 值，监控积压 |
| Callback Latency | 回调 RT 分布，P99 是否超时 |
| Callback Success Rate | 回调成功率，低于阈值告警 |
| Error Bucket Growth | Error Bucket 增速，反映下游健康度 |
| Redis Pool Stats | 连接池等待数、命中数，判断池是否够用 |
| Goroutine Count | 实例 goroutine 数量，防止泄漏 |