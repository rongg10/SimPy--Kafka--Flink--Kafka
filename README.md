# Kafka 到 Flink 的链路模拟管道

## 项目简介

这是一个 **SimPy → Kafka → Flink → Kafka** 的链路模拟与处理管道，用于生成分布式服务调用日志，并在 Flink 中按 IP 进行父子链路匹配与聚合。

- SimPy 生成服务调用链并发送到 Kafka
- Flink 按匹配 IP 分区，进行父子关系链接
- 事件时间 Watermark + idle flush 控制输出与状态清理

## 架构与数据流

1. `simpy_message_generator.py` 生成调用日志写入 Kafka（默认 topic: `test-topic`）
2. `flink-jobs/simpy_kafka_reader.py` 读取 Kafka，按 IP 进行父/子匹配并聚合输出
3. 输出结果写回 Kafka 结果 Topic（`KAFKA_OUTPUT_TOPIC`），同时也会打印到 Flink 日志（stdout）

## 复杂度与性能

### 当前复杂度（按 IP key）

- `IpLinkingProcess`：设 `N` 为某个 IP 下活跃消息数量（父+子）。每条新消息会扫描该 IP 下的所有相反角色消息，单条事件最坏 `O(N)`；一个窗口内总体最坏 `O(N^2)`（更精确为 `O(P*C)`）。状态内存为每 IP `O(N)`，直到 `end_at_ms` 定时器或 idle flush 清理。
- `MessageAggregationProcess`：每个消息 id 维护基础消息+父/子列表。每次更新需要重写 JSON 列表，单次更新与列表大小成正比；每条消息总工作量与链路数量线性相关。内存随已关联的 parent/child 数增长，直到 `end_at_ms` 或 idle flush 清理。
- `linking_utils.WatermarkMatcher`（测试/辅助）：每条消息与缓冲区全部消息比较，发射也需遍历缓冲区；单条 `O(N)`、总体最坏 `O(N^2)`，内存 `O(N)`。

### 已有的性能优化

- 按匹配 IP 分区：把串联限定在单 key 范围，提升并行度。
- 事件时间 Watermark（`SIMPY_MAX_OUT_OF_ORDER_MS`）+ `end_at_ms` 定时器：过期状态自动清理，避免无限保留。
- Idle flush（`SIMPY_IDLE_FLUSH_MS`）：对空闲 key 主动清理，降低稀疏流内存占用。
- 两阶段处理（linking → updates → aggregation）：减少全量消息重复改写。

## 消息格式

SimPy 生成的单条消息格式如下（JSON）：

```json
{
  "id": "msg_1",
  "src_ip": "10.0.0.1",
  "dst_ip": "10.1.0.2",
  "start_at_ms": 1700000000000,
  "latency_msec": 40.0,
  "end_at_ms": 1700000000040
}
```

Flink 聚合后的输出会追加 `parents` / `children`：

```json
{
  "id": "msg_1",
  "src_ip": "10.0.0.1",
  "dst_ip": "10.1.0.2",
  "start_at_ms": 1700000000000,
  "latency_msec": 40.0,
  "end_at_ms": 1700000000040,
  "parents": [],
  "children": ["msg_2"]
}
```

## 链路匹配规则

父子关系匹配条件（见 `flink-jobs/linking_utils.py`）：

- `parent.dst_ip == child.src_ip`
- `parent.start_at_ms <= child.start_at_ms`
- `parent.end_at_ms >= child.end_at_ms`

## 目录结构

- `docker-compose.yml`：启动 Zookeeper / Kafka / Flink
- `Dockerfile.flink`：Flink + Python 运行环境
- `simpy_message_generator.py`：SimPy 调用链模拟器
- `flink-jobs/simpy_kafka_reader.py`：主 Flink 作业（链路匹配 + 聚合）
- `flink-jobs/kafka_reader.py`：简单 Kafka 读入示例
- `flink-jobs/linking_utils.py`：匹配规则与 watermark 工具
- `produce_test_messages.py`：简单测试消息生产
- `test_chain_linking.py`：纯 Python 匹配规则测试

## 快速开始

### 1) 启动服务

```bash
docker compose up -d
```

### 2) 提交 Flink 作业（链路匹配）

```bash
docker exec jobmanager ./bin/flink run -py /opt/flink/usrlib/simpy_kafka_reader.py
```

### 3) 生成模拟消息

```bash
# 持续流式生成
python simpy_message_generator.py --stream --interval 500 --realtime --ip-pool-size 6

# 固定数量
python simpy_message_generator.py --count 5 --interval 50 --realtime --ip-pool-size 3

# 带抖动
python simpy_message_generator.py --count 5 --interval 100 --std-dev 200 --realtime --ip-pool-size 3

# 打印消息详情
python simpy_message_generator.py --count 5 --interval 50 --realtime --print-msg --ip-pool-size 3
```

### 4) 观看输出

```bash
# 最近日志
docker logs taskmanager --since 1m 2>&1 | grep -E "^\\+I\\["

# 持续跟踪
docker logs -f taskmanager 2>&1 | rg "\\+I\\[|linked_message"
```

```bash
# 读取写回 Kafka 的结果
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic linked-topic \
  --from-beginning \
  --max-messages 5
```

## SimPy 生成器说明

### 调用链结构

当前模拟链路大致为：

- Client → Main
- Main 并行调用 SubService1 + SubService2
- SubService2 再调用 SubService3
- Main 可能二次调用 SubService2（约 40% 概率）

### 命令行参数

- `--realtime`：使用实时时钟（1 单位 = 1 ms）
- `--count N`：生成 N 条 trace
- `--stream`：持续生成（会覆盖 `--count`）
- `--interval MS`：平均间隔（毫秒）
- `--std-dev MS`：间隔抖动标准差
- `--print-msg`：打印每条消息
- `--debug`：不写 Kafka，仅本地模拟
- `--topic NAME`：Kafka topic（默认 `test-topic`）
- `--bootstrap HOSTS`：Kafka bootstrap（默认 `localhost:9092`）
- `--ip-pool-size N`：每个服务 IP 池大小

### 延迟与乱序模拟（环境变量）

消息会按卡方分布加入随机延迟，用于制造乱序：

- `SIMPY_DELAY_CHISQ_DF`（默认 2.0）
- `SIMPY_DELAY_SCALE_MS`（默认 1000）
- `SIMPY_MAX_DELAY_MS`（默认 30000）

## Flink 作业说明

### Watermark 与状态清理

- Watermark 基于 `start_at_ms`
- `SIMPY_MAX_OUT_OF_ORDER_MS`：最大乱序容忍
- `SIMPY_IDLE_FLUSH_MS`：空闲 key 的清理时间

### Kafka 配置（环境变量）

- `KAFKA_TOPIC`：消费 topic
- `KAFKA_OUTPUT_TOPIC`：结果写回 topic
- `KAFKA_BOOTSTRAP`：Kafka broker
- `KAFKA_AUTO_OFFSET_RESET`：偏移量策略
- `SIMPY_PARALLELISM` / `KAFKA_PARTITIONS` / `KAFKA_NUM_PARTITIONS`：并行度

## 常用命令

```bash
# 查看 Flink 作业

docker exec jobmanager ./bin/flink list

# 取消作业

docker exec jobmanager ./bin/flink cancel <job-id>

# 查看 Kafka 原始消息

docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic test-topic \
  --from-beginning \
  --max-messages 5

# 重建 taskmanager（清日志）

docker compose rm -sf taskmanager

docker compose up -d taskmanager
```

## 已进行的测试

- 普通fixed count, 及不同count, interval, std-dev, print-msg, realtime等
- 高压流十分钟，interval 50高并发，每分钟约6000多条，约等于一天800多万条

## 本地测试

无需 Flink/Kafka，可直接验证匹配逻辑：

```bash
python test_chain_linking.py
```

## 备注

- Kafka 端口：`localhost:9092`（宿主机访问） / `kafka:29092`（容器内访问）
- Flink UI：`http://localhost:8081`
