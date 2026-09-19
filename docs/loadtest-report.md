# 压测报告

> 状态：**2026-09-19 实测已回填**。本文件先固定「口径、测量方法、复现命令、已知坑」再填结果。
> **不要手填任何数字**——没有 CSV 佐证的数字一律不进本表；每个结果都附 CSV 路径与 sha256。
> 所有数字来自单机 docker compose 原型环境，**不是生产容量证明**：设计目标千万保有 / 300 万并发在线
> 见技术方案 §2，本轮只验证单机链路正确性与瓶颈位置。

## 0. 先报口径，再报数字

| 维度 | 本次口径 | 说明 |
|---|---|---|
| 运行位置 | **host loopback**（EMQX 端口直接映射到 host，gate 与 loadgen 都在 host 跑） | 非 colima 转发，见 §4 第 4 条 |
| 依赖部署 | docker compose（emqx / nats / redis / postgres / tdengine），单副本 | `deploy/docker-compose.yml` |
| pipeline flusher 数 | **1**（`IOT_FLUSHERS=1`） | 单 flusher 是吞吐上限的主因；多 flusher 需按 cell subject 分区 |
| TDengine 写入路径 | **REST（taosAdapter :6041）** | 与技术方案 §6.3 口径一致；原生连接预期高 2–4 倍，不在本轮 |
| 单元数 | `IOT_CELLS=2` | |
| conn-gate 令牌桶 | `IOT_GATE_RPS`（默认 5000/s，burst 200） | |
| 机器 | MacBookPro18,3 · 8 核 · 16 GB · macOS 26.5.1 · `ulimit -n` 1048576 | EMQX 容器 nofile 1048576（compose 已配） |
| 时间 | 2026-09-19 16:17–16:20 CST | 三剧本连续跑，中间未重启依赖 |

## 1. 场景与通过标准

| 剧本 | 命令 | 规模 | 设计通过标准（docs/drill-runbook.md §2） | 结果 |
|---|---|---|---|---|
| conn 连接风暴 | `make loadtest-conn` | 5000 并发，500/s 建连 | 无假成功；被 gate 拒绝的连接以 RST 形式被 loadgen 识别；`alive` 最终 ≈ 放行数 | **通过** |
| storm 重连风暴 | `make loadtest-storm` | 3000 台同时重连（`-burst`） | gate 令牌桶把上游握手压平到 `IOT_GATE_RPS`；EMQX 不掉线、不 OOM；退避设备在 15min 内全部回归 | **通过**（33s 全回归） |
| flood 泄洪 | `make loadtest-flood` | 预灌 30000 条到 JetStream 后计时排空 | TDengine 行数 == 预灌数（对账差 0）；排空吞吐 msg/s 记录；无 Nak 风暴 | **通过**（对账差 0，Nak 0） |

### 1.1 结果表（回填区）

| 剧本 | 指标 | 值 | CSV |
|---|---|---|---|
| conn | 成功建连数（ok） | **5000**（耗时 11.485s） | `conn.csv` |
| conn | RST 数（gate 拒绝 + 熔断） | **0**（5000 未超 5000/s 令牌桶，全部放行） | 同上 |
| conn | 1s 探测后仍存活（alive） | **5000** | 同上 |
| conn | 假成功数（accept 后被 RST） | **0** ✅ 口径成立 | 同上 |
| conn | 三方对账 | loadgen ok 5000 == gate `accepted` 5000 == CSV 末行 5000 | gate `/metrics` |
| storm | 峰值握手速率 / 上游握手速率 | 第 2 秒放行 1679、同时拒 1321 → 上游被压平在令牌桶内 | `storm.csv` |
| storm | 全部回归耗时 | **33.4s**（远优于 15min 标准） | 同上 |
| storm | 被令牌桶拒绝次数 | **1321**（`rejected_rate`，熔断 `rejected_breaker` 0） | gate `/metrics` |
| storm | EMQX 是否掉线 / OOM / 触发 nofile | 否 / 否 / 否（容器 nofile 1048576） | 容器存活 |
| flood | 预灌条数 / TDengine 行数 / 差值 | 30000 / 30000 / **0** ✅ | `flood.csv` |
| flood | 排空耗时 / 吞吐 | 8.503s / **3528 msg/s** | 同上 |
| flood | Nak 数 / 重复数 / 毒消息 | 0 / 0 / 0 | pipeline `/metrics` |
| flood | 批数（30000 条 / 134 批 ≈ 224 条/批） | 134 批，**未打满 500 条**，说明多数由 120ms 窗口触发 | pipeline `/metrics` |
| flood | `IOT_FLUSHERS=4` 对照 | 8.002s / **3749 msg/s**（+6%） | `flood-f4.csv` |

**CSV sha256**（文件已 gitignore，只留指纹供核对）

```
b3c00ef34c26b5afbb5d71dfa5b0696fd33a9c6e6b157220b3b8b7c6c7a9a792  conn.csv
258981386c84e8d283abb94ab1a54c260ffb2927e7e8d919b1e49497f5041349  storm.csv
ca82904c686e950720cc3ff1ddd49abd140662274a1afc40ab10c543c3f43a39  flood.csv
e73d806578bb1c18aa3022308e1c7dc12e7b85cdf0a1d49f92772cc0362d4398  flood-f4.csv
```

### 1.2 本轮推翻的一个假设

§4 第 5 条原本断言「单 flusher 是瓶颈不是 TDengine，加 flusher 比调 TDengine 有用」。
实测 `IOT_FLUSHERS` 从 1 加到 4，吞吐只从 3528 涨到 3749 msg/s（+6%），**假设不成立**。

批大小数据给出了原因：30000 条只攒了 134 批，约 224 条一批，**远未打满 500 条阈值**，
说明批是被 120ms 窗口触发而不是被条数触发——消费端根本没有积压到需要更多 flusher 的程度。
瓶颈在单批的写入往返上（REST 到 taosAdapter 的串行 HTTP），不在并行度。
下一步该验证的是 TDengine 原生连接而不是继续加 flusher。

这条写进来是因为它比「跑通了」更有价值：**先报口径再报数字的意义，就是让原有假设可被推翻**。

### 1.3 这些数字不能用来说明什么

- 不能说明千万级容量。5000 连接是单机 loopback，且贴着 macOS 临时端口池 8192 的上限跑（见 §4 第 3 条）。
- 不能说明生产吞吐。3528 msg/s 是 REST 写入路径、单副本、无 TLS、无真实设备抖动的结果。
- 不能替代真机验证。全部流量来自模拟器，没有固件、没有弱网、没有断连重传的真实分布。

## 2. 每个数字怎么量

- **conn.ok / rst / alive**：loadgen 每个连接 `Dial` 成功后 **等待 1s 并做一次读探测**；读到 RST/EOF 记 `rst`，否则记 `alive`。
  只统计 `Dial` 成功不算 `ok`（见 §4 第 1 条）。CSV 列：`t,ok,rst,alive`，每秒一行。
- **storm 回归耗时**：从 `-burst` 发出到 `online:{cell}` 计数回到基线的时间；设备端退避用 `pkg/backoff`（min(2^n·base, 15min)+rand 30s）。
- **flood 吞吐**：`flood -pre 30000` 先把信封直接发进 `IOT_UP`（不经 EMQX，隔离管道本身），然后启动 pipeline 计时；
  `pipeline` 消费到 pending=0 为止。吞吐 = 30000 / 排空秒数。对账 = `tdengine.CountRows("telemetry")` 增量 vs 30000。
- **CSV 路径**：`docs/loadtest/*.csv`（已 gitignore，报告只引用路径 + 文件 sha256）。

## 3. 复现

```bash
make dev                # 起依赖
make run-conn-gate & make run-bridge & make run-pipeline &   # 至少这三个
make loadtest-conn      # → docs/loadtest/conn.csv
make loadtest-storm     # → docs/loadtest/storm.csv
make loadtest-flood     # → docs/loadtest/flood.csv
```
回填时同时记录：`sysctl -n hw.ncpu hw.memsize`、`sw_vers`、`docker info | grep -i cpus`、`ulimit -n`、colima/host 二选一。

## 4. 已知测量坑（踩过才写进来的）

1. **loadgen 假成功**：conn-gate 无令牌时 `SetLinger(0)+Close` 发 RST，但客户端 `Dial` 在 accept 完成那一刻就返回成功。
   早期版本把 Dial 成功计为 ok，数字虚高一倍以上。修正：连接后 **1s 读探测**，读到 RST 才算拒绝，读超时才算 alive。
2. **EMQX 容器 nofile 1024**：默认容器 ulimit 只有 1024，conn 5000 一定在 ~1000 处崩，看起来像 gate 或 EMQX 的 bug。
   修正：compose 里 `ulimits: nofile: {soft: 1048576, hard: 1048576}`，并确认 `docker exec emqx ulimit -n`。
3. **macOS 临时端口池**：`net.inet.ip.portrange.first=49152 → last=65535` 共 16384 个；loadgen→gate→EMQX 双跳 loopback 每条连接吃两个源端口，
   上限 ≈ **8192** 并发。5000 剧本贴着上限跑，TIME_WAIT 堆积后第二轮会失败。修正：轮次之间等 `sysctl net.inet.tcp.msl` × 2，或调 `portrange.first`。
4. **colima ssh port-forward 塌陷**：通过 colima 转发 1883 时约 **1000 并发**即开始丢连接，且转发进程卡死会把 Docker API 一起带下去（`docker ps` 挂起）。
   修正：高并发剧本必须 **host loopback**（EMQX 端口直接映射到 host，gate 与 loadgen 都在 host 跑），并靠 conn-gate 的 **上游熔断**
   （dial 连续失败 N 次 → 30s 内直接 RST）避免把已经塌陷的上游打得更死。报告里「运行位置」一栏就是为这条坑而设。
5. ~~**单 flusher 是瓶颈不是 TDengine**：加 flusher 比调 TDengine 有用。~~
   **2026-09-19 实测推翻**：flusher 1 → 4 只带来 +6%（3528 → 3749 msg/s）。批大小分布确实显示多数批由 120ms 窗口
   触发而非 500 条触发，但这恰恰说明**消费端没有积压**，不需要更多并行度；瓶颈在单批写入的串行 HTTP 往返
   （REST → taosAdapter）。留着这条原文是为了记住：这个判断当初只是推测，没有数据。下一步验证 TDengine 原生连接。
