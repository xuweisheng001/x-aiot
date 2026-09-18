# 压测报告（模板 · 结果待实测）

> 状态：**原型阶段模板**。本文件只固定「口径、测量方法、复现命令、已知坑」，所有结果单元格保留 `待实测`，
> 由 `make loadtest-*` 跑出 CSV 后回填。**不要手填任何数字**——没有 CSV 佐证的数字一律不进本表。

## 0. 先报口径，再报数字

| 维度 | 本次口径 | 说明 |
|---|---|---|
| 运行位置 | 待实测（`host loopback` / `colima port-forward`） | 二者差一个数量级，见 §4 |
| 依赖部署 | docker compose（emqx / nats / redis / postgres / tdengine），单副本 | `deploy/docker-compose.yml` |
| pipeline flusher 数 | **1**（`IOT_FLUSHERS=1`） | 单 flusher 是吞吐上限的主因；多 flusher 需按 cell subject 分区 |
| TDengine 写入路径 | **REST（taosAdapter :6041）** | 与技术方案 §6.3 口径一致；原生连接预期高 2–4 倍，不在本轮 |
| 单元数 | `IOT_CELLS=2` | |
| conn-gate 令牌桶 | `IOT_GATE_RPS`（默认 5000/s，burst 200） | |
| 机器 | 待实测（型号 / 核数 / 内存 / macOS 版本） | |
| 时间 | 待实测 | |

## 1. 场景与通过标准

| 剧本 | 命令 | 规模 | 设计通过标准（docs/drill-runbook.md §2） | 结果 |
|---|---|---|---|---|
| conn 连接风暴 | `make loadtest-conn` | 5000 并发，500/s 建连 | 无假成功；被 gate 拒绝的连接以 RST 形式被 loadgen 识别；`alive` 最终 ≈ 放行数 | 待实测 |
| storm 重连风暴 | `make loadtest-storm` | 3000 台同时重连（`-burst`） | gate 令牌桶把上游握手压平到 `IOT_GATE_RPS`；EMQX 不掉线、不 OOM；退避设备在 15min 内全部回归 | 待实测 |
| flood 泄洪 | `make loadtest-flood` | 预灌 30000 条到 JetStream 后计时排空 | TDengine 行数 == 预灌数（对账差 0）；排空吞吐 msg/s 记录；无 Nak 风暴 | 待实测 |

### 1.1 结果表（回填区）

| 剧本 | 指标 | 值 | CSV |
|---|---|---|---|
| conn | 成功建连数（ok） | 待实测 | `docs/loadtest/conn.csv` |
| conn | RST 数（gate 拒绝 + 熔断） | 待实测 | 同上 |
| conn | 1s 探测后仍存活（alive） | 待实测 | 同上 |
| conn | 假成功数（accept 后被 RST） | 待实测（**应为 0**，否则口径不成立） | 同上 |
| storm | 峰值握手速率 / 上游握手速率 | 待实测 | `docs/loadtest/storm.csv` |
| storm | 全部回归耗时 | 待实测 | 同上 |
| storm | EMQX 最大连接数 / 是否触发 nofile 上限 | 待实测 | 同上 |
| flood | 预灌条数 / TDengine 行数 / 差值 | 待实测 / 待实测 / 待实测 | `docs/loadtest/flood.csv` |
| flood | 排空耗时 / 吞吐 msg/s | 待实测 | 同上 |
| flood | 批大小分布（500 触发 vs 120ms 触发占比） | 待实测 | pipeline `/metrics` |

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
5. **单 flusher 是瓶颈不是 TDengine**：flood 吞吐先看 pipeline `/metrics` 的批大小分布——如果绝大多数批是 120ms 窗口触发而非 500 条触发，
   说明消费端还没吃满，加 flusher（按 cell 分区）比调 TDengine 有用。
