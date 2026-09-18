# 单元容灾演练 Runbook

> 目标：证明「一个 cell 整体失联」时，设备能在退避纪律下迁移到健康 cell，影子能从 JetStream 重放重建，**RTO ≤ 30 分钟**。
> 本原型单机 compose 只有一套 EMQX，所谓「kill cell」是把该 cell 在 bootstrap 标记 draining 并切断 conn-gate 到它的上游；
> 步骤与生产一致，差别只在被 kill 的东西是容器还是机房。

## 1. 故障切换六步

| 步 | 操作 | 观察点 / 通过条件 | 命令（原型） |
|---|---|---|---|
| 1 | **标记 draining**：目标 cell 状态 active → draining，bootstrap 不再把新设备调度到它 | `GET /api/v1/bootstrap?sn=` 对映射到该 cell 的 SN 返回其它 cell 或 `retry_after` | `curl -XPOST :8081/internal/cells/1/drain` |
| 2 | **bump cell_map_ver**：全局 cell 映射版本 +1，使设备下次 bootstrap 时发现映射变化 | `iot_global.cell.updated_at` 更新；bootstrap 日志出现新版本号 | `psql -c "UPDATE iot_global.cell SET updated_at=now() WHERE cell_id=1"`（原型以 updated_at 代替版本表） |
| 3 | **kill cell**：切断该 cell 的接入面（生产：下线 EMQX 集群；原型：停 conn-gate 上游或直接 `docker stop emqx`） | 设备批量掉线；`online:{cell}` 归零；bridge 日志 JetStream 仍可写（telemetry 丢弃计数 / event 进磁盘缓冲） | `docker stop emqx` 或 `kill <conn-gate>` |
| 4 | **观察退避迁移**：设备用 `backoff.Next` 退避重连，先 bootstrap 再 connect，被调度到健康 cell；conn-gate 令牌桶把重连压平 | conn-gate `/metrics` 的握手速率不超 `IOT_GATE_RPS`；无 RST 风暴；15 分钟内 `online:{healthy}` 达到迁移前总数的 ≥ 99%；坏固件（固定 1s 重连）的设备被 gate 以 RST 拒绝但不影响其它设备 | `device-simulator -n 50 -bad-firmware 0.05`；`watch curl :8084/metrics` |
| 5 | **重放重建影子**：以新的 durable consumer 从 JetStream `IOT_UP`（72h）按时间点重放，pipeline 重写 `shadow:{sn}` | 重放期间 dedupe 使 TDengine 不出现重复行（同 ts 覆盖）；重放完成后随机抽 20 台 `GET /api/v1/devices/{sn}/shadow` 的 `reported` 与最新 telemetry 一致 | `nats consumer add IOT_UP replay-$(date +%s) --filter 'iot.up.telemetry.1' --deliver by_start_time --opt-start-time <T-1h>`；`IOT_FLUSHERS=2 make run-pipeline` |
| 6 | **验证 RTO**：从第 3 步 kill 到第 4 步 ≥99% 在线 + 第 5 步抽检通过的时间 | **≤ 30 分钟**；同时确认 alarm-svc 在整个过程中没有漏掉安全事件（event 走磁盘缓冲重发） | 记录时间戳到 `docs/loadtest/drill-<date>.md` |

演练后回滚：cell 状态 draining → active；不需要回滚 cell_map_ver（版本只增）。

### 1.1 演练中必须盯的反模式
- 重连速率曲线呈「锯齿」而非「平台」→ 有固件没退避（固定间隔重试），这是**固件红线**，记录 SN 清单交固件团队。
- conn-gate RST 数持续高于令牌桶速率 → 上游已塌陷，熔断应已打开；若没打开检查 `IOT_GATE_*` 阈值。
- 重放后 TDengine 行数增加 → dedupe TTL（10 min）短于重放窗口，属预期；以 shadow 一致性而非行数为验收。

## 2. 三个压测剧本的通过标准（设计值）

| 剧本 | 命令 | 通过标准 | 不通过的典型原因 |
|---|---|---|---|
| conn 5000 | `make loadtest-conn` | ① 假成功 = 0（1s 读探测）；② ok + rst = 5000（每个连接有且只有一种结局）；③ 放行速率 ≈ `IOT_GATE_RPS`；④ EMQX 连接数 == alive | nofile 1024；colima 转发塌陷；macOS 端口池耗尽（见 loadtest-report §4） |
| storm 3000 | `make loadtest-storm` | ① 上游握手速率被压平到令牌桶值；② 15 分钟内 ≥99% 回归；③ EMQX 无重启、无 OOM；④ 坏固件比例 5% 时其余 95% 回归时间不受影响 | 令牌桶 burst 过大；退避实现有误（上限或抖动） |
| flood 30000 | `make loadtest-flood` | ① TDengine 行数增量 == 30000（对账差 0）；② 排空过程无 Nak 风暴（Nak 数 < 1% 消息数）；③ 记录 msg/s 与批大小分布；④ Redis 影子每 SN 只保留批内最新 | 单 flusher 吃不满（120ms 窗口触发占比高）；REST 单 SQL >900KB 未切分；dedupe Redis 抖动 |

所有数字先在 `docs/loadtest-report.md` 填口径，再填结果；没有 CSV 的数字不算通过。
