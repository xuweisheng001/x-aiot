# BL1 消费级激光整机 · 线上事故预推演与处置方案

> Incident Pre-mortem · 对应 docs/prd-bl1-consumer-laser.md 与 docs/tech-design-aiot-platform.md

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1 · 2026-09-18 |
| 目的 | 在设备铺出去之前，把最可能发生的线上事故逐个推演一遍：怎么触发、怎么传播、看哪个指标能第一时间发现、前 15 分钟做什么、之后怎么恢复、怎么根治、怎么在 staging 复现 |
| 方法 | 沿五条核心链路（接入 / 数据 / 告警 / 指令 / OTA）加隐私与产线两条支线逐段问「这里挂了会怎样」；每条事故落到具体服务、指标名、命令 |
| 前置 | docs/incident-register-storm.md（一次真实事故的复盘，本文多条处置源自它）· docs/drill-runbook.md（单元容灾六步） |

---

## 0. 通用规则

### 0.1 事故分级

| 级别 | 定义 | 响应 | 示例 |
|---|---|---|---|
| S1 | 安全告警链路失效，或有设备被 OTA 刷砖，或产线停线 | 立即，全员，15 分钟内止血 | 火焰事件未推送；fused 未触发 |
| S2 | 核心链路对 ≥ 10% 设备不可用超过 5 分钟 | 15 分钟内响应 | 重连风暴；auth 全拒 |
| S3 | 单链路降级，用户可感知但有替代 | 1 小时内响应 | 影子延迟 > 30 s；指令回执丢 |
| S4 | 内部指标异常，用户无感 | 工作时间处理 | poison 计数上升；分区未预建 |

### 0.2 处置四原则

1. **先止血再定位**。止血动作必须是本文列出的、事先验证过的动作，不在事故中即兴发明。
2. **安全链路优先**。任何止血动作不得降低 alarm-svc 的可用性。要在 pipeline 与 alarm 之间取舍时，牺牲 pipeline。
3. **临时关闭的保护必须登记**。关掉任何一道护栏（限流、熔断、审批、squelch）时，同时在 §10 的登记表写一行并设到点提醒。历史教训：一段签名校验临时关掉后三天才想起来。
4. **报数字先报口径**。事故通报里每个数字都要写清来自哪个指标、哪个时间窗。

### 0.3 前 15 分钟通用检查单

```bash
# 1 五个服务健康与关键计数（生产替换为 Grafana 面板）
for p in 8081 8082 8083 8085 8086 8087 8088 8089; do curl -s -m 2 localhost:$p/healthz || echo "DOWN $p"; done
curl -s localhost:8084/metrics          # conn-gate: accepted rejected_rate rejected_breaker active upstream_fail breaker_open
curl -s localhost:8089/metrics          # pipeline: consumed poison dup nak lag batches rows_written
curl -s localhost:8088/metrics          # bridge: received published dropped_telemetry buffered buffer_len publish_err
curl -s localhost:8085/metrics          # alarm: events_squelched squelch_redis_errors ...
# 2 JetStream 积压
nats consumer info IOT_UP pipeline-1 | grep -i pending
nats consumer info IOT_UP alarm      | grep -i pending
# 3 在线数与最近 5 分钟事件
redis-cli get online:1; redis-cli get online:2
psql -c "SELECT status, count(*) FROM iot_shard.alarm WHERE created_at > now()-interval '5 min' GROUP BY 1"
# 4 最近一次变更：发布、配置、OTA 批次、证书操作
psql -c "SELECT id,stage,status,ok_count,fail_count,created_at FROM iot_global.ota_batch ORDER BY id DESC LIMIT 5"
```

先看「最近一次变更」。多数事故是变更引起的，回滚变更是最快的止血。

---

## 1. 事故清单总览

| ID | 事故 | 链路 | 级别 | 第一检测信号 | 止血时限 |
|---|---|---|---|---|---|
| INC-01 | 重连风暴：云端重启或令牌集中过期后设备同步重连 | 接入 | S2 | conn-gate rejected_rate 陡升，握手曲线锯齿 | 15 min |
| INC-02 | 认证全拒：auth-svc 依赖的 PG 不可达，fail-closed 放大 | 接入 | S2 | auth deny 比例 > 50%，online 下降 | 10 min |
| INC-03 | 证书误吊销：批量操作把在用证书标 revoked | 接入 | S2 | deny 原因 cert status revoked 集中 | 10 min |
| INC-04 | 熔断误开：EMQX 慢但没死，conn-gate 把所有连接 RST | 接入 | S2 | breaker_open=1 且 EMQX 进程存活 | 5 min |
| INC-05 | JetStream 不可用或磁盘满 | 数据 | S2 | bridge publish_err 上升，buffer_len 增长 | 15 min |
| INC-06 | TDengine 写失败：Nak 风暴、lag 涨 | 数据 | S3 | pipeline nak 上升，NumPending 增长 | 30 min |
| INC-07 | Redis 故障：幂等放行、影子停更、指令结果丢 | 数据 | S3 | dedupe_redis_err、shadow_err 上升 | 15 min |
| INC-08 | 毒消息：固件上报字段变更导致 poison 激增 | 数据 | S3 | poison / consumed > 1% | 1 h |
| INC-09 | 消费 lag 超 30 s：单 flusher 饱和 | 数据 | S3 | NumPending 持续增长，批大小分布偏向窗口触发 | 30 min |
| INC-10 | 火焰事件漏告警 | 告警 | **S1** | alarm 消费组 pending 增长；事件明细有 FLAME 但 alarm 表无行 | 5 min |
| INC-11 | 误报风暴：一批传感器缺陷触发上万条 OVER_TEMP | 告警 | S2 | alarm open 数每分钟 > 100；同 code 同机型集中 | 15 min |
| INC-12 | 升级短信异常：重复发送或该发不发 | 告警 | S2 | escalated 计数与 notified 超时数不匹配 | 15 min |
| INC-13 | 误停机：客服或 Agent 对错误设备下 stop | 指令 | S2 | cmd_audit source=support 突增 | 5 min |
| INC-14 | 指令回执丢失：设备执行了但用户看到未知 | 指令 | S3 | cmd_audit acked_at 为空比例 > 10% | 1 h |
| INC-15 | deviceapi 被刷：App 故障重试或恶意 | 指令 | S3 | 429 比例上升，PG 连接池满 | 15 min |
| INC-16 | 刷坏一批固件且熔断未触发 | OTA | **S1** | rolled_back / failed 上升但 status 仍 running | 5 min |
| INC-17 | 灰度范围失控：抽样错误或档位越级 | OTA | S1 | target_total 与档位百分比不符 | 5 min |
| INC-18 | CDN 故障导致误熔断 | OTA | S3 | failed 的 error_code 全是下载类 | 30 min |
| INC-19 | 作业中升级：idle_only 未生效 | OTA | S2 | JOB_FAIL 与 OTA_PROGRESS 同 SN 时间重叠 | 15 min |
| INC-20 | 未 opt-in 画面落云存储 | 隐私 | S1 | 对象存储审计：写入 SN 的 camera_cloud_optin=false | 立即 |
| INC-21 | 删除传导失败：删号后仍可关联 | 隐私 | S2 | 传导任务失败计数 | 24 h 内 |
| INC-22 | 产线停线：cf001-svc 或其依赖不可用 | 产线 | **S1** | /sign 5xx 或超时，产线告警 | 10 min |
| INC-23 | 配额异常：超发或被吃光 | 产线 | S2 | registered 增速异常，同 IP 大量 /sign | 15 min |
| INC-24 | 月初 cmd_audit 分区缺失，审计落库停止 | 运维 | S3 | IOT_CMD 消费组 pending 增长，PG 报 no partition | 1 h |
| INC-25 | 单元整体故障 | 运维 | S2 | online:{cell} 归零 | 30 min RTO |
| INC-26 | EMQX 连接数触顶：license 或 nofile | 运维 | S2 | EMQX 拒连，conn-gate upstream_fail 上升 | 15 min |
| INC-27 | 时钟漂移：设备 ts 偏差导致纠偏错乱、dedupe 失效 | 运维 | S3 | recv_ts 与 payload.ts 差值分布异常 | 1 h |

---

## 2. 接入链路

### INC-01 重连风暴

**触发**：云端接入层重启、EMQX 滚动升级、区域网络抖动、或一批设备的会话在同一时间窗失效。设备同步重连。若其中有固件退避有缺陷（固定间隔），重连速率不会衰减。

**传播**：TCP 握手挤占 EMQX CPU → TLS 握手排队 → 已连接设备心跳超时掉线 → 更多重连。没有 conn-gate 时 5 分钟内 broker 雪崩。

**检测信号**

| 指标 | 阈值 |
|---|---|
| conn-gate rejected_rate | 每秒增量 > IOT_GATE_RPS 的 20% 持续 1 分钟 |
| 握手速率曲线形态 | 平台形正常（令牌桶在工作）；锯齿形说明有设备不退避 |
| online:{cell} | 下降后 15 分钟内未回到基线 90% |
| conn-gate active vs EMQX 连接数 | 应相等；不等说明有假成功 |

**止血（15 分钟内）**

1. 确认 conn-gate 在工作：rejected_rate 在涨、breaker_open=0、EMQX 未重启。如果是，**不要动令牌桶**，风暴正在被压平，等退避生效。
2. 如果 EMQX 已在抖动：临时调低令牌桶到当前 EMQX 能承受的握手速率（重启 conn-gate 带新 IOT_GATE_RPS，生产用 Redis 集中桶热调）。登记到 §10。
3. 如果单元已过载：`curl -XPOST :8081/internal/cells/1/overload -d '{"overloaded":true}'`，bootstrap 对该 cell 返回 retry_after=30，让还没连上的设备晚点来。

**恢复**：观察 15 分钟回归曲线。回归 ≥ 99% 后取消 overload 标记，令牌桶调回默认，关闭登记项。

**根治**

- 从 conn-gate 日志或 EMQX 连接日志导出锯齿形重连的 SN 清单，比对 fw_version，定位不退避的固件版本，走 OTA 修复。这是固件红线 DR-01 的违反。
- 会话过期时间加随机抖动，避免集中过期。

**演练**：`loadgen storm -n 3000 -burst` + `device-simulator -n 500 -bad-firmware 0.05`。通过标准见 docs/drill-runbook.md §2。

---

### INC-02 认证全拒

**触发**：auth-svc 依赖的 PG 分片库不可达、连接池耗尽、或 device 表被误改（status 批量置 manufactured）。auth-svc 是 fail-closed：查库出错一律 deny。

**传播**：新连接全部被拒 → 设备进入退避 → 已连接设备暂不受影响（EMQX 不重复认证）→ 但任何原因掉线的设备都回不来 → 在线数缓慢下降 → 15 分钟后进入 INC-01 形态。

**检测信号**：auth-svc 日志 deny 原因分布，store error 占比 > 10% 即告警。online 缓慢下降。

**止血**

1. 确认 PG 状态。如果是连接池耗尽：先看是谁占了连接（`pg_stat_activity`），通常是 deviceapi 的 telemetry 查询或 OTA 圈选慢 SQL，先 kill 那些查询。
2. 如果 PG 短时间无法恢复：auth-svc 启用**本地缓存兜底**（生产必备，原型未实现）：最近 24 小时成功认证过的 (sn, cert_fp) 在 PG 不可达时允许通过。这是有意的 fail-open 降级，仅限已知设备，必须登记并在 PG 恢复后 1 小时内关闭。
3. 如果是 device 表被误改：从审计日志找到 UPDATE 语句，按 SN 集合回滚 status。

**恢复**：PG 恢复后观察 deny 比例回到基线（< 1%）。

**根治**：auth-svc 与 deviceapi 用独立连接池与只读副本；auth 查询只走 PK 索引；device 表关键列变更加 trigger 审计。

**演练**：staging 停 PG 30 秒，观察 auth deny 与 online 曲线，验证缓存兜底。

---

### INC-03 证书误吊销

**触发**：运维批量吊销一批过期证书时 SQL 条件写错，把在用证书也标 revoked。或 PKI 系统推送了错误的 CRL。

**传播**：吊销 5 s 内生效是设计目标，这里它变成了放大器：5 秒内被吊销设备全部被 kick 并拒连。

**检测信号**：deny 原因 `cert status revoked` 在 1 分钟内 > 100 台。device_cert 表 revoked 行数突增。

**止血**

```sql
-- 找到本次批量操作影响的证书（按 revoked 时间窗）
SELECT count(*), min(sn), max(sn) FROM iot_shard.device_cert
 WHERE status='revoked' AND revoked_reason='<本次原因>' ;
-- 确认后回滚
UPDATE iot_shard.device_cert SET status='active', revoked_reason=NULL
 WHERE status='revoked' AND revoked_reason='<本次原因>' AND expires_at > now();
```

设备在退避后重连即恢复，最长 15 分钟。

**根治**：吊销走双人审批接口，不允许直接 SQL；单次吊销数量上限（如 100），超过必须分批并在两批之间观察 deny 曲线；吊销先在 staging 跑 dry-run 输出影响 SN 清单。

---

### INC-04 熔断误开

**触发**：EMQX 短时 GC 停顿或网络抖动导致 conn-gate dial 上游连续 5 次超时（IOT_GATE_BREAK_N=5，DialTimeout 2 s），熔断打开 30 秒。EMQX 其实活着。

**传播**：30 秒内所有新连接被 RST。30 秒后自动复位，如果 EMQX 仍慢，再次打开。反复开合形成周期性拒连。

**检测信号**：breaker_open 在 1 与 0 之间反复翻转；upstream_fail 上升但 EMQX 进程与端口正常。

**止血**：如果 EMQX 确实健康只是慢，临时把 IOT_GATE_BREAK_N 调大（20）、DialTimeout 调到 5 s，重启 conn-gate。conn-gate 重启会断开经它代理的所有连接，因此**先确认设备退避正常再重启**，并在低峰执行。多副本时逐个重启。

**根治**：熔断加半开状态（试探性放行 1% 连接再决定全开）；dial 失败区分「拒绝」与「超时」，超时不计入连续失败。

---

## 3. 数据链路

### INC-05 JetStream 不可用或磁盘满

**触发**：NATS 节点磁盘写满（72 小时保留 × 消息量超预估）、节点 OOM、或 Raft 失去多数。

**传播**：bridge publish 失败 → telemetry 丢弃计数（设计如此）→ event / cmd_ack / ota_progress 进磁盘缓冲 `./data/bridge-buffer.jsonl` → 缓冲文件持续增长 → bridge 所在磁盘也满 → bridge 崩溃 → **安全事件丢失**。

**检测信号**

| 指标 | 阈值 |
|---|---|
| bridge publish_err | 每秒 > 0 持续 30 秒 |
| bridge buffer_len | > 10 万行 |
| bridge dropped_telemetry | 上升即意味着影子停更 |
| NATS `nats server report jetstream` | 存储使用 > 85% |

**止血**

1. 磁盘满：先扩容或删最老的 stream 数据（`nats stream purge IOT_UP --seq <保留最近 24h 的起始 seq>`）。72 小时保留是容灾窗口，缩到 24 小时是可接受的临时降级，登记。
2. bridge 磁盘缓冲快满：给 bridge 挂更大的卷；不要删缓冲文件，里面是安全事件。
3. alarm-svc 此时收不到事件。**通知运营：告警链路降级中**，设备端侧闭环仍在工作，但 App 推送会延迟。

**恢复**：JetStream 恢复后 bridge 自动重放缓冲（replayed 计数上升）。重放的事件带原始 ts，alarm-svc 会建告警并推送，用户会收到「迟到的告警」。运营需提前准备文案。

**根治**：JetStream 存储告警在 70%；按 cell 拆 stream 分散容量；bridge 缓冲上限与告警；bridge 缓冲用独立卷。

**演练**：`docker stop nats` 60 秒，模拟器持续跑，观察 buffer_len 增长与恢复后的 replayed。

---

### INC-06 TDengine 写失败

**触发**：TDengine 节点故障、taosAdapter 超时、子表数超上限、磁盘满。

**传播**：pipeline 整批 Nak（NakDelay 2 s）→ 30 秒 AckWait 后重投 → 同一批反复失败 → NumPending 增长 → 但 **影子照常写**（影子写在 TDengine 之后但失败不 Nak），事件走 handleEvent 单独 Nak。安全告警不受影响（alarm-svc 独立消费组）。

**检测信号**：pipeline nak 计数每分钟增量 > 10；NumPending 持续上升；rows_written 停止增长。

**止血**

1. 这是 S3。先确认 alarm consumer pending 正常，安全链路无恙。
2. 看 TDengine 错误：如果是单 SQL 过大或子表建表失败，是 poison 类问题，先把该 cell 的 BatchSize 调小。
3. 如果 TDengine 短期无法恢复，pipeline 继续 Nak 即可，JetStream 72 小时保留兜底。不要为了消 lag 把消息 Ack 丢掉。

**恢复**：TDengine 恢复后 pipeline 自动追赶。追赶期间 lag 看板会告警，属预期。

**根治**：TDengine 多副本；pipeline 对 TDengine 的写超时（FlushTimeout 20 s）小于 AckWait 30 s 已保证不会重复处理同一批；子表数监控。

---

### INC-07 Redis 故障

**触发**：Redis 主从切换、内存满被驱逐、网络分区。

**传播**：四个依赖各自的降级行为不同，这是关键：

| 用途 | Redis 故障时 | 后果 |
|---|---|---|
| dedupe | 出错放行 + 计数 | TDengine 可能出重复行（同 ts 覆盖，实际影响小） |
| shadow reported | 写失败计数，不 Nak | App 影子停更，用户看到「离线」 |
| device 维表富化 | 缺失计数，用信封里的 pk | fw_version / region 标签为空 |
| cmdres | Set 失败 Nak | 指令结果延迟 |
| alarm squelch | 出错放行 | 可能重复告警（宁重复不漏报） |
| CF001 sn:seq | INCR 失败 | **产线停线**，见 INC-22 |
| conn-gate 集中令牌桶（生产） | 必须 fail-open 到本地桶 | 否则全部拒连 |

**检测信号**：dedupe_redis_err、shadow_err、enrich_err 同时上升。

**止血**：Redis 切换通常 30 秒内完成。期间不动服务。切换后确认 `redis-cli info replication`。如果是内存满：先看 `redis-cli --bigkeys`，dedupe 键 TTL 600 s 应自动清理，若 device:{sn} 维表过多则是容量规划问题。

**恢复**：影子在下一条 telemetry 到达后自动更新（作业态 5 s，空闲 60 s）。

**根治**：dedupe 与 shadow 分实例；conn-gate 集中桶 fail-open 到本地桶必须有测试；CF001 sn:seq 用 PG 序列兜底。

---

### INC-08 毒消息激增

**触发**：新固件上报了不兼容的字段类型（如 progress 从 int 变成 string），或 schema_version 升级但 thing_model 未同步发布。

**传播**：ParseAndValidate 失败 → Ack 丢弃 + poison 计数 → **该固件版本的所有遥测静默丢失** → 影子停更 → 用户看到设备离线但机器正常。

**检测信号**：poison / consumed > 1%；按 fw_version 切片的 rows_written 中某版本归零。

**止血**：poison 消息已被 Ack，无法从 JetStream 追回（除非 72 小时内重放）。先定位是哪个固件版本，暂停该版本的 OTA 批次（`POST /ota/batches/{id}/pause`），防止扩散。

**恢复**：发布兼容的 thing_model 版本；或 hotfix pipeline 解析器放宽类型；从 JetStream 重放该时间段（新 durable consumer，by_start_time），dedupe 会放行因为原消息未写 dedupe 键。

**根治**：poison 消息落死信 subject `iot.dlq.>` 而不是直接丢；OTA 前置校验：新固件在模拟器上跑一轮 smoke 验证解析；未知字段透传落明细的原则要覆盖到类型宽松。

---

### INC-09 消费 lag 超 30 s

**触发**：设备增长后消息量超过单 flusher 吞吐；或某 cell 的设备分布不均。

**传播**：NumPending 增长 → 影子新鲜度 SLO 破 → 指令回执（cmd_ack 也经 pipeline）延迟 → 用户看到「未知结果」。

**检测信号**：NumPending 持续增长；批大小分布中 500 条触发占比 > 80%（说明消费端吃满）；MaxAckPending 5000 触顶。

**止血**：`IOT_FLUSHERS=4` 重启 pipeline（pull consumer 重启不丢消息）。多副本部署时增加副本，同一 durable 的多个 pull 实例自动分摊。

**根治**：flusher 数按 cell 消息量自动调；心跳间隔 OTA 调参是容量应急阀门（60 s → 120 s 空闲流量减半）；cell 数从 2 扩到 N 需要 rehash，见 §15 单元化。

---

## 4. 安全告警链路

### INC-10 火焰事件漏告警（S1）

这是全系统最不能发生的事故。端侧已停机，但用户不知道，回家发现机器停了却没收到任何通知，信任崩塌。

**触发路径推演**

| 环节 | 故障 | 概率 |
|---|---|---|
| 设备 → EMQX | 设备断网 | 高，但端侧已停机，属设计内 |
| EMQX → bridge | bridge 全部副本挂 | 低 |
| bridge → JetStream | JetStream 挂 | 中，见 INC-05，有磁盘缓冲 |
| JetStream → alarm-svc | alarm consumer 挂、或 pending 堆积 | 中 |
| alarm-svc 处理 | squelch 误吞、PG 写失败、状态机 bug | 中 |
| 推送 | APNs / FCM 供应商故障、token 失效 | 中 |
| App | 用户关了通知权限 | 高，产品层解决 |

**检测信号**（必须多路冗余，任一触发即 S1）

1. alarm consumer NumPending > 10 持续 1 分钟。
2. 对账任务：每 5 分钟比对 TDengine `events WHERE code IN (SafetyCodes)` 与 PG `alarm` 按 (sn, event_ts) 匹配，缺失 > 0 即告警。
3. 合成探针：staging 与生产各常驻 1 台探针模拟器，每 10 分钟发一条 FLAME_DETECTED，测端到端推送到达时间，> 3 s 告警，未到达 S1。
4. 推送供应商回执失败率 > 5%。

**止血（5 分钟内）**

1. 看 alarm-svc 是否存活、consumer 是否有 pending。挂了先拉起，pending 会自动追。
2. 推送供应商挂：切备用通道（短信直接升级，跳过 10 分钟等待）。原型中升级是日志模拟，生产必须有独立于推送的第二通道。
3. squelch 误吞：`redis-cli keys 'alarm:squelch:*' | wc -l` 看数量是否异常；如果是 squelch 逻辑 bug，`redis-cli --scan --pattern 'alarm:squelch:*' | xargs redis-cli del` 清空，代价是可能重复告警。
4. **通知运营与客服**：告警链路降级，准备主动外呼受影响用户（从 events 表拉最近 1 小时安全事件 SN 列表 → device_binding → PII 平台取联系方式）。

**恢复**：对账任务补齐缺失告警并推送。迟到告警文案说明「设备已于 HH:MM 自动停机」。

**根治**

- 合成探针与对账任务是 P0 必须上线的，不是可选。
- alarm-svc 至少 2 副本跨可用区。
- 推送双通道；critical 告警首次推送与短信并行而不是串行等 10 分钟（产品决策，需业务方确认费用）。

**演练**：每月一次，随机 kill alarm-svc 一个副本 + 模拟器发 FLAME_DETECTED，验证探针告警与对账补齐。

---

### INC-11 误报风暴

**触发**：一批温度传感器出厂偏差，同一机型上万台在环境温度 30 ℃ 时触发 OVER_TEMP。端侧停机 → 用户任务中断 → 事件上云 → 告警推送 → 10 分钟未确认 → 短信升级。

**传播**：用户投诉；短信费用暴涨；客服被打爆；用户关闭联网。

**检测信号**：alarm 表每分钟新增 > 100；同 code 同 product_key 占比 > 80%；同 fw_version 集中。

**止血**

1. 确认是误报而不是真实批次危险：抽 10 台看 temp_cavity 曲线，若温度平稳在阈值附近即误报。**不能确认前按真实处理**。
2. 暂停该 code 该机型的短信升级（登记）：原型无此开关，生产 alarm-svc 需支持按 (product_key, code) 的升级策略配置。推送保留，因为设备确实停了，用户需要知道。
3. 推送文案改为「检测到温度偏高，已自动暂停，可能是传感器校准问题，请查看机器后继续」。

**恢复**：OTA 下发阈值调参（端侧规则阈值可 OTA 调整，DR-04 / FR-08 设计）。走正常灰度，不跳档。

**根治**：端侧阈值带出厂校准偏移；告警按 (product_key, code) 有速率上限，超过自动转「批次事件」通知产品与固件团队而非逐台短信；FR-28 错误码集中爆发告警提前到 P0。

---

### INC-12 升级短信异常

**触发 A 重复发送**：alarm-svc 多副本各自跑升级循环（EscalationInterval 30 s），条件 UPDATE 未做幂等 → 同一告警多条短信。
**触发 B 不发送**：升级循环因 PG 慢查询卡住；或 acked 状态误判。

**检测信号**：短信发送数 vs `alarm WHERE escalated_at IS NOT NULL` 行数不等；notified 且 `now() - notified_at > 11 min` 且 escalated_at 为空的行数 > 0。

**止血**：A：升级只由一个副本执行（leader 选举或 PG advisory lock），临时缩到 1 副本。B：`SELECT id FROM alarm WHERE status='notified' AND notified_at < now()-interval '10 min' AND escalated_at IS NULL`，人工触发升级。

**根治**：升级用 `UPDATE ... SET escalated_at=now() WHERE id=$1 AND escalated_at IS NULL RETURNING id`，affected=1 才发短信，天然幂等。原型 EscalateOnce 已按此设计，生产复核多副本下的行为。

---

## 5. 指令链路

### INC-13 误停机

**触发**：客服在工单系统输错 SN 对无关设备下 stop；或 P1 的 Agent 逻辑 bug 批量下发。

**传播**：用户正在作业的机器停了，材料报废，投诉。

**检测信号**：cmd_audit 中 source=support 或 agent 的每分钟数 > 10；同一 operator 对多个 SN 短时间下发。

**止血**

1. 停止来源：吊销该客服账号的指令权限或下线 Agent。
2. 从 cmd_audit 拉出受影响 SN 与时间，通知客服主动联系用户。

```sql
SELECT cmd_id, sn, action, operator, created_at FROM iot_shard.cmd_audit
 WHERE source IN ('support','agent') AND created_at > now()-interval '30 min' ORDER BY created_at;
```

**根治**：客服指令需用户 App 确认授权（FR-20，P1 提前到 P0 评估）；Agent 每分钟指令数上限；stop 对作业态设备需二次确认。

---

### INC-14 指令回执丢失

**触发**：设备执行了 pause 并回 cmd_ack，但 pipeline lag（INC-09）或 Redis 故障（INC-07）导致 cmdres:{cmd_id} 未写或 1 小时 TTL 过期。

**传播**：用户看到「未知结果，建议现场确认」，但机器实际已暂停。用户以为没生效再按一次，第二次 pause 对已暂停设备返回 fail。

**检测信号**：`cmd_audit WHERE acked_at IS NULL AND created_at < now()-interval '1 min'` 占比 > 10%。

**止血**：修复上游（INC-07 / INC-09）。用户侧：App 提示中加「查看设备影子 work_state」，影子 3 暂停即可确认。

**根治**：cmd_ack 写 cmdres 与 cmd_audit.acked_at 同时进行，PG 是权威、Redis 是缓存；GET /cmds 先查 Redis 再回退 PG。

---

### INC-15 deviceapi 被刷

**触发**：App 版本 bug 导致影子轮询无退避（每 100 ms 一次）；或恶意扫描 SN。

**传播**：deviceapi 进程内限流 429（按 IP，ipLimit）→ 多副本下上限放大 N 倍 → PG 连接池被 telemetry 查询占满 → auth-svc 若共用 PG 则进入 INC-02。

**检测信号**：deviceapi 429 比例 > 5%；PG active connections 接近上限；单 IP 请求数 top 10。

**止血**：网关层按 user token 限流（deviceapi 的 IP 限流只是最后一道）；紧急时对 `GET /telemetry` 单独降级返回 503，保留 shadow / cmd。

**根治**：限流维度用 user_id + path（认证之后），不用 IP（NAT 误伤，见事故复盘）；auth 与 deviceapi 分 PG 连接池甚至分实例；App 轮询改为影子推送。

---

## 6. OTA 链路

### INC-16 刷坏一批固件且熔断未触发（S1）

**触发路径推演**

| 情形 | 为什么熔断没触发 |
|---|---|
| A | 0.1% 档只有 30 台，样本 < MinFuseSamples 50，永不熔断；坏固件在 30 台上全部失败但批次 status 仍 running，运营 advance 到 1% |
| B | 设备刷完重启后砖了，**根本没机会上报 failed**；云端看到的是 downloading 后无终态，ok / fail 都不增长 |
| C | 批次在 0.1% 档 fused 后运营 resume 并直接 advance；resume 是 fused 的唯一出口，但 resume 后 fail_count 不清零，Advance 会因比例超阈值拒绝，若运营改用 CreateBatch 直接建下一档则绕过了这道检查 |
| D | fail_ratio_fuse 被建批次时设成 0.5 |

**检测信号**（任一）

- 批次任务中 `status IN ('downloading','verifying')` 超过 30 分钟未变的比例 > 5%（情形 B 的唯一信号）。
- rolled_back 计数 > 0 立即告警（不等比例）。
- 批次内设备 last_online_at 在 OTA 下发后 30 分钟内未更新的比例 > 5%。
- Advance 时当前档 target_total < 50 且有任何 failed，人工复核。

**止血（5 分钟内）**

```bash
curl -XPOST :8086/api/v1/ota/batches/<id>/pause     # 立即停止下发，DispatchOnce 每 chunk 复查状态
curl :8086/api/v1/ota/batches/<id>                    # 看 target_total / ok / fail / 任务直方图
```

然后拉出该批次全部 SN 与当前 phase，交客服准备联系。

**恢复**

- 有 A/B 回滚的设备自动回到旧版本，可正常作业。确认后该固件 status 置 withdrawn。
- 无法启动的设备（情形 B）：召回或上门刷机。这是 DR-03 红线存在的原因。

**根治**

- MinFuseSamples 之外增加**绝对数熔断**：任何档位 failed + rolled_back ≥ 5 台即 fused。
- 「下发后 30 分钟未上报终态」计为 fail（stale 判定），这是情形 B 的解药。
- rolled_back 已计入 fail_count（原型 consumer 对所有非 success 终态 failInc=1），保持并加单测锁定。
- fail_ratio_fuse 建批次上限 0.05，超过需 approved_by。
- 0.1% 档强制先在内部机（office dogfood 清单）跑完再到外部用户。

**演练**：`device-simulator -n 200 -ota-fail-rate 0.3` + 建 1% 批次，验证 fused；再加模拟器「下载后静默」模式验证 stale 判定（需新增 flag）。

---

### INC-17 灰度范围失控

**触发**：运营误建 stage=50 而不是 0.1（API 接受任何合法档位，不强制顺序）；或 InStage 抽样代码被改成按 target_total 随机。

**检测信号**：新批次 target_total 与 stage × 该 product_key 设备总数不符；同一固件存在跳档的批次序列（0.1 → 50）。

**止血**：pause。已下发的设备无法撤回，只能靠 idle_only 延迟与 A/B 兜底。

**根治**：CreateBatch 强制档位顺序：同一 firmware_id 的批次 stage 必须是上一个批次的 NextStage，第一批只能是 0.1（Advance 已这样做，CreateBatch 直接建应受同样约束）；PG trigger 校验。

---

### INC-18 CDN 故障导致误熔断

**触发**：CDN 区域节点故障，设备下载失败上报 failed，error_code=DOWNLOAD_TIMEOUT，50 台样本后 fail_ratio > 2% → fused。

**传播**：熔断是正确的保护，但原因是 CDN 不是固件。fused 无自动恢复，OTA 全量周期 SLO 受影响。

**检测信号**：fused 批次的 failed 任务 error_code 分布 100% 为下载类。

**止血**：确认 CDN 状态；修复后 `POST /ota/batches/<id>/resume`（这是 fused 的唯一出口，人工）；失败任务重置为 pending 重新下发（retry 计数 +1）。

**根治**：下载类错误单独计数，不计入固件熔断比例但触发 CDN 告警；设备侧下载失败先重试 3 次（退避）再上报 failed。

---

### INC-19 作业中升级

**触发**：设备固件的 idle_only 判断有 bug（如预热态 work_state=1 被当成空闲）；或 ota 指令 policy 字段被固件忽略。

**传播**：作业中重启 → 任务失败 → 材料报废 → JOB_FAIL 事件 → 用户投诉。这是 FR-21「作业中绝不升级」的直接违反。

**检测信号**：同 SN 在 OTA downloading → success 时间窗内出现 JOB_FAIL 或 work_state 从 2 直接跳到 5。

**止血**：pause 该批次。

**根治**：云端下发前也检查影子 work_state，仅对 0 空闲的设备下发（ota-svc pendingTasks 增加影子过滤）；固件 idle_only 单测覆盖全部 work_state。

---

## 7. 隐私合规

### INC-20 未 opt-in 画面落云存储（S1）

**触发**：快照 RRPC 实现时为了调试把预签名 URL 的对象保留了；或 desired camera_cloud_optin 下发失败但云端按 App 显示状态放开了接收。

**检测信号**：对象存储写入审计：每个写入对象的 SN 关联 device 的 camera_cloud_optin（影子 reported，不是 desired）为 false 即告警。每日对账。

**止血**：立即停止接收（关闭上传端点）；列出违规对象清单并删除；保留删除日志供法务。

**根治**：判定依据必须是设备**回报**的 reported.camera_cloud_optin，不是 App 侧开关；对象存储桶策略默认 24 小时生命周期兜底；快照走内存中转不写对象存储（FR-15 设计）。

---

### INC-21 删除传导失败

**触发**：PII 平台删除事件发出，设备云消费失败（网络、schema 变更）且无重试；30 天后审计发现该用户 SN 仍可关联。

**检测信号**：传导任务失败计数；每日对账：PII 平台已删除的 user_id 在 device_binding 中 unbound_at IS NULL 的行数。

**止血**：手动执行传导：绑定 unbound、对象存储按 SN 删除、导出请求取消。

**根治**：传导任务幂等 + 死信 + 每日对账；72 小时 SLA 提前在 48 小时告警。

---

## 8. 产线与激活

### INC-22 产线停线（S1）

**触发**：cf001-svc 不可用；其依赖 PG（cf001 schema）或 Redis（sn:seq）不可用；或密钥文件丢失导致服务用内存临时密钥启动，签名与已出厂设备不一致。

**传播**：产线每台 < 2 s 节拍，/sign 失败即停线。停线每分钟都是钱。历史事故正是产线停线 30 分钟。

**检测信号**：/sign 5xx 或超时 > 3 次即产线告警（产线工位本地告警，不依赖云端监控）。

**止血（10 分钟内）**

1. 服务挂：拉起。cf001-svc 无状态，多副本。
2. Redis 挂：sn:seq INCR 失败。**产线预留离线 SN 段**：每天开工前从云端预取 1000 个 SN 流水号段到工位本地，Redis 故障时用本地段继续签发，恢复后同步。这是产线侧必须实现的。
3. 密钥丢失：服务启动日志有 WARN「generated in-memory key」即立即停止对外服务，从 KMS 恢复密钥。用错误密钥签发的 SN 全部作废重签。
4. PG 挂：配额事务无法执行。允许产线**记账后补**：工位本地记录 digest 与分配的 SN，恢复后批量补 /sign（幂等，同 digest 返回既有 SN 不耗配额，但补录要走配额扣减）。

**根治**：cf001 依赖与设备云主链路完全隔离（独立 PG、独立 Redis）；密钥从 KMS 加载失败时拒绝启动而非生成临时密钥（原型行为是开发便利，生产必须改）；产线离线 SN 段与记账后补流程写进产线 SOP。

---

### INC-23 配额异常

**触发 A 超发**：并发 bug 绕过单条 UPDATE 行锁。实际上 ck_quota_not_exceeded CHECK 是第二道护栏，超发会被库拒绝，因此更可能表现为大量 11010 而非真超发。
**触发 B 被吃光**：供应商或泄漏的载荷被批量重放，/sign 按 IP 10 次/分钟，来自多 IP 即可绕过。

**检测信号**：registered 增速远超产线节拍（如每分钟 > 60）；同一 order_no 的 /sign 来源 IP 数 > 5。

**止血**：B：把该 order_no 的 status 置 2（已完成）暂停签发；分析 digest_maps 中该时段的 mcu_sn / mac 是否在供应商提供的合法清单内，不在的作废。

**根治**：/sign 校验 timestamp 新鲜度（5 分钟）与 nonce 去重（原型未实现，生产必须）；供应商预先上传硬件清单，digest 对应的 mcu_sn 必须在清单内。

---

## 9. 运维

### INC-24 月初 cmd_audit 分区缺失

**触发**：cmd_audit 按月 RANGE 分区，DDL 脚本只预建当月与下月。滚动建分区的定时任务未部署或失败，两个月后的 1 日 00:00 起所有 INSERT 报 no partition of relation。

**传播**：审计消费者 InsertAudit 失败 → NakWithDelay 2 s 反复重投 → IOT_CMD 流 pending 持续增长 → 审计落库停止但指令本身不受影响（deviceapi 先发 down/{sn}/cmd 再发审计，返回 OK 不依赖审计）。IOT_CMD 保留 30 天，分区补上后审计全部追回。

**另一条相关风险**：deviceapi 向 JetStream 发布审计消息是尽力而为，发布失败只记 ERROR 日志，指令照常返回 OK。JetStream 不可用期间（INC-05）的指令**审计会静默丢失**，与 FR-19「审计 100% 落库」冲突。生产必须改为：审计发布失败则指令接口返回 5xx 且不下发，或先落 PG 再下发。

**检测信号**：PG 日志 `no partition of relation "cmd_audit" found for row`；`nats consumer info IOT_CMD cmd-audit` pending 持续增长；deviceapi 日志 publish audit ERROR。**预防性检查**：每天检查未来 60 天分区是否存在。

**止血**

```sql
CREATE TABLE IF NOT EXISTS iot_shard.cmd_audit_YYYYMM PARTITION OF iot_shard.cmd_audit
  FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM+1-01');
```

审计消费者是 JetStream IOT_CMD 的消费者（30 天保留），分区建好后自动追上，审计不丢。

**根治**：pg_partman 或定时任务提前建 3 个月；每日探针 INSERT 一条 60 天后的测试行再删除；DEFAULT 分区兜底（写入后再迁移）。

---

### INC-25 单元整体故障

完整六步与通过条件见 docs/drill-runbook.md。这里只列决策点：

| 决策 | 判断依据 |
|---|---|
| 是否 drain | 该 cell 的 EMQX 或 conn-gate 10 分钟内无法恢复 |
| 是否 bump cell_map_ver | drain 后 5 分钟设备仍回原 cell（固件缓存了接入点） |
| 是否重放重建影子 | 故障期间 > 1 小时，或 Redis 同时受损 |
| 何时回切 | 原 cell 健康 24 小时后，低峰期，先置 standby 再 active |

---

### INC-26 EMQX 连接数触顶

**触发**：商业 license 连接数上限；容器 nofile 1024（压测已踩过）；单节点连接数超规划。

**传播**：EMQX 拒绝新连接 → conn-gate dial 成功但 MQTT CONNECT 被拒 → 设备退避 → 但 conn-gate 的 accepted 计数正常，**熔断不会打开**（dial 成功了）→ 看板上一切正常，只有 online 不涨。

**检测信号**：EMQX 自身 connections 指标接近 license 上限；auth-svc 请求数与 conn-gate accepted 不匹配（EMQX 在 auth 前就拒了）。

**止血**：临时提高 license（供应商紧急通道）或扩节点；调 conn-gate 令牌桶到 EMQX 剩余容量让设备退避而不是反复撞。

**根治**：license 用量 80% 告警；compose / k8s 的 ulimit 明确配置并在启动时自检 `ulimit -n`。

---

### INC-27 时钟漂移

**触发**：设备 RTC 无电池或未同步 NTP，上报 ts 偏差数小时甚至年份错误。

**传播**：TDengine 按 ts 写入，错误 ts 的行落到「过去」或「未来」；telemetry_1h 流窗口错乱；安全事件的 event_ts 错误导致触达 P99 统计失真；SN 日期用服务端 UTC 不受影响。

**检测信号**：`abs(recv_ts - payload.ts)` 分布：> 5 分钟的比例 > 1%。

**止血**：无需紧急动作。统计口径改用 recv_ts 纠偏（方案 §2.2 已定义）。

**根治**：设备连接后云端下发时间（desired 或 CONNECT 响应）；ts 偏差 > 1 小时的消息用 recv_ts 替代并打标签。

---

## 10. 临时关闭保护登记表

任何一道护栏被临时关闭或放宽，**在关闭的同一分钟**在此登记并设到点提醒。到点未恢复自动升级为 S2 事故。

| 时间 | 事故 ID | 关闭 / 放宽的保护 | 原值 → 临时值 | 操作人 | 计划恢复时间 | 实际恢复时间 |
|---|---|---|---|---|---|---|
| | | | | | | |

模板：conn-gate 令牌桶 IOT_GATE_RPS · auth 缓存兜底 fail-open · JetStream 保留 72h → 24h · alarm squelch 清空 · 升级短信按 code 暂停 · OTA fail_ratio_fuse · cf001 IP 限流。

---

## 11. 演练计划

| 季度 | 演练 | 剧本 | 对应事故 |
|---|---|---|---|
| Q+0（P0 出口前） | 三剧本压测 | loadgen conn / storm / flood | INC-01 INC-09 |
| Q+0 | 单元 kill | drill-runbook 六步 | INC-25 |
| Q+0 | 刷坏固件 | simulator -ota-fail-rate 0.3 + 1% 批次 | INC-16 |
| Q+0 | 火焰漏告警 | kill alarm-svc + FLAME_DETECTED，验证探针与对账 | INC-10 |
| Q+1 | 依赖逐个 kill | 依次停 nats / tdengine / redis / postgres 各 60 s | INC-05 06 07 02 |
| Q+1 | 产线断云 | 停 cf001 依赖，验证离线 SN 段与记账后补 | INC-22 |
| Q+1 | 误报风暴 | 500 台模拟器同时 OVER_TEMP | INC-11 |
| Q+2 | 月初分区 | 修改系统时间到下下月 1 日 | INC-24 |
| Q+2 | 证书批量误吊销 | staging 吊销 100 台后回滚 | INC-03 |
| 每月 | 合成探针巡检 | 检查探针告警与推送延迟趋势 | INC-10 |

每次演练产出：时间线、每个检测信号是否在预期时间内触发、止血动作耗时、与本文的差异。差异回写本文。

---

## 12. P0 出口前必须补齐的能力

推演暴露出原型尚未具备、但生产上线前必须有的能力：

| 能力 | 对应事故 | 优先级 | 状态（2026-09-18） |
|---|---|---|---|
| 安全事件合成探针 + 事件与告警对账任务 | INC-10 | P0 | 已实现：alarm-svc Reconciler 每 5 min 比对 TDengine events 与 PG alarm 补录；合成探针 cmd/probe 经真实 MQTT 链路每 10 min 一发，实测端到端 18 ms，杀掉 alarm-svc 即报 S1 并退出码 1 |
| OTA stale 判定（下发后 30 分钟无终态计 fail）+ 绝对数熔断 | INC-16 | P0 | 已实现：ota-svc SweepStale（IOT_OTA_STALE_AFTER 30m）与 ShouldFuseAbs（IOT_OTA_MIN_ABS_FAIL 5） |
| CreateBatch 强制档位顺序 | INC-17 | P0 | 已实现：ExpectedNextStage，越级 409，最新批次 FOR UPDATE |
| poison 消息落死信而非丢弃 | INC-08 | P0 | 已实现：IOT_DLQ 流，`iot.dlq.<kind>`，发布失败 Nak |
| auth-svc 本地缓存 fail-open 兜底（登记制） | INC-02 | P0 | 已实现：IOT_AUTH_FAILOPEN 默认关，仅覆盖 Store 出错，/metrics 暴露 failopen_allowed |
| 产线离线 SN 段与记账后补 SOP | INC-22 | P0 | 未做：产线侧流程，不在本仓库 |
| cf001 nonce 新鲜度与去重、密钥加载失败拒绝启动 | INC-22 INC-23 | P0 | 已实现：CheckFreshness 5m 窗口、Redis SETNX 去重 fail-closed、IOT_CF001_REQUIRE_KEY |
| cmd_audit 分区滚动任务与探针 | INC-24 | P0 | 已实现：ensure_cmd_audit_partitions 函数 + deviceapi 启动与每日调用；探针未做 |
| 审计发布失败时指令拒绝下发（审计先于指令） | INC-24 INC-05 | P0 | 已实现：503 / 100013，指令不下发 |
| 推送双通道，critical 首推与短信策略由业务方定 | INC-10 | P0 决策 | 待业务决策 |
| 按 (product_key, code) 的告警速率上限与升级开关 | INC-11 | P1 | 未做 |
| 下载类错误不计入固件熔断 | INC-18 | P1 | 未做 |
| 云端下发 OTA 前检查影子 work_state | INC-19 | P1 | 未做 |
| conn-gate 半开熔断、超时不计连续失败 | INC-04 | P1 | 未做 |
| 对象存储写入按 reported opt-in 审计 | INC-20 | P0 | 未做：原型无对象存储 |

---

*本文与 docs/tech-design-aiot-platform.md §15.3 依赖故障矩阵互为补充：那里回答「组件挂了系统怎么表现」，这里回答「人在前 15 分钟做什么」。每次真实事故后按 docs/incident-register-storm.md 的格式复盘，并把新教训回写到对应 INC 条目。*
