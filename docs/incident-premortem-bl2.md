# BL2 配件与安全生态 · 线上事故预推演与处置方案

> Incident Pre-mortem · 对应 docs/prd-bl2-accessory-ecosystem.md 与 docs/tech-design-bl2-accessory-ecosystem.md

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1 · 2026-09-18 |
| 目的 | BL2 上线前沿配对、云端联动、本地兜底、安全事件、滤芯寿命、配件 OTA 六条线推演最可能的线上事故，每条给触发、传播、检测信号、止血、恢复、根治、演练 |
| 与其它预推演的关系 | docs/incident-premortem-bl1.md 的 27 条对配件作为设备同样全部适用（接入、数据、告警、OTA）；docs/incident-premortem-bl4.md 的入口与授权类事故对配对界面适用。本文只写 BL2 新增的 20 条 |
| 编号 | INC-2-xx |

**BL2 事故的三个特征**

1. **物理后果**。净化器该开没开是烟雾，该关没关是耗材；灭火器事件漏报是安全事故。事故不止是数据错。
2. **两条路径互为备份，也互为干扰**。云端联动与本地兜底并存，多数事故形态是「一条路径错了，另一条没能兜住」或「两条路径打架」。
3. **配对是新的授权边界**。配对错了，一台主机能控制别人的净化器。

---

## 1. 事故清单总览

| ID | 事故 | 线 | 级别 | 第一检测信号 | 止血时限 |
|---|---|---|---|---|---|
| INC-2-01 | 规则引擎挂或 lag，云端联动全部失效 | 联动 | S3 | consumer pending 增长；trigger_source=local 比例升 | 30 min |
| INC-2-02 | 联动延迟超 3 s：Redis 配对缓存 miss 回源慢或 deviceapi 慢 | 联动 | S3 | 延迟 P99 > 3 s | 30 min |
| INC-2-03 | 云端建议关被固件错误采纳，作业中净化器停转 | 联动 | **S1** | 主机 work_state=2 期间净化器 power_on=false 的对账 | 5 min |
| INC-2-04 | 延时关闭定时器丢失，净化器长开耗滤芯 | 联动 | S3 | 主机空闲 > off_delay 且净化器仍开的设备数 | 1 h |
| INC-2-05 | 联动风暴：主机 work_state 抖动导致每秒下发 desired | 联动 | S2 | linkage_triggered 速率；deviceapi desired 写入量 | 15 min |
| INC-2-06 | 配对错误：净化器被配到别人的主机 | 配对 | **S1** | 配对双方 owner 不一致的对账 | 立即 |
| INC-2-07 | 灭火事件未推送或未关联主机 | 安全 | **S1** | BL1 INC-10 对账探针；alarm_context 缺失 | 5 min |
| INC-2-08 | 烟雾误报风暴：切割烟雾触发 SMOKE_HIGH | 安全 | S2 | 同码同机型每分钟 > 100 | 15 min |
| INC-2-09 | 灭火事件误触发主机 stop 打断正常作业 | 安全 | S2 | cmd_audit operator=accessory-svc 的 stop 数 | 5 min |
| INC-2-10 | 本地兜底失效：作业信号未广播或净化器未响应，云也不可用时净化器不开 | 兜底 | **S1** | trigger_source 分布中 local 归零；断网台架 | 固件版本 |
| INC-2-11 | 本地信号被邻居主机触发 | 兜底 | S3 | 未配对主机附近的净化器 local 启动 | 固件版本 |
| INC-2-12 | 主机解绑后配对未解除，原 owner 仍控制净化器 | 配对 | S2 | unbound_at 之后 linkage_audit 仍有该 host | 5 min |
| INC-2-13 | 材料档位映射发布错误，全量净化器档位错 | 联动 | S2 | 发布后档位分布突变 | 5 min |
| INC-2-14 | 滤芯寿命系数错误导致全量提前或延后提醒 | 滤芯 | S2 | health 分布突变；提醒推送量突增 | 1 h |
| INC-2-15 | 滤芯更换未识别，health 不回 100% | 滤芯 | S3 | FILTER_REPLACED 后 health 未变 | 1 d |
| INC-2-16 | 滤芯批漏跑或重跑，累计风量翻倍 | 滤芯 | S3 | last_hour 不连续；批 runs 计数 | 1 d |
| INC-2-17 | 配件 OTA 在主机作业中升级，排烟中断 | OTA | S2 | 配件 OTA_PROGRESS 与主机 work_state=2 重叠 | 15 min |
| INC-2-18 | 配件心跳流量超预估拖慢管道 | 数据 | S3 | pipeline lag；ACC 心跳占比 | 30 min |
| INC-2-19 | 安全排烟规则卡死，净化器最大档不回落 | 联动 | S3 | fan_level=4 超 10 min 的设备数 | 1 h |
| INC-2-20 | linkage_audit 分区缺失，审计落库停止 | 运维 | S3 | PG no partition；审计 lag | 1 h |

---

## 2. 云端联动

### INC-2-01 规则引擎失效

**触发**：accessory-svc 全部副本挂；JetStream 消费组 pending 堆积；Redis 不可用导致每条都回源 PG 变慢。

**传播**：云端联动停止 → 净化器依赖本地兜底启停（默认档，不按材料调档）→ 用户感知「档位不对」但排烟仍在。延时关闭也停止，本地信号消失后净化器由固件自行延时关。

**检测信号**：`nats consumer info IOT_UP accessory` pending > 100 持续 1 分钟；trigger_source=local 占比从基线（预期 < 10%）升到 > 50%；linkage_triggered 速率归零。

**止血**：拉起服务；pending 自动追。追赶期间会对已结束的作业补发过期的 desired，需**丢弃 recv_ts 早于 60 s 的事件**（规则引擎启动时按时间过滤，不重放历史联动）。

**恢复**：观察 local 比例回到基线。

**根治**：2 副本跨可用区；过期事件丢弃是必备逻辑；本地兜底的默认档按材料类型由主机广播携带（P1 固件）。

**演练**：kill accessory-svc 60 s，模拟器 `-local-signal` 验证净化器仍随主机启停。

---

### INC-2-02 联动延迟超 3 s

**触发**：配对缓存 miss 率高（TTL 太短或频繁失效）→ 每条回源 PG；deviceapi PATCH /desired 慢（PG 分片库压力）；规则引擎单副本处理串行。

**传播**：净化器晚开几秒，前几秒烟雾未被抽走。本地兜底若在则用户无感。

**检测信号**：linkage 延迟 P99 > 3 s 持续 5 分钟；缓存命中率 < 90%；deviceapi PATCH 延迟。

**止血**：配对缓存 TTL 从 60 s 调到 300 s（配对变更主动失效不受影响）；规则引擎按 host_sn 分片并行。

**根治**：三段延迟分别记录（事件到达、决策完成、desired 写入）定位慢在哪一跳；deviceapi 与规则引擎分 PG 连接池。

---

### INC-2-03 云端建议关被错误采纳（S1）

**触发**：净化器固件仲裁 bug，把云端 power_on:false 当成命令执行，不检查本地信号；或本地信号因主机固件 bug 停止广播。规则引擎误判 JOB_DONE（如 JOB_PAUSE 被当成结束）发出关闭建议。

**传播**：作业进行中净化器停转，烟雾外溢。用户投诉、健康风险。

**检测信号**：对账任务每分钟：配对中主机影子 work_state=2 且净化器 reported.power_on=false 持续 > 30 s 的配对数 > 0 即告警。

**止血（5 分钟内）**

1. 规则引擎全局开关 `IOT_ACC_ALLOW_OFF=false`：只发开启建议，不发关闭建议，登记。净化器长开耗滤芯，可接受。
2. 已停转的设备：对这些配对立即下发 power_on:true, fan_level 按当前规则。

**恢复**：固件修复仲裁逻辑后走 OTA 灰度，观察对账指标归零后恢复关闭建议。

**根治**：固件仲裁「关闭需两路径都允许」列为红线并有台架测试；云端关闭建议附带 `reason` 与 `host_work_state`，固件校验主机状态不为 2 才执行；规则引擎只在 JOB_DONE / JOB_FAIL / work_state=0 三种触发下发关闭，JOB_PAUSE 不算。

**演练**：模拟器净化器 `-ignore-arbitration` 模式（错误固件）+ 主机作业中人为发关闭 → 对账告警触发。

---

### INC-2-04 延时关闭定时器丢失

**触发**：Redis ZSET `linkage:off` 被清空或 Redis 切换丢数据；规则引擎重启期间到期项未被处理；ZPOPMIN 后进程崩溃丢失该项。

**传播**：净化器在主机空闲后长开，耗滤芯与电，用户投诉「自动关不灵」。

**检测信号**：对账：主机空闲超 off_delay_s + 60 s 且净化器 power_on=true 且 trigger_source=cloud 的配对数。

**止血**：对账任务对这些配对补发关闭建议（本身就是修复动作）。

**根治**：ZPOPMIN 改为先标记处理中再删除（两步）；对账任务常态化每 5 分钟，作为定时器的兜底；净化器固件自身有最大连续运行时长保护（如 2 h 无主机信号自动关）。

---

### INC-2-05 联动风暴

**触发**：主机固件 work_state 在 2 与 1 之间抖动（预热与作业反复）；或 JOB_START 事件重发未被幂等拦住；规则引擎按每帧 telemetry 触发而未比对影子。

**传播**：每秒对净化器下发 desired → deviceapi PG shadow_desired 版本飞涨 → 净化器频繁调档 → MQTT 下行拥塞。

**检测信号**：单 acc_sn 每分钟 linkage_triggered > 10；deviceapi PATCH /desired QPS 突增。

**止血**：规则引擎按 acc_sn 限流：同一动作 30 s 内不重复下发（目标状态相同则跳过）；对抖动主机暂时禁用联动。

**根治**：动作幂等化：下发前读净化器影子 desired，目标相同不写；work_state 触发加 10 s 去抖窗口；幂等键 `linkage:seen` 覆盖 telemetry 触发。

---

### INC-2-13 材料档位映射发布错误

**触发**：linkage_rule 新版本把亚克力映射到 1 档（应为 4 档），10 分钟内全量生效。

**传播**：切亚克力时净化器低档运行，异味投诉集中。

**检测信号**：发布后档位分布突变（fan_level=4 占比骤降）；投诉。

**止血**：复用 param-svc 回滚（product_key=ACC_PURIFIER 的 release 序列），≤ 5 min。

**根治**：映射发布走 param-svc 的差异校验与双人审批（本方案已定）；规则引擎对映射缓存带版本，回滚后 1 分钟内刷新。

---

### INC-2-19 安全排烟规则卡死

**触发**：主机安全事件触发净化器最大档 10 分钟，但 Redis `linkage:safety:{acc_sn}` TTL 未设或规则引擎未在到期后回到普通规则。

**传播**：净化器长期最大档，噪音大、滤芯消耗快。

**检测信号**：fan_level=4 且持续 > 15 分钟且主机无安全事件的设备数。

**止血**：对账任务对这些设备按普通规则重新下发。

**根治**：安全窗口用带 TTL 的 key 并在到期扫描中显式回落；净化器固件对云端 fan_level 4 有自身最大持续时长。

---

## 3. 配对

### INC-2-06 配对错误（S1）

**触发**：BFF 漏校验用户对 acc_sn 的 owner 绑定，只校验了 host_sn；或配对接口接受任意 acc_sn。攻击者或误操作把别人的净化器配到自己主机。

**传播**：我的作业让别人的净化器启停；别人的灭火事件让我的主机停机。既是隐私也是安全问题。

**检测信号**：每日对账：accessory_pairing 有效配对中 host_sn 与 acc_sn 的 owner（device_binding）不同的行数 > 0。配对接口对未绑定 acc_sn 的请求应 404。

**止血**：立即解除对账发现的错误配对；下线配对接口回滚版本。

**根治**：accessory-svc 配对接口自身也校验两台设备的 owner 一致（不只依赖 BFF，双保险）；配对需配件端确认（配件收到 desired.paired_sn 后用户在配件上按键确认，P1）；对账进每日任务。

---

### INC-2-12 解绑后配对未解除

**触发**：device_binding 的解绑走了别的路径（直接 SQL、批处理）未触发 trigger；或触发器被误删；或解绑写的是 DELETE 而非 unbound_at。

**传播**：转让后原 owner 的新主机若与该净化器仍配对，可继续控制。

**检测信号**：对账：有效配对的 host_sn 在 device_binding 中无有效绑定。

**止血**：对账脚本自动解除。

**根治**：解绑只允许经 deviceapi 接口（UPDATE unbound_at）；触发器存在性进每日巡检；对账常态化。

---

## 4. 安全事件

### INC-2-07 灭火事件未推送或未关联主机（S1）

**触发**：与 BL1 INC-10 同路径（bridge / JetStream / alarm-svc / 推送）；BL2 特有的是 alarm_context 富化失败（accessory-svc 挂、配对缓存 miss）导致告警无主机信息。

**传播**：用户不知道灭火器触发了；或知道了但不知道当时主机在做什么，事后复核困难。

**检测信号**：BL1 的合成探针与对账覆盖告警本身；alarm_context 对账：安全码告警 5 分钟后无 context 行的数量。

**止血**：告警链路按 BL1 INC-10 处置；context 缺失由对账任务补录（从主机影子历史或 job_record 回填）。

**根治**：模拟器 `-acc-event FIRE_SUPPRESSED` 加入合成探针巡检；alarm_context 富化失败 Nak 重试而不是丢弃。

---

### INC-2-08 烟雾误报风暴

**触发**：烟雾传感器阈值偏低，正常切割烟雾触发 SMOKE_HIGH；一批传感器出厂偏差。

**传播**：上万条 critical 告警 → 推送与升级短信 → 净化器全部最大档 → 用户投诉、短信费用。与 BL1 INC-11 同形态但多了「净化器最大档」的物理后果。

**检测信号**：SMOKE_HIGH 每分钟 > 100 且集中于同 fw_version；burnt / 味道投诉不匹配。

**止血**：alarm-svc 按 (product_key, code) 暂停升级短信（BL1 INC-11 的能力，P1）；推送保留；净化器最大档是合理反应，不干预。

**根治**：SMOKE_HIGH 分两档由固件判定（高档才是安全码）；阈值 OTA 调参；同码集中爆发自动转批次事件。

---

### INC-2-09 灭火事件误触发主机 stop

**触发**：FIRE_SUPPRESSED 误报（灭火器传感器故障）或配对错误（INC-2-06）→ accessory-svc 对主机下发 stop → 正常作业中断、材料报废。

**传播**：用户损失材料；若频发，用户关闭联网。

**检测信号**：cmd_audit operator=accessory-svc action=stop 的数量与 FIRE_SUPPRESSED 告警数对比；主机当时无 FLAME_DETECTED 的比例。

**止血**：`IOT_ACC_STOP_HOST_ON_FIRE=false`，登记。

**根治**：stop 只在主机自身也报了 FLAME / OVER_TEMP 或 SMOKE_HIGH 时下发（双证据）；开放问题 3 决策后可能永久关闭该规则。

---

## 5. 本地兜底

### INC-2-10 本地兜底失效（S1）

**触发**：主机固件作业信号广播 bug（某版本不发、发错端口、HMAC 错）；净化器固件接收 bug；路由器隔离组播；蓝牙距离超出。云端同时不可用（INC-2-01）时净化器完全不开。

**传播**：这是 BL2 唯一「两条路径同时失效」的事故，作业中无排烟。

**检测信号**：trigger_source=local 占比归零（云端正常时也应有少量 local，因为本地通常更快）；断网台架测试；用户投诉「云挂了净化器不动」。

**止血**：无线上止血手段，只能保证云端联动可用（提升 accessory-svc 优先级），并推送用户「请手动开启净化器」。

**根治**：本地兜底是固件红线 DR-202，出厂前台架断网测试；主机与净化器双通道（组播 + 蓝牙）；云端持续统计 local 比例作为兜底健康度。

**演练**：模拟器 `-local-signal=false`（错误固件）+ kill accessory-svc → 净化器不开 → 验证告警。

---

### INC-2-11 本地信号被邻居触发

**触发**：作业信号无 HMAC 或 pair_key 泄漏；净化器不校验 host_sn。

**传播**：邻居开机我的净化器跟着转。

**根治**：广播载荷带 host_sn + HMAC(pair_key)；净化器只响应 reported.paired_sn 且 HMAC 通过的信号；pair_key 配对时下发、解除时轮换。

---

## 6. 滤芯寿命

### INC-2-14 寿命系数错误

**触发**：filter_model 的 flow_coeff 或 rated_volume 填错单位（m³ 与 L）；压差系数过大。

**传播**：全量净化器 health 骤降 → 提醒推送量突增 → 用户提前买滤芯（或延后不买）→ 信任受损。

**检测信号**：批后 health 分布中位数变化 > 20%；≤ 20% 设备数突增；提醒推送量。

**止血**：filter_model 回滚上一版本；从 telemetry_1h 重算 eq_air_volume（批是幂等可重算的）。

**根治**：filter_model 变更走双人审批；变更后先对 1% 设备影子计算比对再全量；提醒文案给区间。

---

### INC-2-15 更换未识别

**触发**：用户换了滤芯但未在 App 确认；FILTER_REPLACED 事件丢；desired filter_reset 未回报。

**传播**：health 继续下降，反复提醒，用户烦躁。

**根治**：滤芯仓开合传感器（DR-207）产生候选并在 App 一键确认；压差骤降（换新滤芯的物理特征）作为自动识别的辅助信号。

---

### INC-2-16 滤芯批漏跑或重跑

**触发**：批进程挂了几小时后重启只算「上一小时」漏掉中间；或重跑同一小时累计翻倍。

**传播**：eq_air_volume 偏小或偏大。

**根治**：批按 filter_life.last_hour 补齐到当前的每个小时（幂等：同一小时不重复累加，用 last_hour 守卫）；单测覆盖跨多小时补算。

---

## 7. OTA 与数据

### INC-2-17 配件在主机作业中升级

**触发**：ota-svc 只检查配件自身空闲，未检查配对主机；或主机影子过期。

**传播**：升级重启期间净化器停转，作业中排烟中断。

**根治**：pendingTasks 对 ACC_* 设备加配对主机影子 work_state=0 检查；配件固件收到 OTA 指令时若本地信号活跃则延后。

---

### INC-2-18 配件心跳流量超预估

**触发**：净化器心跳按主机纪律 60 s 而非 120 s；连带率高于估算。

**检测信号**：pipeline lag；按 product_key 切片的消息速率。

**止血**：OTA 调心跳间隔到 120 s（DR-205 可调参）。

---

### INC-2-20 linkage_audit 分区缺失

同 BL1 INC-24 形态，复用 ensure 分区函数模式与每日探针。

---

## 8. 演练计划

| 阶段 | 演练 | 剧本 | 对应事故 |
|---|---|---|---|
| 迭代 1 出口 | 规则引擎 kill | kill accessory-svc + `-local-signal` 净化器随主机启停 | INC-2-01 INC-2-10 |
| 迭代 1 出口 | 错误仲裁固件 | `-ignore-arbitration` + 作业中发关闭 → 对账告警 | INC-2-03 |
| 迭代 1 出口 | 联动风暴 | 主机模拟器 work_state 每秒抖动 → 去抖与限流生效 | INC-2-05 |
| 迭代 2 出口 | 灭火链路 | `-acc-event FIRE_SUPPRESSED` → 告警 + context + 主机 stop，P99 < 3 s | INC-2-07 INC-2-09 |
| 迭代 2 出口 | 配对越权 | A 用户配对 B 的净化器 → 404；对账 | INC-2-06 |
| 迭代 3 出口 | 系数错误 | filter_model 单位错 → 分布告警 → 回滚重算 | INC-2-14 |
| 每月 | 对账巡检 | 作业中停转、长开、配对 owner 一致、解绑传导四项对账 | INC-2-03 04 06 12 |

---

## 9. 迭代 1 出口前必须补齐的能力

| 能力 | 对应事故 | 优先级 | 状态（2026-09-19） |
|---|---|---|---|
| 「作业中净化器停转」对账任务每分钟 | INC-2-03 | P0 | 已实现 ReconcileDecide + IOT_ACC_RECONCILE_INTERVAL |
| 规则引擎关闭建议全局开关 IOT_ACC_ALLOW_OFF | INC-2-03 | P0 | 已实现 AllowOff |
| 过期事件丢弃（recv_ts > 60 s 不联动） | INC-2-01 | P0 | 已实现 IsStale |
| 动作幂等（目标状态相同不下发）+ work_state 去抖 | INC-2-05 | P0 | 已实现 Decide 幂等分支 + 10 s 去抖，单测覆盖 |
| 配对接口双方 owner 一致校验 + 每日对账 | INC-2-06 INC-2-12 | P0 | 已实现：配对时 OwnersConsistent 校验 + 每日归属对账（漂移即停联动不解绑），实测同 owner 零误报 |
| 定时器两步删除 + 长开对账兜底 | INC-2-04 | P0 | 已实现 Redis ZSET 两步 + off_requeued；实机验证 off_sent / off_cancelled |
| 主机 stop 双证据规则 + 开关 | INC-2-09 | P0 | 已实现 HostStopEvidence + IOT_ACC_STOP_HOST_ON_FIRE |
| alarm_context 富化失败 Nak 重试 + 对账 | INC-2-07 | P0 | 写入已实现；失败 Nak 重试未做 |
| 本地兜底台架断网测试进固件出厂 | INC-2-10 | 固件红线 | 固件侧，不在本仓库 |
| 滤芯批按 last_hour 幂等补算 | INC-2-16 | P1 | 已实现 FilterStep |
| filter_model 双人审批与 1% 比对 | INC-2-14 | P1 | 未做 |
| 配件 OTA 检查配对主机空闲 | INC-2-17 | P1 | 未做 |
| linkage_audit 分区滚动 | INC-2-20 | P0 | 已实现 ensure_linkage_audit_partitions |

---

*本文与 docs/incident-premortem-bl1.md、docs/incident-premortem-bl4.md 配套：设备与平台侧看 BL1，入口与授权看 BL4，配对、联动、兜底、滤芯看本文。每次真实事故后按 docs/incident-register-storm.md 的格式复盘并回写对应 INC-2 条目。*
