# xtool-aiot · 工程规格与契约（所有服务必须遵守）

> 目标：把《xTool AIoT 平台技术方案》§17/§18 落成可运行原型。11 个 Go 程序 + 共享包，单机 `make dev` 跑通全链路。
> 模块路径 `github.com/xtool/xtool-aiot`，Go 1.24，只用 go.mod 里已列的依赖 + 标准库。HTTP 用 `net/http`（1.22+ 路由模式）。

## 1. 目录约定
```
cmd/<svc>/main.go            每个服务一个 main，只做装配：读 config → 建依赖 → 起 server
internal/<svc>/...           服务私有代码（handler / service / repo / 纯函数）
internal/pkg/...             共享包（见 §3），任何服务不得复制一份自己的版本
sql/                          global.sql shard.sql cf001.sql bl4.sql tdengine.sql（compose 启动自动执行；bl4.sql 为 BL4 参数库/加工记录/推荐表）
deploy/docker-compose.yml     emqx nats redis postgres tdengine
docs/                         设计与实测文档
scripts/                      make 目标调用的脚本
```
服务清单（P0，全部 cmd/ 下同名目录）：
`bootstrap-svc` `auth-svc` `conn-gate` `bridge` `pipeline` `deviceapi` `alarm-svc` `ota-svc` `cf001-svc` `device-simulator` `loadgen`

## 2. 环境变量（internal/pkg/config 统一读取，缺省值见括号）
```
IOT_NATS_URL       (nats://127.0.0.1:4222)
IOT_REDIS_ADDR     (127.0.0.1:6379)
IOT_PG_DSN         (postgres://iot:iot@127.0.0.1:5432/iot?sslmode=disable)
IOT_TD_URL         (http://127.0.0.1:6041)        TDengine REST(taosAdapter)
IOT_TD_USER/PASS   (root/taosdata)
IOT_MQTT_URL       (tcp://127.0.0.1:1883)         EMQX 明文口（原型）；8883 留给 mTLS
IOT_CELLS          (2)                            单元数 N
IOT_HTTP_ADDR      各服务自带默认端口，见 §6
IOT_IT             非空时集成测试打真实依赖，否则 skip
```

## 3. 共享包（internal/pkg）— 已由骨架提供，实现者补全 TODO 但不得改签名
- `envelope`：统一信封与 subject 规则
  ```go
  type Kind string // telemetry | event | cmd_ack | ota_progress
  type Envelope struct { PK, SN string; Kind Kind; Seq int64; RecvTs int64 /*ms*/; Payload json.RawMessage }
  func Subject(kind Kind, cell int) string   // "iot.up.<kind>.<cell>"
  func SubjectAll(kind Kind) string          // "iot.up.<kind>.*"
  const StreamUp = "IOT_UP"                  // JetStream stream，subjects iot.up.>，保留 72h
  const StreamCmd = "IOT_CMD"                // iot.cmd.>  审计流 30 天
  const SubjectCmdAudit = "iot.cmd.audit"
  const StreamDLQ = "IOT_DLQ"                // iot.dlq.>  死信流 7 天
  func SubjectDLQ(kind Kind) string          // "iot.dlq.<kind>"，未知 kind → "iot.dlq.unknown"
  ```
- `cellmap`：`CellOf(sn string, n int) int` = fnv32a(sn)%n+1（bridge 与 bootstrap 必须同一函数）
- `config`：`Env(key, def string) string`、`EnvInt`、`MustPG(ctx)`, `MustRedis()`, `MustNATS()` 返回已连接客户端；`EnsureStreams(js)` 幂等建两个 stream
- `httpx`：`JSON(w, code, v)`、`Error(w, httpStatus, bizCode int, msg string)`（响应体 `{"code":bizCode,"msg":...}`）、
  `RateLimitByKey(keyFn func(r)*string, rps float64, burst int) Middleware`（进程内令牌桶 map，超限返回 HTTP 429 + code 100012）、
  `Recover`、`Logging`
  业务码常量：`CodeOK=0 CodeRateLimited=100012 CodeNoQuota=11010 CodeBadParam=10001 CodeNotFound=10004 CodeDenied=10003 CodeAuditUnavailable=100013`
- `tdengine`：REST 客户端 `Exec(ctx, sql) error`、`Query(ctx, sql) (*Result, error)`；`BatchInsert(ctx, rows []TelemetryRow) error` 按 SN 分组生成多表多行 INSERT（`INSERT INTO iot.t_<sn> USING iot.telemetry TAGS(...) VALUES (...)(...) iot.t_<sn2> ...`），单条 SQL ≤ 900KB 自动切分
- `shadow`：Redis 影子 `Key(sn)="shadow:"+sn`、`WriteReported(ctx, pipe, sn, fields map[string]any)`、`Read(ctx, sn)`
- `dedupe`：`Seen(ctx, rdb, sn, seq) (dup bool, err error)` = SETNX dedupe:{sn}:{seq} EX 600；Redis 出错时返回 `dup=false, err`（调用方放行并计数）
- `backoff`：`Next(n int, base time.Duration) time.Duration` = min(2^n·base, 15min) + rand(0,30s)（设备端纪律；simulator 用，单测覆盖上限与抖动范围）

## 4. 消息与 Topic 契约
- 设备 → EMQX：`up/{pk}/{sn}/telemetry|event|cmd_ack|ota_progress`，payload JSON：
  ```json
  telemetry: {"seq":123,"ts":1726300000000,"work_state":2,"power_level":87,"temp_cavity":41.5,"temp_water":25.1,"fan_rpm":3000,"laser_hours":120.3,"progress":55}
             v1.1 属性可附加在任意一帧（上线第一帧与 desired 变更后的下一帧必带）：`"module_model":"LM40","job_feedback_optin":true`。
             pipeline 对**未建模的标量字段**（string/bool/number，键名 ^[a-z_][a-z0-9_]{0,31}$，≤32 个）透传写影子 reported，不入 TDengine；对象/数组丢弃。
  event:     {"seq":124,"ts":...,"code":"FLAME_DETECTED","msg":"..."}        安全码集合 SafetyCodes = {FLAME_DETECTED, OVER_TEMP, TILT, ESTOP, WATER_FLOW}
             v1.1 任务事件 JOB_START / JOB_DONE / JOB_FAIL / JOB_PAUSE 可带 `job_id`(UUIDv4) `material_id` `param_profile_id` `params_hash`(sha256 hex) 与 JOB_DONE 的 `duration_s`；
             **设备 reported.job_feedback_optin 为 false 时四字段一律不出现**（设备端第一道闸，job-svc 再校验一次）。pipeline 把四字段写入 TDengine events 新列（缺失写 NULL）。
  cmd_ack:   {"seq":125,"ts":...,"cmd_id":"...","result":"ok|fail","detail":"..."}
  ota_progress: {"seq":126,"ts":...,"batch_id":1,"phase":"notified|downloading|verifying|success|failed|rolled_back","pct":40,"error_code":""}
  ```
- 云 → 设备：`down/{sn}/cmd` `{"cmd_id","action","params"}`；`down/{sn}/desired` `{"version":N,"desired":{...}}`
- 配件（BL2）：净化器等以独立 SN / 证书走同一 Topic 与管道（product_key `ACC_*`）；telemetry 附带 `power_on / fan_level / pressure_diff / runtime_h / trigger_source` 透传进影子 reported，`fan_rpm` 进 TDengine 列；desired `{power_on, fan_level, paired_sn}` 由 accessory-svc 经 deviceapi PATCH /desired 下发（云端只发建议，关闭需固件本地允许）；配件安全码 `FIRE_SUPPRESSED / SMOKE_HIGH` 已加入 SafetyCodes 走 alarm-svc 独立通道（level critical），accessory-svc 同时写 `alarm_context` 关联主机 SN / work_state / job_id。
- bridge：共享订阅 `$share/bridge/up/#` → 信封 → JetStream `iot.up.{kind}.{cell}`，cell=CellOf(sn,N)。JetStream 不可用：telemetry 丢弃计数；event 进本地磁盘缓冲（`./data/bridge-buffer.jsonl`）定时重发。
- pipeline（durable consumer `pipeline-<cell>`，每 cell 一个）四步：解析校验 → 富化(Redis 维表 `device:{sn}` Hash: pk,fw,region,cell) → 幂等(dedupe) → 分发。
  Ack 语义：解析失败 → 包装为 `{subject,cell,error,ts,data_b64}` 发布到 `iot.dlq.<kind>`，成功 Ack（计数 dlq），发布失败 Nak（计数 dlq_publish_err）；未装配 DLQ 时退回 Ack 丢弃+计数 poison；重复 Ack；TDengine 写失败 Nak（AckWait 30s）；成功 Ack。
  **攒批**：`BATCH_SIZE=500` 或 `BATCH_WINDOW=120ms` 先到者触发；一批 = 一次 `tdengine.BatchInsert` + Redis pipelined 写影子（每 SN 只写批内最新）+ 批量 Ack；整批失败整批 Nak。flusher 数 = `IOT_FLUSHERS`（默认 1，按 cell subject 分区可并行）。
- alarm-svc：独立 durable consumer `alarm` 订 `iot.up.event.*`，只处理 SafetyCodes；squelch `alarm:squelch:{sn}:{code}` 5min；状态机 open→notified→acked→closed，全部用「带前置状态的 UPDATE」，affected=0 → 非法迁移返回 409；notified 超 10min 未 acked 且 critical → 升级(记录 escalated_at，日志模拟短信)。
- cmd：`POST /api/v1/devices/{sn}/cmd {action,params}`，白名单 pause/stop/self_check；remote_restart → 403 code 10003。cmd_id=UUIDv4；**审计先于指令**：先同步发布审计到 `iot.cmd.audit`（等 PubAck，5s），审计不可用则拒绝下发（HTTP 503 code 100013），成功后再发布 `down/{sn}/cmd`；MQTT 发布失败追加一条 result=dispatch_failed 审计（尽力）并返回 502；deviceapi 内的审计消费者落 `cmd_audit`；`GET /api/v1/cmds/{cmd_id}` 查结果（cmd_ack 事件回流由 pipeline 写 Redis `cmdres:{cmd_id}` TTL 1h）。
  **来源与授权（BL6）**：source 只由服务身份头 `X-Source`（app|console|support|agent）推导，请求体 source 忽略；`X-Operator` 覆盖 operator。support / agent 来源在白名单之后、审计之前经 grantcheck 直查 `iot_shard.support_grant`（status=granted、动作包含、未过期、未撤回），无有效 grant → 403 code 10003 并留一条 result=denied 审计；agent 只允许 self_check；Checker 缺失或 PG 出错一律拒绝（fail-closed）。放行时 grant_id 合并进审计 params。
- desired：`PATCH /api/v1/devices/{sn}/desired {k:v}` → PG `shadow_desired` 合并 JSONB、version++ (单事务 `UPDATE ... RETURNING version`，不存在则 INSERT) → 发布 `down/{sn}/desired`。`GET /api/v1/devices/{sn}/shadow` 返回 `{reported, desired, desired_version, switches}`；`switches` 对 desired 中每个布尔字段给三态 `{value, state}`：reported==desired → `applied`(value=desired)；不一致且在线（影子 updated_at 90s 内，无时间信息按在线）→ `pending`(value=reported)；不一致且离线 → `offline`(value=reported)。客户端只渲染 state，不得自行比较 reported/desired（INC-4-06）。

## 5. 数据库
- PG 单实例三个 schema 模拟三层：`iot_global`（product/thing_model/cell/firmware/firmware_delta/ota_batch）、`iot_shard`（原型只建一个分片：device/device_cert/device_binding/shadow_desired/cmd_audit(按月分区，脚本预建当月+下月)/ota_device_task/alarm/consumable_health）、`cf001`（见 §7）。DDL 见 sql/*.sql，与方案 §17 同源，**不得改约束**（`ck_full_stage_approved` CHECK、`uk_binding_active` 部分唯一索引 必须保留并有测试证明生效）。
- TDengine：sql/tdengine.sql（KEEP 90；telemetry / events 超级表；events 含 v1.1 四列 job_id/material_id/param_profile_id/params_hash，已有库靠文件内 `ALTER STABLE ... ADD COLUMN` 升级，重复执行的 code 875 "Column already exists" 视为幂等成功；telemetry_1h 流）。pipeline 启动时幂等执行。
- Redis key 见方案 §17.5：shadow:{sn} dedupe:{sn}:{seq} desired:{sn} gate:{cell}:handshake online:{cell} alarm:squelch:{sn}:{code}

## 6. 端口与 HTTP 接口
| 服务 | 端口 | 接口 |
|---|---|---|
| bootstrap-svc | 8081 | `GET /api/v1/bootstrap?sn=` → `{cell_id,mqtt_host,mqtt_port,retry_after}`（首次接入建 device 行并 HSET 维表 `device:{sn}` pk/region/cell，写失败不影响响应）；`POST /internal/cells/{id}/overload {overloaded:bool}`；`POST /internal/cells/{id}/drain` |
| auth-svc | 8082 | `POST /auth {clientid,username,cert_fp}` → `{result:"allow"|"deny"}`；`POST /acl {clientid,topic,action}`；`GET /metrics`（auth_allow/deny、deny_reason_*、auth_failopen_allowed、auth_store_error、cache_size）。`IOT_AUTH_FAILOPEN`（默认 false）开启后 Store 出错且 `IOT_AUTH_CACHE_TTL`（默认 24h）内成功认证过的 (clientid, cert_fp) 放行；明确拒绝不受影响；开启需登记（INC-02） |
| conn-gate | 1884→上游 1883 | TCP 代理：accept 后先取令牌（`gate:{cell}:handshake` 令牌桶，默认 5000/s burst 200；原型用本地桶 + `IOT_GATE_RPS`），无令牌 `SetLinger(0)+Close` 发 RST；上游 dial 失败连续 N 次开熔断 30s（熔断期间直接 RST）；`GET :8084/metrics` 文本计数 |
| deviceapi | 8083 | §4 的 shadow（含 switches 三态）/desired/cmd/cmds 接口 + `GET /api/v1/devices/{sn}/telemetry?from=&to=&limit=`（查 TDengine）；cmd 的 X-Source / X-Operator / grant 语义见 §4 |
| alarm-svc | 8085 | `GET /api/v1/alarms?status=`；`POST /api/v1/alarms/{id}/ack`；`POST /api/v1/alarms/{id}/close`。推送前查组织告警目标（BL5 §09.1）：`GET {IOT_FLEET_URL}/internal/alarm-targets?sn=`，硬超时 500 ms，失败 / 超时 / 未配置 `IOT_FLEET_URL` 一律回退个人绑定（BL1 行为）并计数 `alarm_targets_fallback`；状态机、squelch、升级逻辑不变 |
| ota-svc | 8086 | `POST /api/v1/firmwares` `POST /api/v1/ota/batches {firmware_id,stage,created_by,approved_by?}`（stage=100 且无 approved_by → PG CHECK 拒绝 → 403）`POST /api/v1/ota/batches/{id}/pause|resume|advance` `GET /api/v1/ota/batches/{id}`；内部：圈选 device(product_key,fw_version) 按 stage 百分比 md5(sn) 尾数稳定抽样（档位累进包含），下发 `down/{sn}/cmd action=ota`，消费 `iot.up.ota_progress.*` 更新 ota_device_task 与计数；熔断纯函数 `ShouldFuse(ok, fail int, threshold float64, minSamples int) bool`（样本<50 不熔断；fail_ratio 严格大于阈值）与 `ShouldFuseAbs(..., minAbsFail int)`（或 fail ≥ minAbsFail，默认 5，`IOT_OTA_MIN_ABS_FAIL`）表驱动单测；stale sweeper：notified/downloading/verifying 超 `IOT_OTA_STALE_AFTER`（30m）无回报 → failed/STALE_TIMEOUT 计入失败与熔断（每 `IOT_OTA_STALE_INTERVAL` 1m 一轮，指标 stale_failed）；建批档位顺序强制 0.1→1→10→50→100，越级 409；fused 无自动恢复。组织子批次（BL5 §08）：建批支持 `explicit_sns`（跳过 md5 抽样、按 SN 直建任务，SN 不在 iot_shard.device 的忽略并计数 `explicit_sns_unknown`）、`parent_batch_id`（不入父固件档位链，档位 / 审批人 / 熔断阈值强制继承父批次）与 `policy.window {start,end,tz,weekdays}`（纯函数 `InWindow` 支持跨午夜与周几、无效 tz 回退 UTC，窗口外 DispatchOnce 整批跳过并计数 `window_skipped`）；列见 sql/bl5.sql 末尾幂等 ALTER |
| cf001-svc | 8087 | §7；`GET /metrics`。`IOT_CF001_FRESHNESS_WINDOW`（默认 5m）、`IOT_CF001_SKIP_FRESHNESS`（默认 false，仅开发）、`IOT_CF001_REQUIRE_KEY`（默认 false；生产 true，密钥缺失拒绝启动） |
| job-svc | 8091 | BL4 加工记录与反哺（技术方案 §8）。durable consumer `job` 订 `iot.up.event.*`，只处理 JOB_START/DONE/FAIL/PAUSE；**opt-in fail-closed**：影子 reported.`job_feedback_optin` 为 true 才落库，false / 缺失 / Redis 出错一律丢弃并计数 dropped_optin_*；字段白名单 job_id UUID36、material_id/param_profile_id `^[A-Za-z0-9_-]{1,32}$`、params_hash 64 hex（不符丢弃 dropped_bad_fields）；JOB_START → job_record，终态 → finished_at/outcome/duration_s（START 丢失补插计 late_start）；`POST /api/v1/jobs/{job_id}/feedback {rating}`（头 `X-Device-Sn` 须与 job_record.sn 一致；job_record 未到 → 202 暂存 job_feedback_pending，到达后同事务合并）；撤回删除以 **desired**.job_feedback_optin=false 为触发（`IOT_JOB_PURGE_INTERVAL` 默认 10m）；`GET /metrics` |
| reco-job | — | BL4 推荐离线批（技术方案 §9），`-once` 或 `-interval 24h`。近 `IOT_RECO_WINDOW_DAYS`（30）天 job_record ⋈ job_feedback 聚合，按 (pk, module_model, material_id, params_hash) 生成候选写 `iot_global.param_recommendation`：同 SN 同日去重、样本 ≥ 30 且设备 ≥ 30、good ≥ 0.8、反刷偏（单设备单日 > 20 或 top3 占比 > 50% 排除）；params 取自 user_param 中 `CanonicalHash`（键排序紧凑 JSON 的 sha256）相等者；功率 > 同维度官方档 110% → removed/power_cap；安全关联：7 天内使用该档任务的安全事件率 > 同材料其它任务 2 倍且绝对数 ≥ 3 → removed/safety（TDengine 不可用跳过并 WARN）；校正系数 `Correction` clamp [0.8,1.25]，输入缺失返回 1，输出 k_power 分布与 cap 占比告警。published/removed 状态不回退 |
| support-svc | 8095 | BL6 售后与保修（技术方案 §16.2）。诊断包 `POST /api/v1/support/bundles {sn,trigger?,ticket_id?,user_note?}`（七源并行各 2 s 超时，失败源标 unavailable + degraded；content 由结构体白名单生成只含 SN；30 天过期 410）、`GET /api/v1/support/bundles/{id}`、`GET /api/v1/devices/{sn}/bundles`；授权 `POST /api/v1/support/grants {sn,ticket_id,requested_by,actions?}` → `POST /grants/{id}/confirm|deny|revoke`（X-User-Id，服务查 owner；granted 15 min）→ `POST /grants/{id}/self_check`（X-Operator，source=support，经 deviceapi 下发并轮询回执 10 s）、`GET /api/v1/support/tickets/{ticket_id}/cmds`；保修 `GET /api/v1/support/warranty/{sn}`（只有数据与带 evidence/threshold 的信号，无判定键）；字典 `POST /internal/support/dict`（ck_dict_approved 双人审批）、`GET /api/v1/support/dict/{code}`；批次缺陷 `POST /internal/support/defects/run`、`GET /api/v1/support/defects`（每 `IOT_SUPPORT_DEFECT_INTERVAL` 10m 聚合 TDengine events 24h，阈值字典→机型表→默认 20 台，冷却 24h）；Agent 只读代理 `GET /api/v1/agent/devices/{sn}/context`（untrusted 文本包裹）、`POST /api/v1/agent/devices/{sn}/self_check {grant_id}`（X-Agent-Id），其它 agent 指令路由层 403。`IOT_DEVICEAPI_URL` 默认 http://127.0.0.1:8083。表：sql/bl6.sql |
| param-svc | 8090 | BL4 参数库：`POST /internal/params/releases {product_key,note,created_by,approved_by,rollout_pct?,added,removed,force?}`（新 version=max+1；ck_release_approved 拒绝 → 403；ck_profile_params 拒绝 → 400；发布前差异校验同 (module,material,source) power/speed 变化 >30% 超 10 条 → 409 附 violations，force 需 approved_by；事务内写快照 `IOT_PARAM_SNAPSHOT_DIR`（默认 ./data/params）`/{pk}/v{n}.json` 后才 COMMIT；status=published 的 param_recommendation 随发布写成 source=recommended 档）`POST /internal/params/releases/{pk}/rollback {to_version,created_by,approved_by}`（发布新 version，有效集合等于目标版本）`GET /api/v1/params/releases/latest?product_key=&bucket=`（或 X-User-Id 算 md5 bucket；rollout_pct<100 仅 bucket<pct 可见）`GET /api/v1/params?product_key=&since_version=`（`{version, removed, added}` 先删后加；落后 >20 版本 `force_full`+snapshot_url；ETag/If-None-Match 304）`GET /snapshots/{pk}/v{n}.json`（immutable）`PUT/GET /api/v1/user-params`（X-User-Id；If-Match 乐观锁，冲突 409 带 current_version）`GET /api/v1/devices/{sn}/correction?product_key=&module_model=&laser_hours=&health=`（k∈[0.8,1.25] 代码常量；输入缺失 k=1 reason input_missing）`GET /metrics` |
| accessory-svc | 8092 | BL2：`POST /api/v1/pairings {host_sn,acc_sn,acc_type?,linkage_enabled?,off_delay_s?}`（201 新建 / 200 幂等；配件须为 ACC_* 机型；owner 不一致 403；一配件一主机由部分唯一索引 `uk_pairing_active` 兜底）`DELETE|PATCH /api/v1/pairings/{id}` `GET /api/v1/pairings?host_sn=|acc_sn=` `GET /api/v1/devices/{host_sn}/linkage-audit` `GET /api/v1/alarms/{acc_sn}/context?code=&event_ts=` `GET /api/v1/devices/{acc_sn}/filter` `POST /internal/filter/run-once` `POST /internal/reconcile/run-once` `GET /metrics`。联动引擎：IOT_UP durable `accessory`（event.* + telemetry.*，DeliverNew）→ 纯函数 `Decide` → deviceapi；JOB_START/work_state→2 开机到材料档位（linkage_rule），JOB_DONE/→0 排 ZSET `linkage:off` 延时关闭（60..600 s），主机安全码/SMOKE_HIGH 最大档 10 min 安全排烟，FIRE_SUPPRESSED 双证据停主机；过期 60 s 事件不联动；下发失败不 Nak（本地兜底）；每分钟对账。滤芯寿命每小时批写 `filter_life` + `consumable_health(part='filter')`。环境变量 `IOT_DEVICEAPI_URL` `IOT_ACC_ALLOW_OFF`（默认 true，关闭需登记）`IOT_ACC_STOP_HOST_ON_FIRE` `IOT_ACC_DEFAULT_LEVEL` `IOT_ACC_RECONCILE_INTERVAL` `IOT_ACC_FILTER_INTERVAL` |
| health-svc | 8093 | BL3 耗材与材料（技术方案 §16）。批计算每 `IOT_HEALTH_INTERVAL`（1h）或 `-once`：telemetry_1h 桶（last laser_hours、avg_power、share_low/mid/high）算加权时长 → `ComputeHealth`，**关键输入缺失不写 0 而是 skip**，skip 比例 > `IOT_HEALTH_FUSE_RATIO`（0.3）整批放弃（fused）；health 单调不增（换模块除外，DetectSwap 四种证据）；写 `consumable_health(sn,'module')` 与 health_explain。提醒：档位 80/50/20 跨档触发取最低，`IOT_HEALTH_COOLDOWN`（168h）冷却、作业中 deferred、optout 抑制，落 health_reminder 并发布 `iot.notify.health`（IOT_NOTIFY 流，与 alarm-svc 无共享路径）。接口：`GET /api/v1/health/{sn}`（updated_at 超 `IOT_HEALTH_STALE_AFTER` 2h 标 stale）、`GET /api/v1/health/{sn}/reminders`、`GET /api/v1/sku?product_key=&part=`、`POST /api/v1/reminders/{id}/click`（X-User-Id 必填）、`POST /internal/orders/attribute {reminder_id,order_no}`（30 天窗口）、`POST /api/v1/materials/verify {code,sn}`（31 字符 base32，HMAC 按 batch 派生密钥；**假码或可疑码 valid=false 且不带任何参数字段**；`IOT_MATERIAL_KEY` 缺失用内存密钥并 WARN，`IOT_MATERIAL_MAX_SCANS` 默认 1 一次性）、`POST /internal/health/run`、`GET /metrics`。表：sql/bl3.sql |
| fleet-svc | 8094 | BL5 教育与 B 端（技术方案 §17.1）。租户中间件（路由组级）：`X-Org-Id` + `X-User-Id` → org_member 角色，非成员 / 组织不存在一律 **404**（不泄露存在性），角色不足 403（矩阵纯函数 `Authorize(role, action)`）。`POST /api/v1/orgs`；`GET /api/v1/orgs/{org}`、`GET|POST .../members`、`DELETE .../members/{uid}`、`GET|POST .../sites`；归属 `GET|POST /api/v1/orgs/{org}/devices`（纯函数 `ResolveOwnership` 裁决个人绑定与跨组织占用，放行与拒绝都写 lock_audit）、`DELETE .../devices/{sn}`；看板 `GET .../fleet/summary`（ListDevices → 分批 ≤200 读 Redis 影子 → `Summarize` → `Aggregate`，含在线 90 s / 告警 / 待升级计数）、`GET .../devices` 分页；课表 `PUT|GET .../sites/{site}/policy`（`internal/pkg/schedule` 与设备端共享 `ShouldLock`）、临时解锁 `POST .../devices/{sn}/unlock`（teacher+，desired `{lock:false, lock_expires_at}`，默认 `IOT_FLEET_UNLOCK_TTL` 2h，上限 org.unlock_max_minutes，写 lock_audit）；队列 `POST .../queues`、`POST|GET .../queues/{q}/items`、`POST .../items/{id}/approve|reject|cancel`（迁移一律 `Transition` 纯函数 + 条件 UPDATE）；耗材 `GET .../consumables?format=csv`（`SummarizeConsumables` / `ConsumablesCSV`）；`GET /healthz` `GET /metrics`（fleet_ 前缀：reconcile_runs、locks_applied、unlocks、dispatch_attempts/claimed/ok/failed、items_expired、authz_denied、authz_notfound）。后台两个循环：课表对账每 `IOT_FLEET_RECONCILE_INTERVAL`（1m）`AllPolicies → SiteSNs → ShouldLock → ReconcileLock` 补发 desired；调度器每 `IOT_FLEET_DISPATCH_INTERVAL`（5s）取队首 approved → 影子空闲未锁的候选 SN → `ClaimDispatch` 抢占（affected=0 即跳过，零重复下发）→ deviceapi `POST /api/v1/devices/{sn}/cmd action=job_start` 头 `X-Source: fleet` → 成功 ConfirmDispatch / 失败 RevertDispatch（`IOT_FLEET_MAX_RETRY` 后 needs_teacher）；`IOT_FLEET_EXPIRE_INTERVAL` 清理过期项。`IOT_DEVICEAPI_URL` 默认 http://127.0.0.1:8083。表：sql/bl5.sql；批量 OTA `POST|GET /api/v1/orgs/{org}/ota/batches {firmware_id, sns|site_ids, window}`（sns ⊄ 本组织 → 403 并列出；父批次须 ≥ 10% 档且未 fused；经 `IOT_OTA_URL` 调 ota-svc 建子批次，带 explicit_sns / parent_batch_id / policy.window，写 org_ota_batch）；`GET /internal/alarm-targets?sn=`（**不过租户中间件**，供 alarm-svc 调用：站点订阅在册成员 + 个人绑定用户，org 身份优先去重） |
| device-simulator | — | flags：`-n 50 -pk LM_S1 -mqtt tcp://127.0.0.1:1884 -bootstrap http://127.0.0.1:8081 -hb 60s -work 5s -event FLAME_DETECTED@30s -bad-firmware 0.05 -seq-base 0 -jobs -module LM40 -job-optin=false -accessory=false -pair-host "" -accessory-url http://127.0.0.1:8092`（坏固件 = 固定 1s 重连不退避；accessory = 净化器模式：不跑作业周期、上报 power_on/fan_level/pressure_diff/runtime_h、响应 desired 并立即回报，-pk 未指定时用 ACC_PURIFIER；pair-host = 启动 3 s 后经 accessory-svc 把全部配件配到该主机；jobs = 作业周期边界发 JOB_START/JOB_DONE；module = module_model 属性；job-optin = 出厂 reported.job_feedback_optin，desired 下发后以 desired 为准并在下一帧遥测回报；seq-base 0 = 按启动时刻推导 UnixMilli×1000 使 seq 跨重启单调，避免 10 分钟内重跑撞 dedupe 键，<0 从 0 起）；先 bootstrap 再 connect；退避用 pkg/backoff；订阅 down/{sn}/#，收到 cmd 回 cmd_ack，收到 ota 按 phase 序列回 ota_progress（`-ota-fail-rate` 注入失败） |
| loadgen | — | 剧本：`conn -n 5000 -rate 500 -target 127.0.0.1:1884`（连接后 1s 读探测识别 RST 假成功，输出 CSV：t,ok,rst,alive）；`storm -n 3000 -burst`；`flood -pre 30000`（泄洪：预灌 JetStream 后计时排空，对账 TDengine 行数）；结果写 `docs/loadtest/*.csv` |

## 7. CF001 代工机型激活体系（cf001-svc）
- 表（schema cf001）：`oem_quotas(id, order_no unique, supplier, product_key, quota int, registered int default 0, status smallint 1=进行中 2=已完成, created_at)`；`digest_maps(digest char(64) pk, uuid, mcu_sn, soc_sn, mac, sn char(23) unique, order_no, created_at)`；`oem_devices(sn pk, digest, signature text, product_key, registered_at)`。
- digest = hex(SHA256(uuid + ":" + mcuSN + ":" + socSN + ":" + mac))；uuid 设备内随机生成。
- 认证载荷：设备用云公钥 RSA-OAEP(SHA-256) 加密 `digest|nonce|timestamp` 三段（`|` 分隔；兼容忽略第 4 段）；云端私钥解密。**开发环境一对密钥兼加密与签名两职，生产分两对且签名私钥进 HSM**（写进 docs/cf001-impl.md 差异表）。
- 设备自检签名：云端用私钥 RSA-PSS(SHA-256) 对 digest 签名返回；设备用公钥验签。
- 23 位 SN：`{pk 4 位}{yymmdd 6 位}{产线 2 位}{当日流水 9 位，Redis INCR sn:seq:{yymmdd}}{校验码 2 位}`，校验码 = 前 21 位 ASCII 求和 % 256 转两位大写 hex（文档示例应为 77 非 93，单测断言算法值）。
- 接口：`POST /api/v1/oem/sign {order_no, payload_b64}`（按 **IP** 限频 10 次/分钟，第 11 次 100012）→ 解密 → **新鲜度 + nonce 去重**（重放 400 10001；Redis 出错 503）→ 校验 digest 未注册（已注册返回既有 SN，幂等）→ **配额事务**：
  ```sql
  UPDATE cf001.oem_quotas SET registered = registered + 1,
         status = CASE WHEN registered + 1 >= quota THEN 2 ELSE status END
   WHERE order_no = $1 AND status = 1 AND registered < quota;   -- affected=0 → 11010
  -- 同事务 INSERT digest_maps + oem_devices，任一失败整体回滚
  ```
  返回 `{sn, signature_b64}`。`POST /api/v1/oem/verify {sn, digest, signature_b64}` 三步自检（存在/一致/验签）。`POST /api/v1/oem/quotas` 建工单、`POST /api/v1/oem/quotas/{order_no}/append {delta}`（追加配额 status 回 1 自动恢复）。
- 设备侧限流：认证后的接口按 `deviceId(sn)+path` 维度限频（Security 中间件之后），同设备第 3 次即 100012（rps 1, burst 2 便于演示）。
- 测试：digest/SN 校验码/载荷解析 表驱动单测；配额事务 `-race` 100 goroutine 抢 10 配额 恰好 10 成功 90 拒绝（IOT_IT 门控，打真实 PG）。

## 8. 工程纪律
- 每个服务 `go vet` 干净，`go build ./...` 通过，`go test -race ./...` 通过（集成测试无依赖时 skip 而非失败）。
- 纯逻辑抽成纯函数并表驱动测试：熔断判定、SN 规则、digest、退避、告警状态迁移、cell hash、攒批切分。
- 日志用 `log/slog` JSON；所有服务 `GET /healthz` 只返回 name/version，不检查依赖。
- 不写任何真实密钥：cf001 开发密钥由 `make keys` 生成到 `./data/keys/`（gitignore）。
