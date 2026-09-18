# xtool-aiot · 工程规格与契约（所有服务必须遵守）

> 目标：把《xTool AIoT 平台技术方案》§17/§18 落成可运行原型。11 个 Go 程序 + 共享包，单机 `make dev` 跑通全链路。
> 模块路径 `github.com/xtool/xtool-aiot`，Go 1.24，只用 go.mod 里已列的依赖 + 标准库。HTTP 用 `net/http`（1.22+ 路由模式）。

## 1. 目录约定
```
cmd/<svc>/main.go            每个服务一个 main，只做装配：读 config → 建依赖 → 起 server
internal/<svc>/...           服务私有代码（handler / service / repo / 纯函数）
internal/pkg/...             共享包（见 §3），任何服务不得复制一份自己的版本
sql/                          global.sql shard.sql cf001.sql tdengine.sql（compose 启动自动执行）
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
  ```
- `cellmap`：`CellOf(sn string, n int) int` = fnv32a(sn)%n+1（bridge 与 bootstrap 必须同一函数）
- `config`：`Env(key, def string) string`、`EnvInt`、`MustPG(ctx)`, `MustRedis()`, `MustNATS()` 返回已连接客户端；`EnsureStreams(js)` 幂等建两个 stream
- `httpx`：`JSON(w, code, v)`、`Error(w, httpStatus, bizCode int, msg string)`（响应体 `{"code":bizCode,"msg":...}`）、
  `RateLimitByKey(keyFn func(r)*string, rps float64, burst int) Middleware`（进程内令牌桶 map，超限返回 HTTP 429 + code 100012）、
  `Recover`、`Logging`
  业务码常量：`CodeOK=0 CodeRateLimited=100012 CodeNoQuota=11010 CodeBadParam=10001 CodeNotFound=10004 CodeDenied=10003`
- `tdengine`：REST 客户端 `Exec(ctx, sql) error`、`Query(ctx, sql) (*Result, error)`；`BatchInsert(ctx, rows []TelemetryRow) error` 按 SN 分组生成多表多行 INSERT（`INSERT INTO iot.t_<sn> USING iot.telemetry TAGS(...) VALUES (...)(...) iot.t_<sn2> ...`），单条 SQL ≤ 900KB 自动切分
- `shadow`：Redis 影子 `Key(sn)="shadow:"+sn`、`WriteReported(ctx, pipe, sn, fields map[string]any)`、`Read(ctx, sn)`
- `dedupe`：`Seen(ctx, rdb, sn, seq) (dup bool, err error)` = SETNX dedupe:{sn}:{seq} EX 600；Redis 出错时返回 `dup=false, err`（调用方放行并计数）
- `backoff`：`Next(n int, base time.Duration) time.Duration` = min(2^n·base, 15min) + rand(0,30s)（设备端纪律；simulator 用，单测覆盖上限与抖动范围）

## 4. 消息与 Topic 契约
- 设备 → EMQX：`up/{pk}/{sn}/telemetry|event|cmd_ack|ota_progress`，payload JSON：
  ```json
  telemetry: {"seq":123,"ts":1726300000000,"work_state":2,"power_level":87,"temp_cavity":41.5,"temp_water":25.1,"fan_rpm":3000,"laser_hours":120.3,"progress":55}
  event:     {"seq":124,"ts":...,"code":"FLAME_DETECTED","msg":"..."}        安全码集合 SafetyCodes = {FLAME_DETECTED, OVER_TEMP, TILT, ESTOP, WATER_FLOW}
  cmd_ack:   {"seq":125,"ts":...,"cmd_id":"...","result":"ok|fail","detail":"..."}
  ota_progress: {"seq":126,"ts":...,"batch_id":1,"phase":"notified|downloading|verifying|success|failed|rolled_back","pct":40,"error_code":""}
  ```
- 云 → 设备：`down/{sn}/cmd` `{"cmd_id","action","params"}`；`down/{sn}/desired` `{"version":N,"desired":{...}}`
- bridge：共享订阅 `$share/bridge/up/#` → 信封 → JetStream `iot.up.{kind}.{cell}`，cell=CellOf(sn,N)。JetStream 不可用：telemetry 丢弃计数；event 进本地磁盘缓冲（`./data/bridge-buffer.jsonl`）定时重发。
- pipeline（durable consumer `pipeline-<cell>`，每 cell 一个）四步：解析校验 → 富化(Redis 维表 `device:{sn}` Hash: pk,fw,region,cell) → 幂等(dedupe) → 分发。
  Ack 语义：解析失败 Ack 丢弃+计数；重复 Ack；TDengine 写失败 Nak（AckWait 30s）；成功 Ack。
  **攒批**：`BATCH_SIZE=500` 或 `BATCH_WINDOW=120ms` 先到者触发；一批 = 一次 `tdengine.BatchInsert` + Redis pipelined 写影子（每 SN 只写批内最新）+ 批量 Ack；整批失败整批 Nak。flusher 数 = `IOT_FLUSHERS`（默认 1，按 cell subject 分区可并行）。
- alarm-svc：独立 durable consumer `alarm` 订 `iot.up.event.*`，只处理 SafetyCodes；squelch `alarm:squelch:{sn}:{code}` 5min；状态机 open→notified→acked→closed，全部用「带前置状态的 UPDATE」，affected=0 → 非法迁移返回 409；notified 超 10min 未 acked 且 critical → 升级(记录 escalated_at，日志模拟短信)。
- cmd：`POST /api/v1/devices/{sn}/cmd {action,params}`，白名单 pause/stop/self_check；remote_restart → 403 code 10003。cmd_id=UUIDv4；发布 `down/{sn}/cmd` + 审计消息到 `iot.cmd.audit`；deviceapi 内的审计消费者落 `cmd_audit`；`GET /api/v1/cmds/{cmd_id}` 查结果（cmd_ack 事件回流由 pipeline 写 Redis `cmdres:{cmd_id}` TTL 1h）。
- desired：`PATCH /api/v1/devices/{sn}/desired {k:v}` → PG `shadow_desired` 合并 JSONB、version++ (单事务 `UPDATE ... RETURNING version`，不存在则 INSERT) → 发布 `down/{sn}/desired`。`GET /api/v1/devices/{sn}/shadow` 返回 `{reported, desired, desired_version}`。

## 5. 数据库
- PG 单实例三个 schema 模拟三层：`iot_global`（product/thing_model/cell/firmware/firmware_delta/ota_batch）、`iot_shard`（原型只建一个分片：device/device_cert/device_binding/shadow_desired/cmd_audit(按月分区，脚本预建当月+下月)/ota_device_task/alarm/consumable_health）、`cf001`（见 §7）。DDL 见 sql/*.sql，与方案 §17 同源，**不得改约束**（`ck_full_stage_approved` CHECK、`uk_binding_active` 部分唯一索引 必须保留并有测试证明生效）。
- TDengine：sql/tdengine.sql（KEEP 90；telemetry / events 超级表；telemetry_1h 流）。pipeline 启动时幂等执行。
- Redis key 见方案 §17.5：shadow:{sn} dedupe:{sn}:{seq} desired:{sn} gate:{cell}:handshake online:{cell} alarm:squelch:{sn}:{code}

## 6. 端口与 HTTP 接口
| 服务 | 端口 | 接口 |
|---|---|---|
| bootstrap-svc | 8081 | `GET /api/v1/bootstrap?sn=` → `{cell_id,mqtt_host,mqtt_port,retry_after}`；`POST /internal/cells/{id}/overload {overloaded:bool}`；`POST /internal/cells/{id}/drain` |
| auth-svc | 8082 | `POST /auth {clientid,username,cert_fp}` → `{result:"allow"|"deny"}`；`POST /acl {clientid,topic,action}` |
| conn-gate | 1884→上游 1883 | TCP 代理：accept 后先取令牌（`gate:{cell}:handshake` 令牌桶，默认 5000/s burst 200；原型用本地桶 + `IOT_GATE_RPS`），无令牌 `SetLinger(0)+Close` 发 RST；上游 dial 失败连续 N 次开熔断 30s（熔断期间直接 RST）；`GET :8084/metrics` 文本计数 |
| deviceapi | 8083 | §4 的 shadow/desired/cmd/cmds 接口 + `GET /api/v1/devices/{sn}/telemetry?from=&to=&limit=`（查 TDengine） |
| alarm-svc | 8085 | `GET /api/v1/alarms?status=`；`POST /api/v1/alarms/{id}/ack`；`POST /api/v1/alarms/{id}/close` |
| ota-svc | 8086 | `POST /api/v1/firmwares` `POST /api/v1/ota/batches {firmware_id,stage,created_by,approved_by?}`（stage=100 且无 approved_by → PG CHECK 拒绝 → 403）`POST /api/v1/ota/batches/{id}/pause|resume|advance` `GET /api/v1/ota/batches/{id}`；内部：圈选 device(product_key,fw_version) 按 stage 百分比 md5(sn) 尾数稳定抽样（档位累进包含），下发 `down/{sn}/cmd action=ota`，消费 `iot.up.ota_progress.*` 更新 ota_device_task 与计数；熔断纯函数 `ShouldFuse(ok, fail int, threshold float64, minSamples int) bool`（样本<50 不熔断；fail_ratio 严格大于阈值）表驱动单测；fused 无自动恢复 |
| cf001-svc | 8087 | §7 |
| device-simulator | — | flags：`-n 50 -pk LM_S1 -mqtt tcp://127.0.0.1:1884 -bootstrap http://127.0.0.1:8081 -hb 60s -work 5s -event FLAME_DETECTED@30s -bad-firmware 0.05`（坏固件 = 固定 1s 重连不退避）；先 bootstrap 再 connect；退避用 pkg/backoff；订阅 down/{sn}/#，收到 cmd 回 cmd_ack，收到 ota 按 phase 序列回 ota_progress（`-ota-fail-rate` 注入失败） |
| loadgen | — | 剧本：`conn -n 5000 -rate 500 -target 127.0.0.1:1884`（连接后 1s 读探测识别 RST 假成功，输出 CSV：t,ok,rst,alive）；`storm -n 3000 -burst`；`flood -pre 30000`（泄洪：预灌 JetStream 后计时排空，对账 TDengine 行数）；结果写 `docs/loadtest/*.csv` |

## 7. CF001 代工机型激活体系（cf001-svc）
- 表（schema cf001）：`oem_quotas(id, order_no unique, supplier, product_key, quota int, registered int default 0, status smallint 1=进行中 2=已完成, created_at)`；`digest_maps(digest char(64) pk, uuid, mcu_sn, soc_sn, mac, sn char(23) unique, order_no, created_at)`；`oem_devices(sn pk, digest, signature text, product_key, registered_at)`。
- digest = hex(SHA256(uuid + ":" + mcuSN + ":" + socSN + ":" + mac))；uuid 设备内随机生成。
- 认证载荷：设备用云公钥 RSA-OAEP(SHA-256) 加密 `digest|nonce|timestamp` 三段（`|` 分隔；兼容忽略第 4 段）；云端私钥解密。**开发环境一对密钥兼加密与签名两职，生产分两对且签名私钥进 HSM**（写进 docs/cf001-impl.md 差异表）。
- 设备自检签名：云端用私钥 RSA-PSS(SHA-256) 对 digest 签名返回；设备用公钥验签。
- 23 位 SN：`{pk 4 位}{yymmdd 6 位}{产线 2 位}{当日流水 9 位，Redis INCR sn:seq:{yymmdd}}{校验码 2 位}`，校验码 = 前 21 位 ASCII 求和 % 256 转两位大写 hex（文档示例应为 77 非 93，单测断言算法值）。
- 接口：`POST /api/v1/oem/sign {order_no, payload_b64}`（按 **IP** 限频 10 次/分钟，第 11 次 100012）→ 解密 → 校验 digest 未注册（已注册返回既有 SN，幂等）→ **配额事务**：
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
