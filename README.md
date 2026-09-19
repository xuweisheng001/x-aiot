# xtool-aiot · xTool AIoT 设备云平台预研（六条业务线方案 + 18 服务可运行原型）

按《xTool AIoT 平台技术方案》（18 章）与六条业务线的 PRD / 技术方案落地的可运行原型。
平台底座：接入调度（bootstrap）、认证（auth）、连接许可网关（conn-gate）、EMQX→NATS 桥接（bridge）、
消费管道（pipeline，攒批写 TDengine + Redis 影子）、设备 API（影子 / desired / RRPC 指令 / 审计）、
告警状态机（alarm）、OTA 灰度熔断（ota）、代工设备激活体系（cf001）、设备模拟器与压测负载器。

> 定位：**技术预研原型**，不是生产系统。所有数字来自本机实测，报数字先报口径（docs/loadtest-report.md）。

## 六条业务线

| 业务线 | 服务 | 端口 | 职责 |
|---|---|---|---|
| BL1 消费级整机 | bootstrap / auth / conn-gate / bridge / pipeline / deviceapi / alarm / ota / cf001 | 8081 到 8089 | 接入、影子、安全事件、指令、OTA、代工激活五条核心链路 |
| BL2 配件与安全生态 | accessory-svc | 8092 | 配对、联动规则引擎、延时关闭、灭火事件关联、滤芯寿命 |
| BL3 耗材与材料 | health-svc | 8093 | 模块健康度批计算与熔断、三档提醒与冷却、SKU 归因、材料码防伪 |
| BL4 软件与内容 | param-svc / job-svc / reco-job | 8090 / 8091 | 参数库版本化与灰度、加工记录反哺、推荐候选生成 |
| BL5 教育与 B 端 | fleet-svc | 8094 | 组织租户隔离、多机看板、课表锁、任务队列与调度器 |
| BL6 售后与保修 | support-svc | 8095 | 诊断包白名单、客服授权自检、Agent 只读代理、批次缺陷发现 |

**三条贯穿全部业务线的约束**

1. 安全闭环在端侧。云端只做通知、留痕、复核；断网不降低安全性。
2. 不可事后补救的东西进第一版固件：重连退避、一机一证、A/B 双分区。
3. 护栏做成数据库约束而不是流程：OTA 全量双人审批、参数发布双人审批、配额不超发、告警状态迁移，都是 CHECK 或带前置状态的 UPDATE。

## 快速开始

```bash
make dev            # docker compose 起 emqx/nats/redis/postgres/tdengine，并初始化 TDengine schema
make keys           # CF001 开发密钥
make build && make test
# 分别起服务（各自默认端口与环境变量见 docs/spec.md §6）
make run-bootstrap-svc & make run-auth-svc & make run-conn-gate & make run-bridge & make run-pipeline & \
make run-deviceapi & make run-alarm-svc & make run-ota-svc & make run-cf001-svc & \
make run-param-svc & make run-job-svc & make run-accessory-svc & make run-health-svc & \
make run-fleet-svc & make run-support-svc &
make smoke          # 50 台模拟器 + 火焰事件端到端（11 项检查）
make loadtest-conn  # 5000 并发连接
make loadtest-flood # 泄洪法测管道吞吐并对账
IOT_IT=1 go test -race ./...   # 集成测试（需上面的依赖在跑）
```

模拟器还可以演净化器与作业事件：

```bash
go run ./cmd/device-simulator -n 1 -sn-prefix ACC -accessory -pair-host SIM00001   # 净化器，自动配对
go run ./cmd/device-simulator -n 1 -job-optin=true -module LM40                     # 上报 JOB_* 与模块型号
go run ./cmd/reco-job -once                                                          # 推荐候选离线批
go run ./cmd/health-svc -once                                                        # 健康度批计算
```

## 文档

每条业务线一份 PRD、一份技术方案、一份事故预推演（PRD 与预推演另有同名 HTML）。

| 文档 | 内容 |
|---|---|
| `docs/spec.md` | 工程规格与契约，所有服务共同遵守 |
| `docs/tech-design-aiot-platform.md` | 平台技术方案 18 章 |
| `docs/prd-bl{1..6}-*.md` | 六条业务线 PRD |
| `docs/tech-design-bl{2,3,4,5,6}-*.md` | 各业务线技术方案（只写增量） |
| `docs/incident-premortem-bl{1..6}.md` | 各业务线线上事故预推演与处置方案，附实现状态 |
| `docs/loadtest-report.md` | 压测报告（口径、CSV、复现命令） |
| `docs/incident-register-storm.md` | 线上 /register 事故复盘与回灌 |
| `docs/cf001-impl.md` | CF001 实现与需求文档差异记录 |
| `docs/drill-runbook.md` | 单元容灾演练 Runbook |

## 数据库

`sql/` 下按层与业务线分文件，compose 启动时按序自动执行：
`global.sql`（全局库）、`shard.sql`（分片库）、`cf001.sql`、`bl2.sql`、`bl3.sql`、`bl4.sql`、`bl5.sql`、`bl6.sql`、`tdengine.sql`（时序库）。
新增业务线只追加自己的文件，不改既有表与约束。
