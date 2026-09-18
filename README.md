# xtool-aiot · xTool AIoT 设备云平台预研（架构方案 + 11 服务可运行原型）

按《xTool AIoT 平台技术方案》（18 章）§17 数据模型与 §18 分步实现设计落地的可运行原型：
接入调度（bootstrap）、认证（auth）、连接许可网关（conn-gate）、EMQX→NATS 桥接（bridge）、
消费管道（pipeline，攒批写 TDengine + Redis 影子）、设备 API（影子 / desired / RRPC 指令 / 审计）、
告警状态机（alarm）、OTA 灰度熔断（ota）、代工设备激活体系（cf001）、设备模拟器与压测负载器。

> 定位：**技术预研原型**，不是生产系统。所有数字来自本机实测，报数字先报口径（docs/loadtest-report.md）。

## 快速开始
```bash
make dev            # docker compose 起 emqx/nats/redis/postgres/tdengine，并初始化 TDengine schema
make keys           # CF001 开发密钥
make build && make test
# 分别起服务（各自默认端口见 docs/spec.md §6）
make run-bootstrap-svc & make run-auth-svc & make run-conn-gate & make run-bridge & make run-pipeline & \
make run-deviceapi & make run-alarm-svc & make run-ota-svc & make run-cf001-svc &
make smoke          # 50 台模拟器 + 火焰事件端到端
make loadtest-conn  # 5000 并发连接
make loadtest-flood # 泄洪法测管道吞吐并对账
```

## 文档
- `docs/spec.md` 工程规格与契约（所有服务共同遵守）
- `docs/loadtest-report.md` 压测报告（口径、CSV、复现命令）
- `docs/incident-register-storm.md` 线上 /register 事故复盘与回灌
- `docs/cf001-impl.md` CF001 实现与需求文档差异记录
- `docs/drill-runbook.md` 单元容灾演练 Runbook
