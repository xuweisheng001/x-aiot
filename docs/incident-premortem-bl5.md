# BL5 教育与 B 端 · 线上事故预推演与处置方案

> Incident Pre-mortem · 对应 docs/prd-bl5-education-b2b.md 与 docs/tech-design-bl5-education-b2b.md

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1 · 2026-09-18 |
| 目的 | BL5 试点上线前，沿组织租户、时段锁定、任务队列、批量 OTA、看板、订阅六条线推演最可能的线上事故，每条给触发、传播、检测信号、止血、恢复、根治、演练 |
| 与其它预推演的关系 | BL1（设备与平台）27 条、BL4（入口与业务链路）26 条全部适用于 BL5 的底层；本文只写 BL5 新增的 24 条。分级、处置四原则、临时关闭登记表沿用 BL1 §0 与 §10 |
| 编号 | INC-5-xx |

**BL5 事故的三个特征**

1. **发生在学校**。用户是老师和未成年学生，任何安全事故的后果与舆情都比家庭用户严重一个量级。禁用误伤安全是本文最高优先级。
2. **一次影响一间教室或一所学校**。个人用户的事故影响一台机器，组织用户的事故影响 12 台或 200 台，且发生在上课这个不可重来的时间窗。
3. **锁是新的故障面**。BL1 与 BL4 从未让云端有能力阻止机器工作。BL5 引入的 locked 状态既可能锁不住（违规），也可能锁死（教学中断），两个方向都要推演。

---

## 1. 事故清单总览

| ID | 事故 | 线 | 级别 | 第一检测信号 | 止血时限 |
|---|---|---|---|---|---|
| INC-5-01 | 成员移除后仍可操作：授权缓存未失效 | 租户 | S2 | 移除后 cmd_audit / lock_audit 出现该用户 | 5 min |
| INC-5-02 | 个人绑定用户在组织归属后仍能下指令 | 租户 | S2 | 归属设备的 cmd_audit source=app 且 operator 非成员 | 15 min |
| INC-5-03 | 租户越权：A 校成员看到 B 校设备与学生 | 租户 | **S1** | 单用户访问 org_id 数 > 所属数；某路由 404 比例为 0 | 立即 |
| INC-5-04 | 设备被误归属到别的组织（SN 输错） | 租户 | S2 | 原个人用户收到「由组织管理」通知后投诉 | 1 h |
| INC-5-05 | 看板批量影子打爆 Redis 或 deviceapi | 看板 | S3 | deviceapi 批量接口延迟；Redis 命令数 | 15 min |
| INC-5-06 | 看板显示与设备实际不一致（在线判定错） | 看板 | S3 | 抽样对账影子 updated_at | 1 h |
| INC-5-07 | 禁用误伤安全：locked 下安全事件未停机或未告警 | 锁定 | **S1** | 禁用误伤安全率对账 > 0；台架 | 立即 |
| INC-5-08 | 上课时全站锁死无法解锁 | 锁定 | **S1** | 站点内 LOCKED_START_DENIED 在窗口内突增；老师求助 | 5 min |
| INC-5-09 | 锁不住：窗口外大量 JOB_START | 锁定 | S2 | 时段外违规开机率 > 1% | 15 min |
| INC-5-10 | 时区或夏令时错误导致整站课表偏移 1 小时 | 锁定 | S2 | 多站点同时在整点边界前后出现违规/锁死 | 30 min |
| INC-5-11 | 临时解锁到期不回锁 | 锁定 | S2 | reported.lock_state=2 超过 lock_expires_at 的设备数 | 15 min |
| INC-5-12 | 课表下发风暴：一次编辑触发 200 台 PATCH 与 MQTT | 锁定 | S3 | deviceapi 写 QPS 尖峰；MQTT 下行积压 | 15 min |
| INC-5-13 | 调度器重复下发同一任务到两台设备 | 队列 | **S1** | 同 item_id 两条 dispatched 审计；两台设备同 job_id | 立即 |
| INC-5-14 | 调度器双 leader 或无 leader | 队列 | S2 | scheduler_leader 计数 ≠ 1 | 5 min |
| INC-5-15 | 任务下发到 locked 或作业中设备 | 队列 | S2 | LOCKED_START_DENIED{source:cloud} 或 cmd_ack fail 突增 | 15 min |
| INC-5-16 | 学生绕过审批（自动批准白名单配置错） | 队列 | S2 | approved_by=auto 比例 100% | 1 h |
| INC-5-17 | 队列文件预签名 URL 过期导致设备下载失败循环 | 队列 | S3 | dispatched → skipped 比例上升，error 为下载类 | 30 min |
| INC-5-18 | 批量 OTA 在上课时间下发 | OTA | S2 | 窗口外下发数 > 0 | 5 min |
| INC-5-19 | 批量 OTA 打满校园带宽 | OTA | S3 | 站点并发下载 > 5；下载超时率 | 15 min |
| INC-5-20 | 组织子批次绕过灰度选了未到档固件 | OTA | S2 | 子批次 firmware 的父批次档位 < 10% | 立即 |
| INC-5-21 | 告警推送目标查询超时导致推送延迟 | 告警 | S2 | alarm_targets 延迟 P99 > 500 ms；推送 P99 > 3 s | 5 min |
| INC-5-22 | 告警推给了已离职老师 | 告警 | S3 | 订阅表含已移除成员 | 1 h |
| INC-5-23 | 订阅到期误伤课表与告警（P2） | 订阅 | **S1** | 到期组织的锁 / 告警接口 403 | 立即回滚 |
| INC-5-24 | 学生数据超范围采集 | 合规 | S2 | 数据字典审计发现 org_member 或 queue_item 含额外字段 | 24 h |

---

## 2. 租户与授权

### INC-5-01 成员移除后仍可操作

**触发**：DELETE member 只写 removed_at，没有删 Redis `orgauth:{user}:{org}`；或多副本进程内缓存。

**传播**：离职老师 60 秒内仍能解锁、下指令、审批。若是被辞退的员工，60 秒足够做坏事。

**检测信号**：每日对账 lock_audit / cmd_audit / queue_item.approved_by 中操作时间晚于该用户 removed_at 的行。

**止血**：手动 `redis-cli del orgauth:{user}:{org}`；紧急时清空 orgauth:* 前缀（所有用户下一次请求回源 PG，短时压力可接受）。

**根治**：成员变更事务提交后同步 DEL；授权缓存只允许 Redis 不允许进程内；对账进每日任务。这是 BL4 INC-4-04 在组织维度的复现，同一根治。

---

### INC-5-02 个人绑定用户在组织归属后仍能下指令

**触发**：BFF 个人视角的 `/devices/{sn}/cmd` 没有查 device_org；或 fleet-svc 的 arbiter 接口超时被当作放行。

**传播**：学校的机器被原来某个个人账号（如采购时用的老师私人账号）远程暂停或停止，课堂中断。

**检测信号**：归属设备的 cmd_audit 中 source=app 且 operator 不在 org_member。

**止血**：BFF 对 arbiter 超时改为 fail-closed（拒绝控制类动作，允许读）；登记。

**根治**：arbiter 判定结果随授权缓存一起缓存；device_org 变更时同时失效相关个人用户的缓存；控制类动作对归属设备默认拒绝，只有明确允许才放行。

---

### INC-5-03 租户越权

**触发**：新增路由忘挂 org 中间件；repo 层某条 SQL 漏了 org_id 条件；BFF 从请求体而非 token 取 user_id。

**传播**：A 校老师能看 B 校的设备状态、学生 user_id、队列文件缩略图，甚至解锁 B 校机器。涉及未成年人数据，是 BL5 最严重的事故。

**检测信号**

- 单 user_id 在 1 小时内访问的 org_id 数 > org_member 中的所属数。
- 越权探测路由的 404 比例：任何 `/orgs/{org_id}/*` 路由对非成员必须 100% 404；某路由 404 为 0 且请求量正常 → 疑似漏校验。
- CI 越权测试：A 校成员用 B 校的 org_id / site_id / sn 遍历全部路由。

**止血**：下线该路由或回滚 fleet-svc / BFF 版本；从访问日志导出被越权访问的 org 与数据类型清单交法务；涉及学生数据的按试点协议通知学校。

**根治**：三道锁（§4.4）全部进 CI 阻断；repo 层 SQL 由模板生成，禁止 handler 直写；越权测试覆盖每一条新路由（路由表变更触发测试用例生成）。

---

### INC-5-04 设备被误归属到别的组织

**触发**：管理员手输 SN 打错一位，恰好是另一位个人用户的机器；或扫码扫到展示机。

**传播**：无辜个人用户的机器突然「由组织管理」，指令面板置灰，课表一到点就锁。

**检测信号**：归属后 24 小时内原个人用户投诉；归属设备的个人绑定用户与组织所在地区不一致。

**止血**：解除归属；通知个人用户并说明。

**根治**：归属需设备端确认（设备屏幕显示「加入 X 学校？」，或设备在线且 5 分钟内有作业才允许）；归属前显示该 SN 的个人绑定用户脱敏信息让管理员核对；一次归属数量上限。

---

## 3. 看板

### INC-5-05 看板批量影子打爆 Redis 或 deviceapi

**触发**：讲台平板前台常驻，1 万站点同时上课，30 s 轮询且 ETag 未生效（每次都是 200）；或站点 SN 列表缓存失效导致每次查 PG。

**传播**：deviceapi 批量影子接口延迟上升 → 个人 App 的影子读也变慢（共用 deviceapi 与 Redis）→ BL1 体验受损。

**检测信号**：deviceapi `/devices/shadows` QPS 与延迟；Redis MGET 命令数；304 比例 < 50%。

**止血**：BFF 对看板路由返回 `Retry-After: 60` 放慢；fleet-svc 看板聚合改为只读 Redis 缓存（15 s）不透传到 deviceapi。

**根治**：ETag 必须基于 max(updated_at) 稳定生成（不含请求时间）；站点看板结果 Redis 缓存 10 s，同站点多个讲台共享；批量影子接口独立限流（按 org）。

---

### INC-5-06 看板显示与设备实际不一致

**触发**：在线判定用影子 updated_at 90 s，但空闲设备心跳 60 s 加上管道 lag 可能超过 90 s；或待升级判定的「最新固件」缓存过期。

**传播**：老师看到机器离线跑去查，机器其实正常；或看到「待升级」点了批量升级实际已是最新。

**检测信号**：抽样对账：看板标记离线但 EMQX 连接存在的比例 > 2%。

**止血**：在线阈值临时调到 150 s。

**根治**：在线判定统一用 deviceapi 的规则并随 BL1 心跳参数调整；看板对「离线」显示最后上报时间而不是二元状态；待升级判定带固件缓存版本号。

---

## 4. 时段锁定

### INC-5-07 禁用误伤安全（S1）

**触发路径推演**

| 情形 | 描述 |
|---|---|
| A | 固件 locked 实现把「禁止开始」误做成「断激光电源」或「忽略传感器」，安全规则不再触发 |
| B | 固件 locked 下屏蔽了所有上行事件（以为锁了就不用上报），FLAME_DETECTED 没发出 |
| C | 云端 fleet-svc 的 lock 下发与 alarm-svc 共用某个开关，锁定时误关了推送 |
| D | 急停按钮在 locked 状态下被软件层拦截 |

**传播**：禁用期间（通常是无人的夜间或周末）若有火灾隐患，机器不停机或没人知道。学校场景下这是最坏结果。

**检测信号**

- 禁用误伤安全率对账（PRD §8）：禁用时段内 events 安全码 vs alarm 表，缺失 > 0 即 S1。
- 台架：每个固件版本发布前 locked 状态下注入火焰、超温、急停三项。
- 合成探针：试点学校每站点 1 台探针机夜间 locked 状态下每小时发一条测试安全事件（用专用测试码 SAFETY_PROBE，走同一链路但不推送用户）。

**止血（立即）**

1. 全局解除锁：fleet-svc 对全部归属设备 PATCH desired lock=false（应急接口 `POST /internal/unlock-all`，需双人确认），登记。
2. 通知试点学校暂停使用课表锁定功能，改为物理断电。
3. 固件回滚到不含 locked 逻辑的版本（走 OTA 灰度）。

**根治**：DR-501 红线台架测试进固件发布门禁；locked 只允许改一个变量（start_allowed），其它任何模块不读它；云端 lock 与安全链路零依赖由依赖扫描锁定；合成探针 P0 上线。

---

### INC-5-08 上课时全站锁死无法解锁（S1）

**触发**：课表编辑错误（把上课时间填成禁用时间）；夏令时切换；fleet-svc 停机导致解锁接口不可用；设备 RTC 漂移。

**传播**：一间教室 12 台机器上课时全部 locked，老师在 App 解锁失败，一节课报废。发生在周一早八点影响全校。

**检测信号**：站点在课表窗口内出现 LOCKED_START_DENIED 突增；老师求助工单；解锁接口 5xx。

**止血（5 分钟内）**

1. 站点级解锁 `POST /orgs/{org}/sites/{site}/unlock {minutes: 240}`。
2. fleet-svc 不可用：直接对 deviceapi 逐台 PATCH desired lock=false（运维脚本，SN 列表来自 device_org）。
3. 设备离线或云端全挂：教师用设备屏幕解锁码（P1，无网时唯一路径）；P0 试点期给每校一个「应急解锁 U 盘」（固件支持本地解锁文件）。

**根治**：课表保存前预览「未来 7 天锁定时段」让老师确认；解锁路径不依赖 fleet-svc（deviceapi desired 直写为兜底）；解锁码 P1 提前；RTC 漂移 > 10 min 以云端最后指令为准（DR-502）。

---

### INC-5-09 锁不住

**触发**：设备本地课表未下发成功（desired.schedule 未收敛）且云端 lock 补发对账未运行；固件 locked 实现只拦本地开始键没拦局域网 XCS 直连加工。

**传播**：放学后学生用 XCS 直连开机器，违反学校规定，安全隐患。

**检测信号**：时段外违规开机率 > 1%；reported.schedule_version 落后 desired 的设备数。

**止血**：对账重发 desired；确认固件是否拦截局域网路径，未拦则通知学校物理断网或断电。

**根治**：locked 必须拦截所有开始路径（本地键、局域网、云端）是 DR-501 的一部分，台架测试三条路径；对账每分钟运行；学生角色是否允许局域网直连由组织策略决定（PRD 开放问题 1）。

---

### INC-5-10 时区或夏令时错误

**触发**：site.tz 填错（学校在美东填了美西）；夏令时切换日 ShouldLock 用了固定偏移；设备本地时区与 site.tz 不一致。

**传播**：整站课表偏移 1 小时：早上第一节课锁着，下午放学后开着。同一时区的所有学校同一天出问题。

**检测信号**：多站点在整点边界前后 1 小时同时出现锁死或违规；发生在 3 月与 11 月的切换日。

**止血**：站点级解锁；修正 tz 后重发课表。

**根治**：ShouldLock 用 IANA 时区库计算，单测覆盖夏令时切换日；课表下发给设备带 tz 而不是偏移；设备本地按 tz 计算；夏令时切换前一周自动巡检所有站点。

---

### INC-5-11 临时解锁到期不回锁

**触发**：设备到期回落逻辑 bug；云端到期补发未运行；lock_expires_at 单位错误（秒当毫秒）。

**传播**：老师解锁 60 分钟，机器整晚开着。

**检测信号**：reported.lock_state=2 且 now > desired.lock_expires_at 的设备数 > 0 持续 5 分钟。

**止血**：对账补发 lock=true。

**根治**：设备端与云端双重到期；lock_expires_at 单位在物模型固定为 ms 并单测；解锁上限 240 分钟限制影响范围。

---

### INC-5-12 课表下发风暴

**触发**：管理员编辑组织级课表模板应用到全部 50 个站点，一次触发 2000 台 PATCH desired。

**传播**：deviceapi 写 PG 与 MQTT 下行尖峰；shadow_desired 行锁竞争；其它用户的 desired 写变慢。

**检测信号**：deviceapi PATCH QPS 尖峰；MQTT 下行积压。

**止血**：fleet-svc 下发限速（每秒 20 台）；剩余排队。

**根治**：课表下发走后台任务队列，限速且可观测；组织级模板应用需确认影响设备数。

---

## 5. 任务队列

### INC-5-13 调度器重复下发（S1）

**触发**：两个副本同时认为自己是 leader（advisory lock 会话断开后重连，旧 leader 未感知）；或条件 UPDATE 被写成无条件。

**传播**：同一个学生任务下发到两台机器，两份材料报废；或同一台机器收到两个任务，第二个把第一个打断。

**检测信号**：同 item_id 多条 dispatched；同 job_id 出现在两台设备的 JOB_START；uk_item_device_active 唯一冲突日志。

**止血**：停止全部调度器副本（`IOT_FLEET_SCHEDULER=off`）；人工处理在途项。

**根治**：条件 UPDATE + 唯一索引双保险（§7.3）；job_start 幂等键 = item_id 在 deviceapi 层去重；leader 每轮开始重新确认锁仍持有；重复下发对账进每日任务且必须为 0。

---

### INC-5-14 调度器双 leader 或无 leader

**触发**：PG 连接抖动导致 advisory lock 释放，新副本获锁，旧副本因网络分区仍在跑；或全部副本都拿不到锁（锁被僵死会话持有）。

**传播**：双 leader → INC-5-13；无 leader → 队列停摆，学生等待。

**检测信号**：`fleet_scheduler_leader` 计数 ≠ 1；approved 项等待中位数上升。

**止血**：无 leader：`pg_terminate_backend` 僵死会话；双 leader：重启全部副本。

**根治**：leader 用「锁 + 租约时间戳」双重判定；每轮前检查 `pg_advisory_lock` 仍持有且租约未过期；锁会话与业务连接分离。

---

### INC-5-15 任务下发到 locked 或作业中设备

**触发**：调度器读影子时设备空闲未锁，下发瞬间课表切换或本地开始了任务（影子 30 s 新鲜度窗口）。

**传播**：设备端拒绝（DR-501）并回 cmd_ack fail，任务回队列。若设备端校验缺失则打断进行中任务。

**检测信号**：LOCKED_START_DENIED{source:cloud} 与 cmd_ack fail 比例 > 5%。

**止血**：调度器在窗口边界前后 2 分钟不下发。

**根治**：设备端校验是安全边界（BL4 INC-4-11 同一结论）；调度器把 ShouldLock(now+估算时长) 也纳入判定，避免任务跨越锁定边界。

---

### INC-5-16 学生绕过审批

**触发**：auto_approve_materials 配置为通配或包含不该自动的材料；参数档判定 official 的逻辑错误把学生自定义参数也当官方。

**传播**：学生用不允许的材料或危险参数直接加工。

**检测信号**：approved_by=auto 比例 100%；自动批准项的 material_id 不在白名单。

**止血**：清空该队列白名单，全部转人工。

**根治**：白名单只允许 material 表中的官方材料且 param_profile_id 必须为 official 源；配置变更需 org_admin 二次确认；自动批准比例进看板。

---

### INC-5-17 队列文件 URL 过期循环

**触发**：任务在队列等待超过 24 小时，文件预签名 URL 过期；下发时设备下载失败 → skipped → 回队尾 → 再下发再失败。

**传播**：任务永远完成不了，学生不知道原因。

**检测信号**：skipped 项的失败原因为下载类；skip_count ≥ 2 项数上升。

**止血**：对 skip_count ≥ 2 项直接 needs_teacher 并提示重新上传。

**根治**：下发前检查 file_url_expires_at，过期则请求客户端重新上传（状态 needs_reupload）；队列等待超 20 小时主动提醒。

---

## 6. 批量 OTA

### INC-5-18 批量 OTA 在上课时间下发

**触发**：window 的 tz 与 site.tz 不一致；weekdays 用 0 起还是 1 起理解不同；ota-svc InWindow 未实现或被绕过。

**传播**：周一早八点 12 台机器同时进入升级，一节课没了。

**检测信号**：窗口外下发数 > 0（对账 ota_device_task.updated_at vs window）。

**止血**：pause 子批次。

**根治**：InWindow 纯函数表驱动单测覆盖时区与跨午夜；window 的 tz 必填且默认取 site.tz；下发前 fleet-svc 与 ota-svc 双重校验。

---

### INC-5-19 批量 OTA 打满校园带宽

**触发**：站点并发下载限制未生效，200 台同时下载 100 MB 整包。

**传播**：校园网拥塞，其它教学系统受影响，学校投诉。

**检测信号**：站点并发下载 > 5；下载超时率上升。

**止血**：pause；调小并发。

**根治**：fleet-svc 按站点分片 explicit_sns 并串行创建子批次；差分包优先（BL1 FR-24）；窗口内速率上限可按组织配置。

---

### INC-5-20 组织子批次绕过灰度

**触发**：fleet-svc 校验父批次档位的查询错误（查了 fused 批次或未按 firmware_id 过滤）；或运维直接调 ota-svc 建 explicit_sns 批次。

**传播**：某学校成为未经灰度固件的第一批用户，若固件有问题则一所学校全部刷坏。

**检测信号**：子批次 firmware 的父批次最高档位 < 10% 或 fused。

**止血**：pause 子批次；A/B 回滚兜底。

**根治**：ota-svc 层校验 explicit_sns 批次必须引用已达 10% 的 parent_batch_id（不只在 fleet-svc 校验）；PG 触发器或应用层双校验。

---

## 7. 告警

### INC-5-21 告警推送目标查询超时

**触发**：fleet-svc 慢或不可用；alarm-svc 等待 alarm-targets 响应超过 500 ms 超时未生效。

**传播**：告警推送延迟，触达 P99 > 3 s，破 BL1 SLO。

**检测信号**：alarm_targets 延迟；推送 P99；`alarm_targets_fallback` 计数。

**止血**：确认 alarm-svc 的 500 ms 超时与回退生效；不生效则临时关闭 alarm-targets 查询（环境变量），全部回退个人绑定用户，登记。

**根治**：alarm-svc 对 fleet-svc 的调用有超时、熔断、回退三件套且集成测试覆盖「fleet-svc 停机推送仍到达」；订阅关系可选地由 fleet-svc 推到 Redis 供 alarm-svc 直读，去掉同步调用。

---

### INC-5-22 告警推给了已离职老师

**触发**：org_alarm_subscription 未随成员移除清理。

**传播**：离职老师半夜收到学校机器告警；现任老师没收到（若订阅只有一人）。

**检测信号**：订阅表 user_id 在 org_member 中 removed_at 非空。

**止血**：清理订阅；补订阅现任老师。

**根治**：成员移除事务内删除订阅；站点至少一个有效订阅人否则告警 org_admin；每日对账。

---

## 8. 订阅与合规

### INC-5-23 订阅到期误伤课表与告警（P2，S1）

**触发**：entitlement 中间件挂到 `/orgs/*` 根路由；或到期处理把 device_org 也解除了。

**传播**：到期学校的机器课表停止下发、解锁失败、告警订阅失效。学校没付钱不该导致机器锁死或告警丢失。

**检测信号**：到期组织的 schedule / unlock / alarm-subscriptions 路由 403；依赖扫描。

**止血**：回滚；entitlement 中间件全局短路放行（商业损失可接受）。

**根治**：与 BL4 INC-4-23 同一三重锁定；到期处理只改 entitlement 表不碰 device_org；停机冒烟增加「到期组织仍可解锁」用例。

---

### INC-5-24 学生数据超范围采集

**触发**：为了「显示学生姓名」在 org_member 加了 display_name 列；或队列缩略图被持久化；或 queue_item 存了文件名。

**传播**：未成年人数据在设备云持久化，违反试点协议与法规。

**检测信号**：数据字典审计；表结构变更评审。

**止血**：删列并清数据；通知法务。

**根治**：BL5 表结构变更需法务评审；显示名只从 PII 平台按 user_id 实时取；文件与缩略图 24 h 生命周期由对象存储策略兜底。

---

## 9. BL5 特有的临时关闭项模板

沿用 BL1 §10 登记表：

- `POST /internal/unlock-all` 全局解锁
- 调度器停止 `IOT_FLEET_SCHEDULER=off`
- alarm-targets 查询关闭（全部回退个人绑定）
- 看板路由 Retry-After 放慢
- 在线判定阈值 90 s → 150 s
- entitlement 中间件全局短路

---

## 10. 演练计划

| 阶段 | 演练 | 剧本 | 对应事故 |
|---|---|---|---|
| 试点前 | locked 台架三项 | locked 下注入火焰 / 超温 / 急停，三条开始路径被拦 | INC-5-07 INC-5-09 |
| 试点前 | 越权 | A 校成员遍历 B 校全部路由 | INC-5-03 |
| 试点前 | 锁死恢复 | 故意填错课表 → 站点解锁 → fleet-svc 停机后 deviceapi 直写解锁 | INC-5-08 |
| 试点前 | 调度器双副本 | 两个 Scheduler 并发 100 轮，dispatched == 项数 | INC-5-13 INC-5-14 |
| 试点前 | 窗口外 OTA | window 设为过去 → 零下发 | INC-5-18 |
| 试点前 | fleet-svc 停机 | 冒烟五步 + 告警推送到达 | INC-5-21 |
| 试点中 | 夏令时巡检 | 切换日前一周对全部站点跑 ShouldLock 对比 | INC-5-10 |
| 每月 | 合成探针 | locked 探针机安全事件链路 | INC-5-07 |
| P2 | 到期组织 | entitlement 停机与到期组织仍可解锁 | INC-5-23 |

---

## 11. 试点上线前必须补齐的能力

| 能力 | 对应事故 | 优先级 | 状态（2026-09-19） |
|---|---|---|---|
| locked 只改 start_allowed 一个变量，台架三项进固件门禁 | INC-5-07 | P0 红线 | 固件侧；云端已实现 DecideJobStart 仅拦「开始新任务」，pause/stop/告警不受锁影响 |
| 合成探针：站点探针机 locked 下安全事件链路每小时 | INC-5-07 | P0 | 未做 |
| 解锁兜底不依赖 fleet-svc（deviceapi 直写脚本）；应急解锁 U 盘 | INC-5-08 | P0 | 未做：deviceapi PATCH /desired 可直接改 lock，脚本与 U 盘流程未做 |
| 越权测试进 CI；repo 层 SQL 模板化 | INC-5-03 | P0 | 已实现：越权测试（fake Store 单测 + IT）+ 角色矩阵，实机验证跨组织 404，并进 CI guardrails 作业阻断 |
| 授权与订阅在成员变更事务内失效 | INC-5-01 INC-5-22 | P0 | 未做 |
| 调度器条件 UPDATE + uk_item_device_active + 幂等键 + 租约 | INC-5-13 INC-5-14 | P0 | 已实现 ClaimDispatch/ConfirmDispatch/RevertDispatch + uk_item_device_active，IT 验证不重复下发 |
| InWindow / ShouldLock 用 IANA 时区并单测夏令时 | INC-5-10 INC-5-18 | P0 | 已实现，ShouldLock 含夏令时切换日用例，InWindow 26 例表驱动 |
| ota-svc 层校验 explicit_sns 批次的父批次档位 | INC-5-20 | P0 | 已实现 ParentEligible（≥10% 档且未 fused）；并修掉子批次污染平台档位链的 bug |
| alarm-svc 对 fleet-svc 超时 / 熔断 / 回退 + 停机测试 | INC-5-21 | P0 | 已实现 500 ms 超时与回退，实机验证 fleet 停机后告警照常推送并计 fallback；熔断未做 |
| 课表下发限速与后台任务 | INC-5-12 | P0 | 后台对账已实现（每分钟）；限速未做 |
| 归属需设备确认或核对个人绑定用户 | INC-5-04 | P1 | 已实现 ResolveOwnership 裁决并留痕；设备确认未做 |
| 屏幕解锁码 | INC-5-08 | P1 | 未做：固件与 App 侧 |
| 文件过期 needs_reupload | INC-5-17 | P1 | 未做 |
| entitlement 三重锁定 + 到期组织仍可解锁用例 | INC-5-23 | P2 | 未做：P2 |

---

*本文与 docs/incident-premortem-bl1.md、docs/incident-premortem-bl4.md 配套：设备与平台侧看 BL1，入口与业务链路看 BL4，组织与锁定看本文。每次真实事故后按 docs/incident-register-storm.md 的格式复盘并回写对应 INC-5 条目。*
