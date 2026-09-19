# BL3 耗材与材料 · 线上事故预推演与处置方案

> Incident Pre-mortem · 对应 docs/prd-bl3-consumables-materials.md 与 docs/tech-design-bl3-consumables-materials.md

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1 · 2026-09-18 |
| 目的 | BL3 上线前把健康度计算、换模块识别与预测、提醒、下单归因、材料防伪、数据外供六条线上最可能的事故推演一遍，每条给触发、传播、检测信号、止血、恢复、根治、演练 |
| 与 BL1 / BL4 预推演的关系 | 设备与平台侧看 docs/incident-premortem-bl1.md，用户入口与 BFF 看 docs/incident-premortem-bl4.md；本文只写 BL3 新增的 20 条。分级、处置四原则、临时关闭登记表沿用 BL1 §0 与 §10 |
| 编号 | INC-3-xx |

**BL3 事故的三个特征**

1. **错误的数字比没有数字更贵**。health 算错会直接触发购买与更换；预测算错会让用户提前或延后备件。所有止血都先「停止写入 / 撤回提醒」，再修算法。
2. **事故会传导到 BL4 与 BL6**。BL4 校正系数吃 health，BL6 保修判定吃 laser_hours 与更换记录；BL3 的错误在别的业务线以另一种形态出现。
3. **商业动作不可逆**。用户已经下单、已经换了模块、已经因假码烧了材料，回滚数据不能回滚现实。检测靠对账与看板阈值，不靠投诉。

---

## 1. 事故清单总览

| ID | 事故 | 线 | 级别 | 第一检测信号 | 止血时限 |
|---|---|---|---|---|---|
| INC-3-01 | 权重或额定时长配置错，全机型 health 集体跳变 | 健康度 | S2 | health 分布小时环比位移 > 10 分 | 15 min |
| INC-3-02 | 输入缺失被算成 0，大量「需更换」误提醒 | 健康度 | **S1** | skipped 为 0 但 health=0 设备数突增；20 档提醒数突增 | 5 min |
| INC-3-03 | 批作业挂或超时，health 陈旧，BL4 校正与 BL6 判定用旧值 | 健康度 | S3 | stale 比例 > 1%；batch_duration 超 SLO | 30 min |
| INC-3-04 | laser_hours 乱序或回退未识别为异常，加权时长虚增或虚减 | 健康度 | S2 | hours_regress / clipped 计数上升；health 非单调计数 | 1 h |
| INC-3-05 | telemetry_1h 流重建后历史桶缺列，兜底口径与新口径混用 | 健康度 | S3 | 同设备相邻桶 share_* 为 NULL 与非 NULL 混合 | 1 h |
| INC-3-06 | 换模块漏检：用户换了新模块 health 仍显示 20，持续提醒更换 | 预测 | S2 | 用户点「已更换」后 24 h 未确认的比例 > 10% | 1 h |
| INC-3-07 | 换模块误检：固件重置 laser_hours 被当成更换，health 错误重置为 100 | 预测 | S2 | module_swap reason=hours_reset 在某固件版本集中 | 1 h |
| INC-3-08 | SKU 映射错，批量买错件 | 归因 | **S1** | 该映射版本订单退货率 > 基线 2 倍；客服工单 | 15 min |
| INC-3-09 | 冷却失效，提醒风暴 | 提醒 | S2 | 单 SN 7 天内 sent > 1；打扰率上升 | 15 min |
| INC-3-10 | 阈值抖动：health 在 50 附近来回，反复跨档 | 提醒 | S3 | 同 SN 同档 suppressed 数上升 | 1 h |
| INC-3-11 | 提醒弹在加工中或 deferred 队列丢失 | 提醒 | S3 | deferred 计数与后续 sent 不匹配；投诉 | 1 h |
| INC-3-12 | 商城回调丢失或重复，归因偏低或偏高 | 归因 | S3 | 每日对账差 > 1% | 24 h |
| INC-3-13 | 材料码密钥泄露或映射错，假材料带出官方参数烧材 | 防伪 | **S1** | 同批次 verified=true 但 suspicious 激增；burnt 标记集中在某 batch | 立即 |
| INC-3-14 | 验签服务不可用，客户端只做格式校验放行 | 防伪 | S2 | verify 5xx 比例；客户端「未联网验证」比例 | 15 min |
| INC-3-15 | 正版码被误判 suspicious，用户扫码被拒 | 防伪 | S3 | suspicious 数突增且集中于教育账号（多人共用） | 1 h |
| INC-3-16 | 外供接口 stale 标记失效，BL4 用陈旧 health 校正 | 外供 | S2 | BL4 correction 请求中 health 的 updated_at 分布 | 15 min |
| INC-3-17 | 保修接口口径变更未版本化，BL6 判定结果漂移 | 外供 | S2 | BL6 争议率上升；接口 model_version 变化无公告 | 1 h |
| INC-3-18 | health 与 user_id 出现在同一导出或表中 | 合规 | **S1** | 数据字典审计 | 立即 |
| INC-3-19 | 批作业两轮重叠运行，累计量重复累加 | 运维 | S2 | used_weighted 单桶增量 > 1 h × 权重上限 | 15 min |
| INC-3-20 | 商城可售状态同步失败，提醒跳转死链 | 归因 | S3 | click 后 302 目标 404 率 | 1 h |

---

## 2. 健康度计算

### INC-3-01 配置错导致集体跳变

**触发**：运营把 rated_weighted_hours 从 10000 填成 1000，或 w_high 填成 18；配置生效后下一轮批作业全机型 health 跳变。

**传播**：跳变过 80 / 50 / 20 档 → 大量提醒 → 购买；BL4 校正系数随 health 变化上调功率。

**检测信号**：按 product_key × module_model 的 health 分布小时环比位移 > 10 分即告警；配置变更后首轮批作业自动与上一轮比对，位移 > 10 分暂停写入等人工确认。

**止血**：配置回滚到上一 version（配置表按 version 追加，不覆盖）；本轮已写入的 consumable_health 按 model_version 回滚（health_explain 保留了上一轮值）；已发提醒撤回并推送更正文案。

**根治**：配置变更走双人审批与 CHECK 范围；首轮「影子运行」只算不写，比对通过才生效。

**演练**：staging 把 rated 改小 10 倍，验证影子运行拦截。

---

### INC-3-02 输入缺失算成 0（S1）

**触发**：代码把 telemetry_1h 无行当作 laser_hours=0 且 Δ 计算得到「用完了」；或配置缺失时默认 rated=0 导致除零后 clamp 到 0。

**传播**：一夜之间数万台 health=0 → 20 档「建议更换」提醒 → 用户下单 → 客服爆量 → 信任崩塌。这是 BL3 最贵的事故。

**检测信号**：health=0 设备数小时环比 > 100 台；skipped 计数为 0 而 total 正常（说明缺失没被识别为缺失）；20 档提醒数环比 > 5 倍。批级缺失率熔断应先于此触发。

**止血（5 分钟内）**：暂停批作业；对本轮 model_version 的写入按 health_explain 上一值回滚；撤回本轮 20 档提醒并推送更正；商城侧对本轮 reminder_id 的订单标记「可无理由退换」。

**根治**：缺失不写 0 是代码原则并有单测锁定（缺 rows / cfg / model 三种）；批级缺失率熔断；health 单调性检查（0 的跳变一定非单调）；20 档提醒增加二次确认（连续两轮 ≤ 20 才发）。

---

### INC-3-03 批作业挂或超时

**触发**：TDengine 慢查询；PG 分片锁等待；health-svc 全部副本挂。

**传播**：health 陈旧 → 外供接口 stale=true → BL4 退回 k=1（正确降级）、BL6 判定用旧数据（需人工注意）→ 提醒延迟。

**检测信号**：stale 比例 > 1%；batch_duration > 10 min；连续两轮无写入。

**止血**：重启服务；TDengine 慢查询改按 cell 范围查询（方案 §13.1）。

**根治**：批作业按 cell 分片多副本；每轮开始写心跳，上一轮未结束下一轮跳过并告警（INC-3-19）。

---

### INC-3-04 laser_hours 乱序或回退

**触发**：设备重连后补报旧数据；固件 bug 让 laser_hours 偶发回退；乱序到达导致相邻桶 Δ 为负。

**传播**：Δ<0 若被累加会让 used_weighted 减少（health 上升，被单调性拦下），Δ 异常大若不截断则虚增（health 骤降，触发提醒）。

**检测信号**：hours_regress / hours_clipped 计数上升；health_nonmonotonic 计数。

**止血**：对受影响 SN 触发 recompute（幂等）。

**根治**：Δ 截断规则与单测；连续 2 桶回落才判更换（INC-3-07）；异常率进看板按固件版本切片交固件团队。

---

### INC-3-05 流重建后口径混用

**触发**：telemetry_1h 流增加 share_* 列后重建，历史桶无该列；兜底逻辑用 avg_power 单档，与新桶三档加权口径不一致。

**传播**：同一设备累计量前后口径不一，health 曲线出现斜率突变。

**检测信号**：share_* NULL 与非 NULL 在同 SN 相邻桶混合的比例。

**止血**：无需紧急动作；口径差异在 explain 中标注 `bucket_fallback_count`。

**根治**：流重建同时对历史用 health-svc 的 backfill 命令按 telemetry 原始表重算 share_*（原始表 KEEP 90 天内可算）。

---

## 3. 换模块识别与预测

### INC-3-06 换模块漏检

**触发**：新模块与旧模块 module_model 相同（同型号更换）且固件未重置 laser_hours（或重置后单桶被当乱序）；用户点「已更换」但 24 h 内无信号确认。

**传播**：health 仍 20，持续「建议更换」；用户认为系统不可信。

**检测信号**：user_marked 后 24 h 未确认比例 > 10%；同型号更换占比。

**止血**：用户标记 48 h 内无矛盾信号（laser_hours 未继续大幅增长）→ 接受用户标记为更换（reason=user_marked_accepted），health 重置。

**根治**：DR-303 模块计时芯片上报 module_hours 是最可靠信号，推动进硬件排期；同型号更换要求固件在检测到模块重新插拔时重置 laser_hours 并上报 MODULE_SWAPPED 事件。

---

### INC-3-07 换模块误检

**触发**：某固件版本升级后 laser_hours 计数从 0 重新累计（固件 bug），被判定 hours_reset 更换 → health 重置 100。

**传播**：老模块显示满健康度 → 不提醒 → 用户在模块寿命末期无预警停机；BL6 保修判定以为是新模块。

**检测信号**：module_swap reason=hours_reset 在某 fw_version 集中（> 该版本设备的 5%）；无对应 module_model 变化。

**止血**：对该固件版本的 hours_reset 判定暂停；已重置设备用 health_explain 上一值恢复累计（recompute since 上次真实更换）。

**根治**：hours_reset 判定要求同时满足「无 OTA 事件在前 24 h」；固件 OTA 后的 laser_hours 连续性纳入 OTA 验收项。

---

## 4. 提醒

### INC-3-09 冷却失效提醒风暴

**触发**：last_sent_at 读取用了 health_reminder 最新一行（可能是 suppressed 行，sent_at 为空）→ 判定为「从未发过」；或多副本各自判定。

**传播**：同一设备每小时一条提醒；打扰率飙升；用户关闭提醒甚至联网。

**检测信号**：单 SN 7 天内 sent > 1 的设备数 > 0（对账 SQL 每小时）；打扰率日环比。

**止血**：全局关闭提醒发送（仍记录判定），登记；给受影响用户推送一次致歉并默认延长冷却到 30 天。

**根治**：冷却判定用 `max(sent_at)` 而非最新行；提醒发送由单实例或 PG advisory lock 串行；对账 SQL 进每小时任务。

---

### INC-3-10 阈值抖动

**触发**：health 在 50.2 与 49.8 间因输入噪声来回；跨档判定每次都触发（虽然被冷却抑制，但 suppressed 行暴涨，且 7 天后会再发）。

**检测信号**：同 SN 同档 suppressed 数 > 3 / 周。

**根治**：跨档带滞回（回升需 > level + 3 才重新武装该档）；levels_sent 在更换前不清零，同档终身一次已在设计中，抖动只影响 suppressed 计数，不影响用户。

---

### INC-3-11 加工中弹出或 deferred 丢失

**触发**：work_state 读影子时影子陈旧（设备刚开始作业，影子还是 0）；或 Redis 重启丢失 health:pending。

**传播**：加工中弹出打扰；或 deferred 后永远不发。

**检测信号**：deferred 计数与 24 h 内后续 sent 数差 > 5%；用户投诉。

**止血**：pending 改存 PG health_reminder(suppressed_reason='deferred') 并由批作业下一轮重判。

**根治**：作业中判定同时看影子 updated_at 新鲜度（> 90 s 视为未知，按作业中处理，宁可延后）；deferred 持久化到 PG。

---

## 5. 归因与商城

### INC-3-08 SKU 映射错（S1）

**触发**：运营把 LM40 的模块 SKU 填成 LM20；或映射按 product_key 未区分 module_model。

**传播**：所有该组合的提醒都带错误 SKU → 用户一键下单 → 收货不匹配 → 退货、差评、客服。

**检测信号**：按映射版本切片的退货率 > 基线 2 倍；客服工单关键词「不匹配」；映射变更后 24 h 内该 SKU 订单量异常。

**止血（15 分钟内）**：映射回滚到上一版本；商城侧对 ref=xt_reminder 且 sku 为该值的未发货订单拦截；从 reminder_attribution 拉出已下单用户主动联系换货。

**根治**：双人审批 CHECK；映射变更灰度（先 10% 提醒带新映射）；退货率按映射版本进看板并设自动回滚阈值；商品页显示「适配机型 / 模块」供用户二次确认。

---

### INC-3-12 商城回调丢失或重复

**触发**：商城回调重试导致同 order_id 两次；网络问题回调丢失。

**传播**：重复 → 归因偏高（被 order_id UNIQUE 拦下则无影响）；丢失 → 转化率偏低，运营误判提醒无效。

**检测信号**：每日对账差 > 1%。

**止血**：按商城导出补录。

**根治**：order_id UNIQUE + reminder_id PK 双幂等；主动轮询商城订单接口补偿；报转化率先报对账差。

---

### INC-3-20 可售状态同步失败

**触发**：商城同步任务失败，sellable 停留在 true 而商品已下架。

**传播**：提醒购买按钮跳转 404；用户体验差且转化率失真。

**止血**：sellable 同步失败超 2 小时 → 全部按不可售处理（显示「联系客服」而非死链）。

**根治**：同步心跳监控；302 前实时探测商品页可用性（HEAD，300 ms 超时，失败降级）。

---

## 6. 材料防伪

### INC-3-13 密钥泄露或映射错（S1）

**触发**：批次密钥从供应链工位泄露，假材料印上可验签的码；或 material_code.material_id 登记错（3 mm 码登记为 5 mm）。

**传播**：假材料或错材料带出官方参数 → 烧焦、切不透，极端情况起火；用户信任「官方码」而不试切。

**检测信号**：某 batch 的 burnt / uncut 标记率 > 同材料基线 2 倍（BL4 job_feedback）；同 batch suspicious 激增；供应链报告丢失。

**止血（立即）**：吊销该 batch 密钥与全部码（status=revoked）；客户端对 revoked 码提示「请手动选择材料」；推送使用过该批次的用户「请核对材料厚度」。

**根治**：密钥按批次派生可单独吊销；登记走双人核对（扫样品码回读 material_id）；burnt 率按 batch 切片进看板并设自动 suspicious。

---

### INC-3-14 验签不可用放行

**触发**：health-svc 挂；客户端只做格式校验并带参数。

**传播**：假码在离线期间被当正版（格式校验能被逆向绕过）。

**检测信号**：verify 5xx；客户端上报「未联网验证」比例 > 5%。

**止血**：客户端策略：未联网验证的码可带参数但显示黄色提示「未验证」，且不写 material_code_scan.verified=true；恢复后补验。

**根治**：verify 接口多副本；格式校验只用于用户体验不作为信任来源（设计原则）。

---

### INC-3-15 正版码误判 suspicious

**触发**：教育机构一个材料包多人共用，同码 30 天内 > 5 账号扫描。

**传播**：正版用户被提示可疑，扫码不带参数。

**检测信号**：suspicious 集中于 BL5 教育账号或同一 IP 段。

**止血**：suspicious 只提示不阻断（设计中）；对 BL5 组织账号按组织计数而非按用户。

**根治**：阈值按账号类型区分；同组织内成员扫描算一个 distinct。

---

## 7. 数据外供与合规

### INC-3-16 stale 标记失效

**触发**：接口用缓存返回，updated_at 是缓存时间而非 consumable_health 的；或 stale 阈值配置错。

**传播**：BL4 用陈旧 health 校正功率；BL6 用陈旧数据判定。

**检测信号**：对账 GET /health 返回的 updated_at 与表中值差 > 1 min 的比例。

**止血**：关闭接口缓存。

**根治**：stale 由数据库字段计算不依赖缓存；接口契约测试断言 stale 语义。

---

### INC-3-17 保修口径未版本化

**触发**：health-svc 改了加权口径但 GET /warranty 返回字段不带 model_version；BL6 用新口径判定历史争议。

**传播**：同一设备昨天「正常使用」今天「超负荷使用」，保修争议率上升。

**止血**：接口立即加 model_version，BL6 按判定当时版本复算。

**根治**：所有外供接口带 model_version；口径变更公告与双业务线验收。

---

### INC-3-18 health 与 user_id 同表（S1）

**触发**：为方便看板把 reminder_attribution 与 health_reminder 做成宽表并导出；或 material_code_scan 与 consumable_health JOIN 导出含 user_id 与 health。

**传播**：设备使用行为与个人身份在同一处出现，违反数据分级承诺。

**检测信号**：数据字典审计；导出任务的列名扫描（同时含 user_id 与 health / laser_hours 即拦截）。

**止血**：删除导出；审计影响范围。

**根治**：看板查询只允许经聚合视图；导出任务列白名单进 CI。

---

## 8. 运维

### INC-3-19 批作业重叠运行

**触发**：上一轮超 1 小时未结束，下一轮定时启动；或多副本同时跑。

**传播**：同一小时桶被累加两次 → used_weighted 虚增 → health 骤降 → 提醒。

**检测信号**：health_explain 单桶增量 > 1 h × w_high；批心跳重叠。

**止血**：停掉重叠实例；对受影响 SN recompute。

**根治**：PG advisory lock 保证单轮；last_bucket_ts 严格大于比较天然幂等（同桶不会二次累加，设计已保证，此事故主要影响性能与锁竞争）。

---

## 9. 演练计划

| 阶段 | 演练 | 剧本 | 对应事故 |
|---|---|---|---|
| P1 迭代 1 | 缺失熔断 | 停 telemetry_1h 写入后跑批，验证整轮不写且告警 | INC-3-02 |
| P1 迭代 1 | 配置影子运行 | 把 rated 改小 10 倍，验证拦截 | INC-3-01 |
| P1 迭代 1 | 换模块 | 模拟器 -module-swap 与 hours 归零两种，验证识别与重置 | INC-3-06 / 07 |
| P1 迭代 2 | 冷却 | 模拟器 -laser-hours-rate 3600 让 health 一小时跨三档，验证只发一次 | INC-3-09 / 10 |
| P1 迭代 2 | 假码 | 用错误密钥生成 100 个码，验证 100% 拒；同码 6 账号 suspicious | INC-3-13 / 15 |
| P1 迭代 2 | 映射回滚 | staging 发错映射 → 回滚计时 | INC-3-08 |
| P1 出口 | 安全隔离 | 停 health-svc 跑 BL1 冒烟五步 | 架构约束 |
| 每月 | 对账巡检 | 归因对账差、stale 比例、user_id/health 同表扫描 | INC-3-12 / 16 / 18 |

---

## 10. P1 出口前必须补齐的能力

| 能力 | 对应事故 | 优先级 | 状态（2026-09-19） |
|---|---|---|---|
| 缺失不写 0 单测锁定 + 批级缺失率熔断 + 20 档二次确认 | INC-3-02 | P1 | 已实现 ComputeHealth + ShouldFuseBatch，单测与集成测试锁定；20 档二次确认未做 |
| 配置版本化 + 首轮影子运行比对 | INC-3-01 | P1 | 配置已版本化 health_model_cfg.version；影子运行比对未做 |
| health 单调性检查与 explain 保留上一值（可回滚） | INC-3-01 / 02 / 04 | P1 | 已实现 Monotonic + health_explain |
| 冷却用 max(sent_at)、发送串行化、每小时对账 SQL | INC-3-09 | P1 | 已实现 LastSentAt + runMu 串行 + 每小时冷却对账（只报不改） |
| deferred 持久化 PG、影子新鲜度判定 | INC-3-11 | P1 | deferred 已落 health_reminder.suppressed_reason；影子新鲜度判定未做 |
| sku_mapping 双人审批 CHECK + 退货率按版本看板 | INC-3-08 | P1 | 已实现 ck_sku_approved；看板未做 |
| 材料码按批次派生密钥、可吊销、burnt 率按 batch 看板 | INC-3-13 | P1 | 已实现 DeriveBatchKey 与 status 吊销，单测覆盖跨批次验签失败；看板未做 |
| 外供接口带 model_version 与 stale 由 DB 字段计算 | INC-3-16 / 17 | P1 | 已实现，集成测试覆盖 stale |
| 导出列白名单进 CI | INC-3-18 | P1 | 未做 |
| 批作业 advisory lock + 心跳 | INC-3-19 | P1 | 未做：当前只有进程内 runMu，多副本需 advisory lock |
| hours_reset 判定排除 OTA 后 24 h | INC-3-07 | P1 | 未做 |
| 商城可售探测与死链降级 | INC-3-20 | P1 | 未做 |

---

*本文与 docs/incident-premortem-bl1.md、docs/incident-premortem-bl4.md 配套：设备与平台看 BL1，入口与 BFF 看 BL4，耗材决策链路看本文。每次真实事故后按 docs/incident-register-storm.md 的格式复盘，并把新教训回写到对应 INC-3 条目。*
