-- BL6 售后与保修：诊断包 / 客服授权 / 保修汇总 / Agent 留痕 / 错误码字典 / 批次缺陷（docs/tech-design-bl6-aftersales-warranty.md §11）
-- 只新增表，不改动 BL1 / BL4 任何表与约束。

-- ===================== 分片库 iot_shard =====================
SET search_path TO iot_shard;

-- 诊断包：content 由结构体白名单生成，只含 SN 与设备数据；30 天过期
CREATE TABLE IF NOT EXISTS diagnostic_bundle (
  bundle_id  CHAR(36)    PRIMARY KEY,
  sn         VARCHAR(32) NOT NULL,
  trigger    VARCHAR(8)  NOT NULL,
  ticket_id  VARCHAR(64),
  content    JSONB       NOT NULL,
  sources    JSONB       NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT ck_bundle_trigger CHECK (trigger IN ('user','support','agent'))
);
CREATE INDEX IF NOT EXISTS idx_bundle_sn      ON diagnostic_bundle(sn, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bundle_expires ON diagnostic_bundle(expires_at);

-- 客服授权：requested → granted（用户确认，15 分钟）→ 过期靠 expires_at；denied / revoked 终态
CREATE TABLE IF NOT EXISTS support_grant (
  grant_id     CHAR(36)    PRIMARY KEY,
  sn           VARCHAR(32) NOT NULL,
  ticket_id    VARCHAR(64) NOT NULL,
  operator     VARCHAR(64) NOT NULL,
  actions      TEXT[]      NOT NULL DEFAULT ARRAY['self_check'],
  status       VARCHAR(12) NOT NULL DEFAULT 'requested',
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  granted_at   TIMESTAMPTZ,
  expires_at   TIMESTAMPTZ,
  revoked_at   TIMESTAMPTZ,
  denied_at    TIMESTAMPTZ,
  CONSTRAINT ck_grant_status  CHECK (status IN ('requested','granted','denied','revoked')),
  -- P0 只允许自检：约束即护栏
  CONSTRAINT ck_grant_actions CHECK (actions <@ ARRAY['self_check']::text[])
);
CREATE INDEX IF NOT EXISTS idx_grant_sn ON support_grant(sn, status, expires_at);

-- 指令 → 工单映射：客服/Agent 发起的指令回填到哪张工单，SN 一并落库供回填校验
CREATE TABLE IF NOT EXISTS cmd_ticket_map (
  cmd_id     VARCHAR(64) PRIMARY KEY,
  ticket_id  VARCHAR(64) NOT NULL,
  grant_id   CHAR(36)    NOT NULL,
  sn         VARCHAR(32) NOT NULL,
  source     VARCHAR(16) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmd_ticket ON cmd_ticket_map(ticket_id, created_at DESC);

-- 保修数据快照：summary 只有数据，signals 每条带 evidence / threshold；不含任何判定字段
CREATE TABLE IF NOT EXISTS warranty_case (
  case_id      CHAR(36)    PRIMARY KEY,
  sn           VARCHAR(32) NOT NULL,
  ticket_id    VARCHAR(64),
  summary      JSONB       NOT NULL,
  signals      JSONB       NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_warranty_sn ON warranty_case(sn, generated_at DESC);

-- Agent 每次调用留痕
CREATE TABLE IF NOT EXISTS agent_call_log (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  agent_id   VARCHAR(64) NOT NULL,
  ticket_id  VARCHAR(64),
  sn         VARCHAR(32) NOT NULL,
  endpoint   VARCHAR(64) NOT NULL,
  grant_id   CHAR(36),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agent_log_sn ON agent_call_log(sn, created_at DESC);

-- ===================== 全局库 iot_global =====================
SET search_path TO iot_global;

-- 错误码字典：版本化；发布双人审批做成约束（与 ota_batch / param_release 同一思路）
CREATE TABLE IF NOT EXISTS error_code_dict (
  code             VARCHAR(32) NOT NULL,
  version          BIGINT      NOT NULL,
  product_keys     TEXT[]      NOT NULL DEFAULT '{}',
  severity         VARCHAR(8)  NOT NULL DEFAULT 'info',
  cause            TEXT        NOT NULL,
  steps            TEXT        NOT NULL,
  need_service     BOOLEAN     NOT NULL DEFAULT false,
  defect_threshold INT,
  created_by       VARCHAR(64) NOT NULL,
  approved_by      VARCHAR(64),
  released_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (code, version),
  CONSTRAINT ck_dict_severity CHECK (severity IN ('info','warn','critical')),
  CONSTRAINT ck_dict_approved CHECK (approved_by IS NOT NULL AND approved_by <> created_by)
);

-- 批次缺陷告警：同 (product_key, fw_version, code, 窗口日) 唯一；冷却由 cooldown_until 判定
CREATE TABLE IF NOT EXISTS defect_alert (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_key    VARCHAR(32) NOT NULL,
  fw_version     VARCHAR(32) NOT NULL,
  error_code     VARCHAR(32) NOT NULL,
  device_count   INT         NOT NULL,
  event_count    INT         NOT NULL,
  online_count   INT         NOT NULL DEFAULT 0,
  first_seen     TIMESTAMPTZ,
  window_start   DATE        NOT NULL,
  ticket_id      VARCHAR(64),
  notified_at    TIMESTAMPTZ,
  cooldown_until TIMESTAMPTZ NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (product_key, fw_version, error_code, window_start)
);
CREATE INDEX IF NOT EXISTS idx_defect_combo ON defect_alert(product_key, fw_version, error_code, created_at DESC);

-- 机型级默认阈值（字典 defect_threshold 为 NULL 时回退到这里；两者都缺用代码默认 20）
CREATE TABLE IF NOT EXISTS defect_threshold (
  product_key    VARCHAR(32)  PRIMARY KEY,
  min_devices    INT          NOT NULL DEFAULT 20,
  min_ratio      NUMERIC(5,4) NOT NULL DEFAULT 0.0100,
  cooldown_hours INT          NOT NULL DEFAULT 24
);

-- 保修基线（每日批），signals 的 P90 比较依据
CREATE TABLE IF NOT EXISTS warranty_baseline (
  product_key VARCHAR(32) NOT NULL,
  metric      VARCHAR(32) NOT NULL,
  p50         NUMERIC(12,4),
  p90         NUMERIC(12,4),
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (product_key, metric)
);

INSERT INTO error_code_dict(code, version, product_keys, severity, cause, steps, need_service, created_by, approved_by) VALUES
  ('E_LASER_WEAK', 1, ARRAY['LM_S1'], 'warn', '激光模块出光减弱：镜片污染或模块老化', '1 清洁保护镜片；2 用标准测试卡试切；3 仍不达标联系售后', false, 'seed', 'seed-approver'),
  ('E_FAN_STALL',  1, ARRAY['LM_S1'], 'critical', '排烟风扇停转或转速异常', '1 检查风扇是否被异物卡住；2 检查线缆；3 需要更换风扇请联系售后', true, 'seed', 'seed-approver'),
  ('E_CAMERA_OFFLINE', 1, ARRAY['LM_S1'], 'info', '摄像头未响应', '1 重启机器；2 检查摄像头排线；3 仍未恢复联系售后', false, 'seed', 'seed-approver')
ON CONFLICT DO NOTHING;
