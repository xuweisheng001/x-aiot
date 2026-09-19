-- BL3 耗材与材料：健康度模型配置 / SKU 映射 / 材料码 / 解释 / 更换记录 / 提醒 / 归因 / 扫码（docs/tech-design-bl3-consumables-materials.md §11）
-- 只新增表，不改 BL1/BL4 任何表与约束。consumable_health(sn, part='module') 复用（used_hours 列存加权时长）。

-- ===================== 全局库 iot_global =====================
SET search_path TO iot_global;

-- 健康度模型配置：按 product_key × module_model 版本化；权重与额定时长带范围约束
CREATE TABLE IF NOT EXISTS health_model_cfg (
  product_key          VARCHAR(32)  NOT NULL REFERENCES product(product_key),
  module_model         VARCHAR(32)  NOT NULL,
  rated_weighted_hours NUMERIC(10,2) NOT NULL,
  w_low                NUMERIC(4,2) NOT NULL DEFAULT 1.0,
  w_mid                NUMERIC(4,2) NOT NULL DEFAULT 1.3,
  w_high               NUMERIC(4,2) NOT NULL DEFAULT 1.8,
  overtemp_penalty     NUMERIC(4,2) NOT NULL DEFAULT 0.5,
  version              INT          NOT NULL DEFAULT 1,
  approved_by          VARCHAR(64),
  created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (product_key, module_model, version),
  CONSTRAINT ck_hcfg_rated   CHECK (rated_weighted_hours > 0),
  CONSTRAINT ck_hcfg_weights CHECK (w_low BETWEEN 0.5 AND 3 AND w_mid BETWEEN 0.5 AND 3 AND w_high BETWEEN 0.5 AND 3),
  CONSTRAINT ck_hcfg_penalty CHECK (overtemp_penalty BETWEEN 0 AND 10)
);

-- SKU 映射：提醒里带出可买的件；变更需双人审批（与 param_release 同思路）
CREATE TABLE IF NOT EXISTS sku_mapping (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  part         VARCHAR(16) NOT NULL,
  product_key  VARCHAR(32) NOT NULL REFERENCES product(product_key),
  module_model VARCHAR(32) NOT NULL DEFAULT '*',
  sku_id       VARCHAR(64) NOT NULL,
  title        VARCHAR(128) NOT NULL,
  compat_note  TEXT,
  sellable     BOOLEAN     NOT NULL DEFAULT true,
  created_by   VARCHAR(64) NOT NULL,
  approved_by  VARCHAR(64),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (part, product_key, module_model),
  CONSTRAINT ck_sku_approved CHECK (approved_by IS NOT NULL AND approved_by <> created_by)
);

-- 材料码：31 字符 base32（material_idx 4B | batch 3B | seq 4B | sig 8B）
CREATE TABLE IF NOT EXISTS material_code (
  code_id     CHAR(31)    PRIMARY KEY,
  material_id VARCHAR(32) NOT NULL REFERENCES material(material_id),
  batch       VARCHAR(16) NOT NULL,
  seq         BIGINT      NOT NULL,
  status      VARCHAR(16) NOT NULL DEFAULT 'active',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at  TIMESTAMPTZ,
  CONSTRAINT ck_matcode_status CHECK (status IN ('active','revoked','suspicious'))
);
CREATE INDEX IF NOT EXISTS idx_matcode_batch ON material_code(batch);

-- 材料 idx ↔ material_id 映射（码里只放 4 字节索引，不放字符串）
CREATE TABLE IF NOT EXISTS material_index (
  material_idx BIGINT      PRIMARY KEY,
  material_id  VARCHAR(32) NOT NULL UNIQUE REFERENCES material(material_id)
);

INSERT INTO health_model_cfg(product_key, module_model, rated_weighted_hours, w_low, w_mid, w_high, overtemp_penalty, version, approved_by)
  VALUES ('LM_S1','LM40',10000,1.0,1.3,1.8,0.5,1,'seed') ON CONFLICT DO NOTHING;
INSERT INTO sku_mapping(part, product_key, module_model, sku_id, title, compat_note, created_by, approved_by) VALUES
  ('module','LM_S1','LM40','SKU-LM40-MOD','xTool S1 40W 激光模块','适配 S1 全系','seed','seed-approver'),
  ('lens','LM_S1','*','SKU-S1-LENS','xTool S1 聚焦镜片','所有模块通用','seed','seed-approver')
ON CONFLICT DO NOTHING;
INSERT INTO material_index(material_idx, material_id) VALUES (1,'BASSWOOD_3MM'),(2,'ACRYLIC_3MM'),(3,'LEATHER_1MM') ON CONFLICT DO NOTHING;

-- ===================== 分片库 iot_shard =====================
SET search_path TO iot_shard;

-- 健康度解释：与 consumable_health(sn, part) 伴随，回答「为什么是 62 分」
CREATE TABLE IF NOT EXISTS health_explain (
  sn             VARCHAR(32)  NOT NULL,
  part           VARCHAR(16)  NOT NULL,
  model_version  INT          NOT NULL,
  used_weighted  NUMERIC(12,3) NOT NULL DEFAULT 0,
  rated          NUMERIC(10,2) NOT NULL,
  share_low      NUMERIC(5,4),
  share_mid      NUMERIC(5,4),
  share_high     NUMERIC(5,4),
  overtemp_count INT          NOT NULL DEFAULT 0,
  penalty        NUMERIC(6,2) NOT NULL DEFAULT 0,
  last_bucket_ts TIMESTAMPTZ,
  last_hours     NUMERIC(12,3),
  module_model   VARCHAR(32),
  levels_sent    VARCHAR(32)  NOT NULL DEFAULT '',
  computed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (sn, part)
);

-- 换模块记录
CREATE TABLE IF NOT EXISTS module_swap (
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sn                   VARCHAR(32) NOT NULL,
  detected_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  from_model           VARCHAR(32),
  to_model             VARCHAR(32),
  reason               VARCHAR(16) NOT NULL,
  predicted_eol_before TIMESTAMPTZ,
  CONSTRAINT ck_swap_reason CHECK (reason IN ('model_change','hours_reset','user_marked','module_hours'))
);
CREATE INDEX IF NOT EXISTS idx_swap_sn ON module_swap(sn, detected_at DESC);

-- 用户「已更换」标记（等待 24 h 内信号确认）
CREATE TABLE IF NOT EXISTS module_swap_mark (
  sn        VARCHAR(32) NOT NULL,
  part      VARCHAR(16) NOT NULL,
  marked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (sn, part)
);

-- 提醒（含 suppressed / deferred）；id 即 reminder_id；不含 user_id
CREATE TABLE IF NOT EXISTS health_reminder (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sn                VARCHAR(32) NOT NULL,
  part              VARCHAR(16) NOT NULL,
  level             VARCHAR(8)  NOT NULL,
  health_at         NUMERIC(5,2),
  sku_id            VARCHAR(64),
  sent_at           TIMESTAMPTZ,
  suppressed_reason VARCHAR(16),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_reminder_level CHECK (level IN ('80','50','20','lens','material'))
);
CREATE INDEX IF NOT EXISTS idx_reminder_sn ON health_reminder(sn, created_at DESC);

-- 下单归因：只含 user_id 与 reminder_id（不与 SN 并存）
CREATE TABLE IF NOT EXISTS reminder_attribution (
  reminder_id   BIGINT       PRIMARY KEY REFERENCES health_reminder(id),
  user_id       BIGINT,
  order_id      VARCHAR(64)  UNIQUE,
  sku_id        VARCHAR(64),
  amount        NUMERIC(12,2),
  clicked_at    TIMESTAMPTZ,
  paid_at       TIMESTAMPTZ,
  attributed_at TIMESTAMPTZ
);

-- 扫码记录（防伪阈值需要 user 维度；1 年后删除）
CREATE TABLE IF NOT EXISTS material_code_scan (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code_id    CHAR(31)    NOT NULL,
  user_id    BIGINT,
  sn         VARCHAR(32),
  verified   BOOLEAN     NOT NULL,
  scanned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scan_code ON material_code_scan(code_id, scanned_at DESC);
