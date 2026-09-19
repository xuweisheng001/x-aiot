-- BL4 软件与内容：参数库 / 加工记录与反哺 / 推荐（docs/tech-design-bl4-software-content.md §11）
-- 只新增表，不改动 BL1 任何表与约束。compose 初始化按文件名顺序在 global/shard/cf001 之后执行。

-- ===================== 全局库 iot_global =====================
SET search_path TO iot_global;

CREATE TABLE IF NOT EXISTS material (
  material_id  VARCHAR(32)  PRIMARY KEY,
  name         VARCHAR(64)  NOT NULL,
  category     VARCHAR(32)  NOT NULL,
  thickness_mm NUMERIC(6,2),
  vendor       VARCHAR(64),
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 参数档：按 product_key × module_model × material_id 组织；version_added/version_removed 描述生命周期。
-- 任意 version v 下的有效集合 = version_added <= v AND (version_removed IS NULL OR version_removed > v)。
CREATE TABLE IF NOT EXISTS param_profile (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_key     VARCHAR(32) NOT NULL REFERENCES product(product_key),
  module_model    VARCHAR(32) NOT NULL,
  material_id     VARCHAR(32) NOT NULL REFERENCES material(material_id),
  params          JSONB       NOT NULL,
  source          VARCHAR(16) NOT NULL DEFAULT 'official',
  version_added   BIGINT      NOT NULL,
  version_removed BIGINT,
  sample_count    INT         NOT NULL DEFAULT 0,
  confidence      NUMERIC(5,4),
  approved_by     VARCHAR(64),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_profile_source CHECK (source IN ('official','recommended')),
  -- 参数范围护栏（INC-4-13）：功率百分比 0..100，速度为正；缺字段视为非法
  CONSTRAINT ck_profile_params CHECK (
    (params ? 'power') AND (params ? 'speed')
    AND (params->>'power')::numeric BETWEEN 0 AND 100
    AND (params->>'speed')::numeric > 0
  ),
  CONSTRAINT ck_profile_lifecycle CHECK (version_removed IS NULL OR version_removed > version_added)
);
CREATE INDEX IF NOT EXISTS idx_profile_added   ON param_profile(product_key, version_added);
CREATE INDEX IF NOT EXISTS idx_profile_removed ON param_profile(product_key, version_removed);
CREATE INDEX IF NOT EXISTS idx_profile_lookup  ON param_profile(product_key, module_model, material_id);

-- 发布记录：每个 product_key 的 version 单调递增；回滚 = 发布一个内容等于旧版本的新 version。
CREATE TABLE IF NOT EXISTS param_release (
  product_key      VARCHAR(32) NOT NULL REFERENCES product(product_key),
  version          BIGINT      NOT NULL,
  note             TEXT,
  snapshot_url     TEXT,
  rollout_pct      SMALLINT    NOT NULL DEFAULT 100,
  created_by       VARCHAR(64) NOT NULL,
  approved_by      VARCHAR(64),
  released_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  rolled_back_from BIGINT,
  PRIMARY KEY (product_key, version),
  CONSTRAINT ck_release_rollout  CHECK (rollout_pct BETWEEN 0 AND 100),
  -- 双人审批做成约束（与 ota_batch.ck_full_stage_approved 同一思路）
  CONSTRAINT ck_release_approved CHECK (approved_by IS NOT NULL AND approved_by <> created_by)
);

-- 校正系数配置（P1），随 release 版本化
CREATE TABLE IF NOT EXISTS param_correction_cfg (
  product_key  VARCHAR(32)  NOT NULL REFERENCES product(product_key),
  module_model VARCHAR(32)  NOT NULL,
  a            NUMERIC(6,4) NOT NULL DEFAULT 0.15,
  b            NUMERIC(6,4) NOT NULL DEFAULT 0.05,
  c            NUMERIC(6,4) NOT NULL DEFAULT 0.05,
  rated_hours  NUMERIC(10,2) NOT NULL DEFAULT 10000,
  version      INT          NOT NULL DEFAULT 1,
  updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (product_key, module_model)
);

-- 推荐候选（P1）：reco-job 的输出，param-svc 发布时把 published 的候选写成 source=recommended 的 param_profile。
-- 两个服务通过这张表解耦：reco-job 不碰 param_profile / param_release。
CREATE TABLE IF NOT EXISTS param_recommendation (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_key    VARCHAR(32) NOT NULL,
  module_model   VARCHAR(32) NOT NULL,
  material_id    VARCHAR(32) NOT NULL,
  params_hash    CHAR(64)    NOT NULL,
  params         JSONB,
  sample_count   INT         NOT NULL,
  distinct_users INT         NOT NULL DEFAULT 0,
  good_ratio     NUMERIC(5,4) NOT NULL,
  status         VARCHAR(16) NOT NULL DEFAULT 'candidate',
  removed_reason VARCHAR(64),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_reco_status CHECK (status IN ('candidate','published','removed')),
  UNIQUE (product_key, module_model, material_id, params_hash)
);

INSERT INTO material(material_id,name,category,thickness_mm,vendor) VALUES
  ('BASSWOOD_3MM','椴木板 3mm','wood',3.00,'xTool'),
  ('ACRYLIC_3MM','亚克力 3mm','acrylic',3.00,'xTool'),
  ('LEATHER_1MM','植鞣革 1mm','leather',1.00,'xTool')
ON CONFLICT DO NOTHING;

-- ===================== 分片库 iot_shard =====================
SET search_path TO iot_shard;

-- 加工记录：只含 SN；opt-in 才写（job-svc 以影子 reported.job_feedback_optin 为准，Redis 不可读 → 丢弃）
CREATE TABLE IF NOT EXISTS job_record (
  job_id           CHAR(36)    PRIMARY KEY,
  sn               VARCHAR(32) NOT NULL,
  material_id      VARCHAR(32),
  param_profile_id VARCHAR(32),
  params_hash      CHAR(64),
  started_at       TIMESTAMPTZ NOT NULL,
  finished_at      TIMESTAMPTZ,
  outcome          VARCHAR(16),
  duration_s       INT,
  CONSTRAINT ck_job_outcome CHECK (outcome IS NULL OR outcome IN ('done','fail','pause'))
);
CREATE INDEX IF NOT EXISTS idx_job_sn ON job_record(sn, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_hash ON job_record(material_id, params_hash, started_at DESC);

CREATE TABLE IF NOT EXISTS job_feedback (
  job_id     CHAR(36)    PRIMARY KEY REFERENCES job_record(job_id) ON DELETE CASCADE,
  sn         VARCHAR(32) NOT NULL,
  rating     VARCHAR(8)  NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_feedback_rating CHECK (rating IN ('good','burnt','uncut'))
);
CREATE INDEX IF NOT EXISTS idx_feedback_sn ON job_feedback(sn, created_at DESC);

-- job_record 尚未到达时的标记暂存（INC-4-22）：job-svc 收到 JOB 记录后合并
CREATE TABLE IF NOT EXISTS job_feedback_pending (
  job_id     CHAR(36)    PRIMARY KEY,
  sn         VARCHAR(32) NOT NULL,
  rating     VARCHAR(8)  NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_feedback_pending_rating CHECK (rating IN ('good','burnt','uncut'))
);

-- 用户自定义参数：只含 user_id；version 用于 If-Match 乐观锁（INC-4-16）
CREATE TABLE IF NOT EXISTS user_param (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id      BIGINT      NOT NULL,
  product_key  VARCHAR(32) NOT NULL,
  module_model VARCHAR(32) NOT NULL,
  material_id  VARCHAR(32) NOT NULL,
  params       JSONB       NOT NULL,
  version      BIGINT      NOT NULL DEFAULT 1,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, product_key, module_model, material_id)
);
