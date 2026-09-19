-- BL5 教育与 B 端：组织 / 站点 / 成员 / 归属 / 课表锁 / 任务队列 / 告警订阅 / 组织 OTA（docs/tech-design-bl5-education-b2b.md §11）
-- 只新增表，不改 BL1 / BL4 任何表与约束。所有租户表 org_id 为复合索引首列（租户隔离第一道锁）。

-- ===================== 全局库 iot_global =====================
SET search_path TO iot_global;

CREATE TABLE IF NOT EXISTS org (
  org_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name               VARCHAR(128) NOT NULL,
  type               VARCHAR(16)  NOT NULL,
  parent_id          BIGINT REFERENCES org(org_id),
  region             VARCHAR(8)   NOT NULL DEFAULT 'US',
  unlock_max_minutes INT          NOT NULL DEFAULT 240,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT ck_org_type   CHECK (type IN ('school','studio')),
  CONSTRAINT ck_org_unlock CHECK (unlock_max_minutes BETWEEN 1 AND 1440)
);
CREATE INDEX IF NOT EXISTS idx_org_parent ON org(parent_id);

CREATE TABLE IF NOT EXISTS site (
  site_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id     BIGINT       NOT NULL REFERENCES org(org_id),
  name       VARCHAR(128) NOT NULL,
  tz         VARCHAR(48)  NOT NULL,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT ck_site_tz CHECK (tz <> '')
);
CREATE INDEX IF NOT EXISTS idx_site_org ON site(org_id);

-- 组织 OTA 子批次策略：ota_batch 不改表，显式 SN / 父批次 / 维护窗口放这张伴随表（ota-svc 读它决定窗口外不下发）。
CREATE TABLE IF NOT EXISTS ota_batch_policy (
  batch_id        BIGINT PRIMARY KEY REFERENCES ota_batch(id),
  parent_batch_id BIGINT REFERENCES ota_batch(id),
  explicit_sns    BOOLEAN     NOT NULL DEFAULT false,
  dispatch_window JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ota_policy_parent ON ota_batch_policy(parent_batch_id);

-- ===================== 分片库 iot_shard =====================
SET search_path TO iot_shard;

CREATE TABLE IF NOT EXISTS org_member (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id     BIGINT      NOT NULL,
  user_id    BIGINT      NOT NULL,
  role       VARCHAR(16) NOT NULL,
  invited_by BIGINT,
  joined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at TIMESTAMPTZ,
  CONSTRAINT ck_member_role CHECK (role IN ('org_admin','teacher','student'))
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_member_active ON org_member(org_id, user_id) WHERE removed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_member_user ON org_member(user_id) WHERE removed_at IS NULL;

-- 一台设备同时只属一个组织：sn 主键即护栏
CREATE TABLE IF NOT EXISTS device_org (
  sn           VARCHAR(32) PRIMARY KEY,
  org_id       BIGINT      NOT NULL,
  site_id      BIGINT      NOT NULL,
  assigned_by  BIGINT,
  assigned_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  freeze_until TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_device_org_site ON device_org(org_id, site_id);

CREATE TABLE IF NOT EXISTS schedule_policy (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id     BIGINT      NOT NULL,
  site_id    BIGINT      NOT NULL UNIQUE,
  version    BIGINT      NOT NULL DEFAULT 1,
  tz         VARCHAR(48) NOT NULL,
  weekly     JSONB       NOT NULL DEFAULT '{}'::jsonb,
  overrides  JSONB       NOT NULL DEFAULT '[]'::jsonb,
  updated_by BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_schedule_org ON schedule_policy(org_id, site_id);

-- 锁审计：按月分区，2 年；预建当月起 3 个月，滚动由 ensure 函数负责（与 cmd_audit 同思路）
CREATE TABLE IF NOT EXISTS lock_audit (
  id         BIGINT GENERATED ALWAYS AS IDENTITY,
  org_id     BIGINT      NOT NULL,
  sn         VARCHAR(32),
  action     VARCHAR(24) NOT NULL,
  actor      VARCHAR(64) NOT NULL,
  source     VARCHAR(16) NOT NULL DEFAULT 'fleet',
  reason     VARCHAR(128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at),
  CONSTRAINT ck_lock_action CHECK (action IN ('lock','unlock','temp_unlock','expire','denied_personal','schedule_change','locked_start_denied','lock_state_changed'))
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_lock_audit_org ON lock_audit(org_id, created_at DESC);

CREATE OR REPLACE FUNCTION iot_shard.ensure_lock_audit_partitions(months_ahead int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  m0 date := date_trunc('month', now())::date;
  i int; created int := 0; part text; lo date; hi date;
BEGIN
  IF months_ahead < 0 THEN months_ahead := 0; END IF;
  FOR i IN 0..months_ahead LOOP
    lo := (m0 + make_interval(months => i))::date;
    hi := (m0 + make_interval(months => i + 1))::date;
    part := format('lock_audit_%s', to_char(lo, 'YYYYMM'));
    IF to_regclass('iot_shard.' || part) IS NULL THEN
      EXECUTE format('CREATE TABLE IF NOT EXISTS iot_shard.%I PARTITION OF iot_shard.lock_audit FOR VALUES FROM (%L) TO (%L)', part, lo, hi);
      created := created + 1;
    END IF;
  END LOOP;
  RETURN created;
END $$;
SELECT iot_shard.ensure_lock_audit_partitions(3);

CREATE TABLE IF NOT EXISTS job_queue (
  queue_id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id                  BIGINT       NOT NULL,
  site_id                 BIGINT       NOT NULL,
  name                    VARCHAR(128) NOT NULL,
  class_name              VARCHAR(128),
  auto_approve_materials  JSONB        NOT NULL DEFAULT '[]'::jsonb,
  device_sns              TEXT[]       NOT NULL DEFAULT '{}',
  created_by              BIGINT,
  created_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_queue_org ON job_queue(org_id, site_id);

CREATE TABLE IF NOT EXISTS queue_item (
  item_id             CHAR(36)     PRIMARY KEY,
  org_id              BIGINT       NOT NULL,
  queue_id            BIGINT       NOT NULL REFERENCES job_queue(queue_id),
  submitter_user_id   BIGINT       NOT NULL,
  file_sha256         CHAR(64)     NOT NULL,
  file_url            TEXT,
  file_url_expires_at TIMESTAMPTZ,
  material_id         VARCHAR(32),
  param_profile_id    VARCHAR(32),
  est_minutes         INT          NOT NULL DEFAULT 10,
  status              VARCHAR(16)  NOT NULL DEFAULT 'submitted',
  position            INT          NOT NULL DEFAULT 0,
  approved_by         VARCHAR(64),
  rejected_reason     VARCHAR(128),
  assigned_sn         VARCHAR(32),
  job_id              CHAR(36),
  -- 与 iot_shard.cmd_audit.cmd_id / bl6.cmd_ticket_map.cmd_id 同型：CHAR 定长会给短值补空格，
  -- 读回 Go 后带尾随空格，跨表比较与 API 输出都要额外 TRIM，不值得。
  dispatched_cmd_id   VARCHAR(64),
  retry               SMALLINT     NOT NULL DEFAULT 0,
  skip_count          SMALLINT     NOT NULL DEFAULT 0,
  submitted_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
  approved_at         TIMESTAMPTZ,
  assigned_at         TIMESTAMPTZ,
  dispatched_at       TIMESTAMPTZ,
  started_at          TIMESTAMPTZ,
  finished_at         TIMESTAMPTZ,
  CONSTRAINT ck_item_status CHECK (status IN ('submitted','approved','rejected','canceled','assigned','dispatched','running','done','failed','skipped','needs_teacher'))
);
CREATE INDEX IF NOT EXISTS idx_item_org ON queue_item(org_id, queue_id, status, submitted_at);
-- 同一设备同时只有一个进行中任务：数据库护栏，与调度器条件 UPDATE 双保险（INC-5-13 / 5-14）
CREATE UNIQUE INDEX IF NOT EXISTS uk_item_device_active ON queue_item(assigned_sn) WHERE status IN ('assigned','dispatched','running');
-- 零重复下发：dispatched_cmd_id 唯一
CREATE UNIQUE INDEX IF NOT EXISTS uk_item_cmd ON queue_item(dispatched_cmd_id) WHERE dispatched_cmd_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_item_job ON queue_item(job_id) WHERE job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS org_alarm_subscription (
  org_id     BIGINT      NOT NULL,
  site_id    BIGINT      NOT NULL,
  user_id    BIGINT      NOT NULL,
  channels   VARCHAR(64) NOT NULL DEFAULT 'app',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (site_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_alarm_sub_org ON org_alarm_subscription(org_id, site_id);

CREATE TABLE IF NOT EXISTS org_ota_batch (
  org_id     BIGINT      NOT NULL,
  batch_id   BIGINT      NOT NULL,
  dispatch_window JSONB,
  created_by BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, batch_id)
);

-- ---------------------------------------------------------------------------
-- BL5 §08 批量 OTA：ota_batch 的两列增量。
-- 放这里而不是改 sql/global.sql 的建表语句：global.sql 是 BL1 已经跑过的既有 DDL，
-- 改它对已存在的库毫无作用（CREATE TABLE IF NOT EXISTS 不会补列），只有幂等 ALTER 才能同时
-- 满足「新库从 global.sql 建 + bl5.sql 补」与「老库重复执行 bl5.sql」两种路径。
--   parent_batch_id：组织子批次指向平台父批次，NULL = 平台批次（档位顺序链只看 NULL 的那些）
--   policy        ：下发策略，目前只识别 window {start,end,tz,weekdays}
-- 上面的伴随表 ota_batch_policy 是同一需求的早期设计（不改主表、旁挂一张），代码最终走了本节的两列：
-- 下发主循环每轮都要读 policy 判窗口，多一次 JOIN 不值当。ota_batch_policy 暂留空表不再写入。
-- ---------------------------------------------------------------------------
ALTER TABLE iot_global.ota_batch ADD COLUMN IF NOT EXISTS parent_batch_id BIGINT;
ALTER TABLE iot_global.ota_batch ADD COLUMN IF NOT EXISTS policy JSONB;
DO $$
BEGIN
  ALTER TABLE iot_global.ota_batch ADD CONSTRAINT fk_ota_batch_parent
    FOREIGN KEY (parent_batch_id) REFERENCES iot_global.ota_batch(id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_ota_batch_parent ON iot_global.ota_batch(parent_batch_id) WHERE parent_batch_id IS NOT NULL;
