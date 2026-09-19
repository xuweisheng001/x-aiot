-- iot_shard：设备档案/证书/绑定/影子期望/审计/OTA 任务/告警（技术方案 §17.3，原型只建一个分片）
CREATE SCHEMA IF NOT EXISTS iot_shard;
SET search_path TO iot_shard;

CREATE TABLE IF NOT EXISTS device (
  sn             VARCHAR(32)  PRIMARY KEY,
  product_key    VARCHAR(32)  NOT NULL,
  region         VARCHAR(8)   NOT NULL,
  cell_id        SMALLINT     NOT NULL,
  cell_map_ver   INT          NOT NULL DEFAULT 1,
  fw_version     VARCHAR(32),
  schema_version INT          NOT NULL DEFAULT 1,
  status         VARCHAR(16)  NOT NULL DEFAULT 'manufactured',
  cert_fp        CHAR(64),
  activated_at   TIMESTAMPTZ,
  last_online_at TIMESTAMPTZ,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_device_product ON device(product_key, status);
CREATE INDEX IF NOT EXISTS idx_device_cell    ON device(cell_id);
CREATE INDEX IF NOT EXISTS idx_device_fw      ON device(product_key, fw_version);

CREATE TABLE IF NOT EXISTS device_cert (
  cert_fp        CHAR(64)    PRIMARY KEY,
  sn             VARCHAR(32) NOT NULL,
  issued_at      TIMESTAMPTZ NOT NULL,
  expires_at     TIMESTAMPTZ NOT NULL,
  status         VARCHAR(16) NOT NULL DEFAULT 'active',
  revoked_reason VARCHAR(64)
);
CREATE INDEX IF NOT EXISTS idx_cert_sn ON device_cert(sn);

CREATE TABLE IF NOT EXISTS device_binding (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sn         VARCHAR(32) NOT NULL,
  user_id    BIGINT      NOT NULL,
  role       VARCHAR(16) NOT NULL DEFAULT 'owner',
  bound_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  unbound_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_binding_active ON device_binding(sn, user_id) WHERE unbound_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_binding_user ON device_binding(user_id) WHERE unbound_at IS NULL;

CREATE TABLE IF NOT EXISTS shadow_desired (
  sn         VARCHAR(32) PRIMARY KEY,
  desired    JSONB       NOT NULL DEFAULT '{}'::jsonb,
  version    BIGINT      NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cmd_audit (
  cmd_id     VARCHAR(64)  NOT NULL,
  sn         VARCHAR(32)  NOT NULL,
  action     VARCHAR(32)  NOT NULL,
  params     JSONB,
  operator   VARCHAR(64)  NOT NULL,
  source     VARCHAR(16)  NOT NULL,
  result     VARCHAR(16)  NOT NULL DEFAULT 'dispatched',
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  acked_at   TIMESTAMPTZ,
  PRIMARY KEY (cmd_id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_audit_sn ON cmd_audit(sn, created_at DESC);
-- 预建当月与下月分区（生产由定时任务滚动创建 / DROP 180 天前分区）
DO $$
DECLARE m0 date := date_trunc('month', now())::date; m1 date := (date_trunc('month', now()) + interval '1 month')::date; m2 date := (date_trunc('month', now()) + interval '2 month')::date;
BEGIN
  EXECUTE format('CREATE TABLE IF NOT EXISTS cmd_audit_%s PARTITION OF cmd_audit FOR VALUES FROM (%L) TO (%L)', to_char(m0,'YYYYMM'), m0, m1);
  EXECUTE format('CREATE TABLE IF NOT EXISTS cmd_audit_%s PARTITION OF cmd_audit FOR VALUES FROM (%L) TO (%L)', to_char(m1,'YYYYMM'), m1, m2);
END $$;

CREATE TABLE IF NOT EXISTS ota_device_task (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_id   BIGINT      NOT NULL,
  sn         VARCHAR(32) NOT NULL,
  status     VARCHAR(16) NOT NULL DEFAULT 'pending',
  retry      SMALLINT    NOT NULL DEFAULT 0,
  error_code VARCHAR(32),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (batch_id, sn)
);

CREATE TABLE IF NOT EXISTS alarm (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sn                VARCHAR(32) NOT NULL,
  code              VARCHAR(32) NOT NULL,
  level             VARCHAR(8)  NOT NULL,
  event_ts          TIMESTAMPTZ NOT NULL,
  status            VARCHAR(16) NOT NULL DEFAULT 'open',
  notified_channels VARCHAR(64),
  notified_at       TIMESTAMPTZ,
  acked_at          TIMESTAMPTZ,
  escalated_at      TIMESTAMPTZ,
  closed_by         VARCHAR(64),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alarm_open ON alarm(status) WHERE status IN ('open','notified');

CREATE TABLE IF NOT EXISTS consumable_health (
  sn               VARCHAR(32)  NOT NULL,
  part             VARCHAR(16)  NOT NULL,
  health           NUMERIC(5,2) NOT NULL,
  used_hours       NUMERIC(10,2),
  predicted_eol_at TIMESTAMPTZ,
  notified_at      TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (sn, part)
);

-- ---------------------------------------------------------------------------
-- cmd_audit 分区滚动（INC-24）：deviceapi 启动与每日调用；生产可换 pg_partman。
-- 只新增函数，不改任何表与约束。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION iot_shard.ensure_cmd_audit_partitions(months_ahead int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  m0 date := date_trunc('month', now())::date;
  i int; created int := 0; part text; lo date; hi date;
BEGIN
  IF months_ahead < 0 THEN months_ahead := 0; END IF;
  FOR i IN 0..months_ahead LOOP
    lo := (m0 + make_interval(months => i))::date;
    hi := (m0 + make_interval(months => i + 1))::date;
    part := format('cmd_audit_%s', to_char(lo, 'YYYYMM'));
    IF to_regclass('iot_shard.' || part) IS NULL THEN
      EXECUTE format('CREATE TABLE IF NOT EXISTS iot_shard.%I PARTITION OF iot_shard.cmd_audit FOR VALUES FROM (%L) TO (%L)', part, lo, hi);
      created := created + 1;
    END IF;
  END LOOP;
  RETURN created;
END $$;

-- 删除整月早于 now()-days 的分区（180 天滚动）。返回删除数量。
CREATE OR REPLACE FUNCTION iot_shard.drop_cmd_audit_partitions_older_than(days int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  r record; dropped int := 0; cutoff date := (now() - make_interval(days => days))::date;
  hi date;
BEGIN
  FOR r IN
    SELECT c.relname
      FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
      JOIN pg_class p ON p.oid = h.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace
     WHERE n.nspname = 'iot_shard' AND p.relname = 'cmd_audit'
  LOOP
    -- relname = cmd_audit_YYYYMM；分区上界 = 该月 +1 月
    hi := (to_date(substr(r.relname, 11, 6), 'YYYYMM') + interval '1 month')::date;
    IF hi <= cutoff THEN
      EXECUTE format('DROP TABLE IF EXISTS iot_shard.%I', r.relname);
      dropped := dropped + 1;
    END IF;
  END LOOP;
  RETURN dropped;
END $$;
