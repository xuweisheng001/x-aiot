-- BL2 配件与安全生态：配对 / 联动留痕 / 告警关联 / 滤芯寿命 / 联动规则 / 滤芯型号（docs/tech-design-bl2-accessory-ecosystem.md §16.2）
-- 只新增表与函数，不改动 BL1 / BL4 任何表与约束。compose 挂为 05-bl2.sql。

-- ===================== 分片库 iot_shard =====================
SET search_path TO iot_shard;

-- 配对：一台配件同时只配一台主机（部分唯一索引），一台主机可配多台配件。
CREATE TABLE IF NOT EXISTS accessory_pairing (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  host_sn         VARCHAR(32) NOT NULL,
  acc_sn          VARCHAR(32) NOT NULL,
  acc_type        VARCHAR(16) NOT NULL,
  linkage_enabled BOOLEAN     NOT NULL DEFAULT true,
  off_delay_s     INT         NOT NULL DEFAULT 180,
  pair_key        CHAR(64)    NOT NULL,
  paired_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  unpaired_at     TIMESTAMPTZ,
  CONSTRAINT ck_pairing_off_delay CHECK (off_delay_s BETWEEN 60 AND 600),
  CONSTRAINT ck_pairing_not_self  CHECK (host_sn <> acc_sn)
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_pairing_active ON accessory_pairing(acc_sn) WHERE unpaired_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_pairing_host ON accessory_pairing(host_sn) WHERE unpaired_at IS NULL;

-- 联动留痕：按月分区，180 天滚动（与 cmd_audit 同模式）
CREATE TABLE IF NOT EXISTS linkage_audit (
  id         BIGINT GENERATED ALWAYS AS IDENTITY,
  host_sn    VARCHAR(32) NOT NULL,
  acc_sn     VARCHAR(32) NOT NULL,
  trigger    VARCHAR(32) NOT NULL,
  action     JSONB       NOT NULL,
  result     VARCHAR(16) NOT NULL,
  latency_ms INT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_linkage_audit_host ON linkage_audit(host_sn, created_at DESC);

CREATE OR REPLACE FUNCTION iot_shard.ensure_linkage_audit_partitions(months_ahead int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  m0 date := date_trunc('month', now())::date;
  i int; created int := 0; part text; lo date; hi date;
BEGIN
  IF months_ahead < 0 THEN months_ahead := 0; END IF;
  FOR i IN 0..months_ahead LOOP
    lo := (m0 + make_interval(months => i))::date;
    hi := (m0 + make_interval(months => i + 1))::date;
    part := format('linkage_audit_%s', to_char(lo, 'YYYYMM'));
    IF to_regclass('iot_shard.' || part) IS NULL THEN
      EXECUTE format('CREATE TABLE IF NOT EXISTS iot_shard.%I PARTITION OF iot_shard.linkage_audit FOR VALUES FROM (%L) TO (%L)', part, lo, hi);
      created := created + 1;
    END IF;
  END LOOP;
  RETURN created;
END $$;
SELECT iot_shard.ensure_linkage_audit_partitions(1);

-- 配件安全事件关联主机（alarm_id 由 alarm-svc 异步生成，关联键用 acc_sn+code+event_ts）
CREATE TABLE IF NOT EXISTS alarm_context (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  acc_sn          VARCHAR(32) NOT NULL,
  code            VARCHAR(32) NOT NULL,
  event_ts        TIMESTAMPTZ NOT NULL,
  host_sn         VARCHAR(32),
  host_work_state SMALLINT,
  job_id          CHAR(36),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (acc_sn, code, event_ts)
);
CREATE INDEX IF NOT EXISTS idx_alarm_context ON alarm_context(acc_sn, code, event_ts);

-- 滤芯寿命：每小时批更新；health 同步写 consumable_health(part='filter')
CREATE TABLE IF NOT EXISTS filter_life (
  acc_sn           VARCHAR(32)   PRIMARY KEY,
  filter_model     VARCHAR(32)   NOT NULL,
  installed_at     TIMESTAMPTZ   NOT NULL,
  eq_air_volume    NUMERIC(14,2) NOT NULL DEFAULT 0,
  run_seconds      NUMERIC(14,2) NOT NULL DEFAULT 0,
  health           NUMERIC(5,4)  NOT NULL DEFAULT 1,
  predicted_eol_at TIMESTAMPTZ,
  last_hour        TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT ck_filter_health CHECK (health BETWEEN 0 AND 1)
);

ALTER TABLE filter_life ADD COLUMN IF NOT EXISTS run_seconds NUMERIC(14,2) NOT NULL DEFAULT 0;

-- 主机解绑 → 配对解除（跨服务事务用触发器代替）
CREATE OR REPLACE FUNCTION iot_shard.trg_binding_unpair() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.unbound_at IS NOT NULL AND OLD.unbound_at IS NULL THEN
    UPDATE iot_shard.accessory_pairing SET unpaired_at = NEW.unbound_at WHERE host_sn = NEW.sn AND unpaired_at IS NULL;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS binding_unpair ON iot_shard.device_binding;
CREATE TRIGGER binding_unpair AFTER UPDATE OF unbound_at ON iot_shard.device_binding
  FOR EACH ROW EXECUTE FUNCTION iot_shard.trg_binding_unpair();

-- ===================== 全局库 iot_global =====================
SET search_path TO iot_global;

-- 材料 → 风量档位（按配件 product_key 组织；version 单调，引擎取最大 version）
CREATE TABLE IF NOT EXISTS linkage_rule (
  product_key VARCHAR(32) NOT NULL,
  material_id VARCHAR(32) NOT NULL,
  fan_level   SMALLINT    NOT NULL,
  version     BIGINT      NOT NULL,
  PRIMARY KEY (product_key, material_id, version),
  CONSTRAINT ck_linkage_fan_level CHECK (fan_level BETWEEN 1 AND 4)
);

-- 滤芯型号系数（实验室标定）：rated_volume 等效风量上限；flow_coeff {档位: m3/h}；压差折损 p0/p_span/k_p
CREATE TABLE IF NOT EXISTS filter_model (
  model        VARCHAR(32)   PRIMARY KEY,
  rated_volume NUMERIC(14,2) NOT NULL,
  flow_coeff   JSONB         NOT NULL,
  p0           NUMERIC(8,2)  NOT NULL DEFAULT 50,
  p_span       NUMERIC(8,2)  NOT NULL DEFAULT 100,
  k_p          NUMERIC(6,4)  NOT NULL DEFAULT 0.5,
  rpm_levels   JSONB         NOT NULL DEFAULT '[800,1600,2400,3200]',
  version      INT           NOT NULL DEFAULT 1
);

INSERT INTO product(product_key,name,category) VALUES ('ACC_PURIFIER','Smoke Purifier','accessory') ON CONFLICT DO NOTHING;
INSERT INTO filter_model(model, rated_volume, flow_coeff) VALUES ('HEPA-STD', 3600000, '{"1":60,"2":120,"3":200,"4":300}') ON CONFLICT DO NOTHING;
INSERT INTO linkage_rule(product_key, material_id, fan_level, version) VALUES
  ('ACC_PURIFIER','BASSWOOD_3MM',2,1),('ACC_PURIFIER','ACRYLIC_3MM',3,1),('ACC_PURIFIER','LEATHER_1MM',3,1)
ON CONFLICT DO NOTHING;
