-- iot_global：产品/物模型/单元/固件/OTA 批次（技术方案 §17.2，原型用 schema 模拟全局库）
CREATE SCHEMA IF NOT EXISTS iot_global;
SET search_path TO iot_global;

CREATE TABLE IF NOT EXISTS product (
  product_key    VARCHAR(32) PRIMARY KEY,
  name           VARCHAR(64)  NOT NULL,
  category       VARCHAR(32)  NOT NULL,
  model_version  INT          NOT NULL DEFAULT 1,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS thing_model (
  product_key    VARCHAR(32) NOT NULL REFERENCES product(product_key),
  schema_version INT         NOT NULL,
  definition     JSONB       NOT NULL,
  released_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (product_key, schema_version)
);
CREATE TABLE IF NOT EXISTS cell (
  cell_id     SMALLINT     PRIMARY KEY,
  region      VARCHAR(8)   NOT NULL,
  mqtt_host   VARCHAR(128) NOT NULL,
  mqtt_port   INT          NOT NULL DEFAULT 8883,
  status      VARCHAR(16)  NOT NULL DEFAULT 'active',  -- active/draining/standby/overloaded
  capacity    INT          NOT NULL,
  updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS firmware (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_key  VARCHAR(32) NOT NULL REFERENCES product(product_key),
  version      VARCHAR(32) NOT NULL,
  full_url     TEXT        NOT NULL,
  full_size    BIGINT      NOT NULL,
  sha256       CHAR(64)    NOT NULL,
  signature    TEXT        NOT NULL,
  release_note TEXT,
  status       VARCHAR(16) NOT NULL DEFAULT 'draft',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (product_key, version)
);
CREATE TABLE IF NOT EXISTS firmware_delta (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  firmware_id  BIGINT      NOT NULL REFERENCES firmware(id),
  from_version VARCHAR(32) NOT NULL,
  delta_url    TEXT        NOT NULL,
  delta_size   BIGINT      NOT NULL,
  sha256       CHAR(64)    NOT NULL,
  UNIQUE (firmware_id, from_version)
);
CREATE TABLE IF NOT EXISTS ota_batch (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  firmware_id     BIGINT       NOT NULL REFERENCES firmware(id),
  stage           VARCHAR(8)   NOT NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'running',
  target_total    INT          NOT NULL DEFAULT 0,
  ok_count        INT          NOT NULL DEFAULT 0,
  fail_count      INT          NOT NULL DEFAULT 0,
  fail_ratio_fuse NUMERIC(5,4) NOT NULL DEFAULT 0.0200,
  created_by      VARCHAR(64)  NOT NULL,
  approved_by     VARCHAR(64),
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  paused_at       TIMESTAMPTZ,
  fused_at        TIMESTAMPTZ,
  CONSTRAINT ck_full_stage_approved
    CHECK (stage <> '100' OR approved_by IS NOT NULL)   -- 全量档强制双人审批：约束即护栏
);

INSERT INTO product(product_key,name,category) VALUES
  ('LM_S1','xTool S1','laser_diode'),('LM_P2','xTool P2','laser_co2'),('ACC_PURIFIER','Smoke Purifier','accessory')
ON CONFLICT DO NOTHING;
INSERT INTO cell(cell_id,region,mqtt_host,mqtt_port,status,capacity) VALUES
  (1,'US','127.0.0.1',1884,'active',1000000),(2,'US','127.0.0.1',1884,'active',1000000),(3,'US','127.0.0.1',1884,'standby',1000000)
ON CONFLICT DO NOTHING;
