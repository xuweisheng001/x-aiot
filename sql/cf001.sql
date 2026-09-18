-- cf001：代工机型激活体系（配额工单 / digest 映射 / 已注册设备）
CREATE SCHEMA IF NOT EXISTS cf001;
SET search_path TO cf001;

CREATE TABLE IF NOT EXISTS oem_quotas (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_no    VARCHAR(32) NOT NULL UNIQUE,
  supplier    VARCHAR(64) NOT NULL,
  product_key VARCHAR(32) NOT NULL,
  quota       INT         NOT NULL CHECK (quota >= 0),
  registered  INT         NOT NULL DEFAULT 0 CHECK (registered >= 0),
  status      SMALLINT    NOT NULL DEFAULT 1,   -- 1 进行中 / 2 已完成
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_quota_not_exceeded CHECK (registered <= quota)
);
CREATE TABLE IF NOT EXISTS digest_maps (
  digest     CHAR(64)    PRIMARY KEY,
  uuid       VARCHAR(64) NOT NULL,
  mcu_sn     VARCHAR(64) NOT NULL,
  soc_sn     VARCHAR(64) NOT NULL,
  mac        VARCHAR(32) NOT NULL,
  sn         CHAR(23)    NOT NULL UNIQUE,
  order_no   VARCHAR(32) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS oem_devices (
  sn            CHAR(23)    PRIMARY KEY,
  digest        CHAR(64)    NOT NULL REFERENCES digest_maps(digest),
  signature     TEXT        NOT NULL,
  product_key   VARCHAR(32) NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
