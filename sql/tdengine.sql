CREATE DATABASE IF NOT EXISTS iot KEEP 90 DURATION 10 PRECISION 'ms';
CREATE STABLE IF NOT EXISTS iot.telemetry (
  ts TIMESTAMP, seq BIGINT, work_state TINYINT, power_level TINYINT,
  temp_cavity FLOAT, temp_water FLOAT, fan_rpm INT,
  laser_hours FLOAT, progress TINYINT
) TAGS (sn BINARY(32), product_key BINARY(16), fw_version BINARY(16), region BINARY(8), cell_id TINYINT);
CREATE STABLE IF NOT EXISTS iot.events (
  ts TIMESTAMP, seq BIGINT, code BINARY(32), msg BINARY(256),
  job_id BINARY(36), material_id BINARY(32), param_profile_id BINARY(32), params_hash BINARY(64)
) TAGS (sn BINARY(32), product_key BINARY(16));
-- 物模型 v1.1 任务字段：已存在的库靠 ALTER 升级（重复执行返回 code 875 "Column already exists"，EnsureSchema 视为幂等成功）
ALTER STABLE iot.events ADD COLUMN job_id BINARY(36);
ALTER STABLE iot.events ADD COLUMN material_id BINARY(32);
ALTER STABLE iot.events ADD COLUMN param_profile_id BINARY(32);
ALTER STABLE iot.events ADD COLUMN params_hash BINARY(64);
-- 小时级降采样流（BL3 §16.3）：新增 avg_power 与功率档占比 share_low/mid/high 供健康度加权。
-- 流的目标表结构变化不能 ALTER：已有库由 pipeline -init-schema 检测 telemetry_1h 缺列时 DROP STREAM / DROP STABLE 后重建
-- （tdengine.RebuildStreamIfMissing）；历史桶缺 share_* 由 health-svc 用 avg_power 单档兜底。
CREATE STREAM IF NOT EXISTS telemetry_1h_s TRIGGER WINDOW_CLOSE INTO iot.telemetry_1h AS
  SELECT _wstart ts, avg(temp_cavity) avg_temp, max(temp_cavity) max_temp, last(laser_hours) laser_hours,
         avg(power_level) avg_power,
         sum(case when power_level <= 50 then 1 else 0 end) / count(*) share_low,
         sum(case when power_level > 50 and power_level <= 80 then 1 else 0 end) / count(*) share_mid,
         sum(case when power_level > 80 then 1 else 0 end) / count(*) share_high
  FROM iot.telemetry PARTITION BY tbname INTERVAL(1h);
