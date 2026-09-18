CREATE DATABASE IF NOT EXISTS iot KEEP 90 DURATION 10 PRECISION 'ms';
CREATE STABLE IF NOT EXISTS iot.telemetry (
  ts TIMESTAMP, seq BIGINT, work_state TINYINT, power_level TINYINT,
  temp_cavity FLOAT, temp_water FLOAT, fan_rpm INT,
  laser_hours FLOAT, progress TINYINT
) TAGS (sn BINARY(32), product_key BINARY(16), fw_version BINARY(16), region BINARY(8), cell_id TINYINT);
CREATE STABLE IF NOT EXISTS iot.events (
  ts TIMESTAMP, seq BIGINT, code BINARY(32), msg BINARY(256)
) TAGS (sn BINARY(32), product_key BINARY(16));
CREATE STREAM IF NOT EXISTS iot.telemetry_1h_s TRIGGER WINDOW_CLOSE INTO iot.telemetry_1h AS
  SELECT _wstart ts, avg(temp_cavity) avg_temp, max(temp_cavity) max_temp, last(laser_hours) laser_hours
  FROM iot.telemetry PARTITION BY tbname INTERVAL(1h);
