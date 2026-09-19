// Package tdengine 是 taosAdapter REST 客户端，含按 SN 分组的多表多行攒批写入。
// 原型阶段刻意走 REST：与技术方案 §6.3 / 手册 §7 的实测口径一致（955 → 4,300 msg/s）。
package tdengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Client struct {
	URL, User, Pass string
	HTTP            *http.Client
	DB              string
}

func New(url, user, pass string) *Client {
	return &Client{URL: strings.TrimRight(url, "/"), User: user, Pass: pass, DB: "iot",
		HTTP: &http.Client{Timeout: 15 * time.Second}}
}

type Result struct {
	Code       int     `json:"code"`
	Desc       string  `json:"desc"`
	ColumnMeta [][]any `json:"column_meta"`
	Data       [][]any `json:"data"`
	Rows       int     `json:"rows"`
}

func (c *Client) do(ctx context.Context, sql string) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/rest/sql/"+c.DB, bytes.NewReader([]byte(sql)))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.User, c.Pass)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tdengine http: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("tdengine decode (%d): %s", resp.StatusCode, string(b))
	}
	if r.Code != 0 {
		return &r, fmt.Errorf("tdengine code %d: %s", r.Code, r.Desc)
	}
	return &r, nil
}

func (c *Client) Exec(ctx context.Context, sql string) error             { _, err := c.do(ctx, sql); return err }
func (c *Client) Query(ctx context.Context, sql string) (*Result, error) { return c.do(ctx, sql) }

// TelemetryRow 对应超级表 iot.telemetry 的一行（列 + 标签）。
type TelemetryRow struct {
	SN, PK, FW, Region string
	Cell               int
	Ts                 int64 // ms
	Seq                int64
	WorkState          int
	PowerLevel         int
	TempCavity         float64
	TempWater          float64
	FanRPM             int
	LaserHours         float64
	Progress           int
}

// EventRow 对应超级表 iot.events。v1.1 四个任务字段为空串时写 NULL（列存在但未上报 / 设备未 opt-in）。
type EventRow struct {
	SN, PK    string
	Ts, Seq   int64
	Code, Msg string

	JobID, MaterialID, ParamProfileID, ParamsHash string
}

const maxSQLBytes = 900 * 1024

func q(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// qn 与 q 相同，但空串写 NULL（可选列）。
func qn(s string) string {
	if s == "" {
		return "NULL"
	}
	return q(s)
}

// BuildTelemetryInserts 按 SN 分组生成多表多行 INSERT，超过 maxSQLBytes 自动切分。
// 纯函数，便于单测；BatchInsert 调它。
func BuildTelemetryInserts(rows []TelemetryRow) []string {
	if len(rows) == 0 {
		return nil
	}
	bySN := map[string][]TelemetryRow{}
	order := []string{}
	for _, r := range rows {
		if _, ok := bySN[r.SN]; !ok {
			order = append(order, r.SN)
		}
		bySN[r.SN] = append(bySN[r.SN], r)
	}
	sort.Strings(order)
	var out []string
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	for _, sn := range order {
		rs := bySN[sn]
		sort.Slice(rs, func(i, j int) bool { return rs[i].Ts < rs[j].Ts })
		var part strings.Builder
		f := rs[0]
		fmt.Fprintf(&part, "iot.t_%s USING iot.telemetry TAGS (%s,%s,%s,%s,%d) VALUES ",
			sanitize(sn), q(f.SN), q(f.PK), q(f.FW), q(f.Region), f.Cell)
		for _, r := range rs {
			fmt.Fprintf(&part, "(%d,%d,%d,%d,%g,%g,%d,%g,%d) ",
				r.Ts, r.Seq, r.WorkState, r.PowerLevel, r.TempCavity, r.TempWater, r.FanRPM, r.LaserHours, r.Progress)
		}
		if sb.Len()+part.Len() > maxSQLBytes && sb.Len() > len("INSERT INTO ") {
			out = append(out, sb.String())
			sb.Reset()
			sb.WriteString("INSERT INTO ")
		}
		sb.WriteString(part.String())
	}
	out = append(out, sb.String())
	return out
}

func BuildEventInserts(rows []EventRow) []string {
	if len(rows) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	for _, r := range rows {
		fmt.Fprintf(&sb, "iot.e_%s USING iot.events TAGS (%s,%s) VALUES (%d,%d,%s,%s,%s,%s,%s,%s) ",
			sanitize(r.SN), q(r.SN), q(r.PK), r.Ts, r.Seq, q(r.Code), q(r.Msg),
			qn(r.JobID), qn(r.MaterialID), qn(r.ParamProfileID), qn(r.ParamsHash))
	}
	return []string{sb.String()}
}

// sanitize 把 SN 变成合法子表名片段（字母数字下划线）。
func sanitize(sn string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(sn) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func (c *Client) BatchInsert(ctx context.Context, rows []TelemetryRow) error {
	for _, sql := range BuildTelemetryInserts(rows) {
		if err := c.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) InsertEvents(ctx context.Context, rows []EventRow) error {
	for _, sql := range BuildEventInserts(rows) {
		if err := c.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// SchemaErrorIsBenign 判定 DDL 错误是否为「已存在」类可忽略错误（纯函数）。
// 实测 TDengine 3.3.5 taosAdapter 文案：
//   - 重复建库/表：desc 含 "already exists"
//   - ALTER STABLE ADD COLUMN 重复列：code 875, desc "Column already exists"
func SchemaErrorIsBenign(err error) bool {
	if err == nil {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "code 875")
}

// EnsureSchema 幂等执行 sql/tdengine.sql 的语句（调用方按 ; 切分传入）。
func (c *Client) EnsureSchema(ctx context.Context, stmts []string) error {
	for _, s := range stmts {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		db := c.DB
		if strings.HasPrefix(strings.ToUpper(s), "CREATE DATABASE") {
			c.DB = ""
		}
		err := c.Exec(ctx, s)
		c.DB = db
		if !SchemaErrorIsBenign(err) {
			return err
		}
	}
	return nil
}

// CountRows 用于泄洪法对账。
func (c *Client) CountRows(ctx context.Context, stable string) (int64, error) {
	r, err := c.Query(ctx, "SELECT count(*) FROM iot."+stable)
	if err != nil {
		return 0, err
	}
	if len(r.Data) == 0 || len(r.Data[0]) == 0 {
		return 0, nil
	}
	switch v := r.Data[0][0].(type) {
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unexpected count type %T", v)
	}
}

// Telemetry1hRequiredCols 是 BL3 健康度需要的 telemetry_1h 列（流目标表）；缺任一列说明是老流，需重建。
var Telemetry1hRequiredCols = []string{"avg_power", "share_low", "share_mid", "share_high"}

// StreamNeedsRebuild 纯函数：existing 为空（表不存在）不需重建（CREATE STREAM 会建）；否则 required 有缺即需重建。
func StreamNeedsRebuild(existing, required []string) bool {
	if len(existing) == 0 {
		return false
	}
	have := map[string]bool{}
	for _, c := range existing {
		have[strings.ToLower(strings.TrimSpace(c))] = true
	}
	for _, r := range required {
		if !have[strings.ToLower(r)] {
			return true
		}
	}
	return false
}

// Columns 用 DESCRIBE 取表的列名（含标签列）；表不存在返回 nil, nil。
func (c *Client) Columns(ctx context.Context, table string) ([]string, error) {
	r, err := c.Query(ctx, "DESCRIBE iot."+table)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "not exist") || strings.Contains(msg, "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(r.Data))
	for _, row := range r.Data {
		if len(row) > 0 {
			if s, ok := row[0].(string); ok {
				out = append(out, s)
			}
		}
	}
	return out, nil
}

// RebuildStreamIfMissing：流目标表 table 缺 required 列时 DROP STREAM stream 与 DROP STABLE iot.table，
// 让随后的 EnsureSchema 用新定义重建。只动流与其目标表，不碰 telemetry 超级表。返回是否执行了重建。
func (c *Client) RebuildStreamIfMissing(ctx context.Context, stream, table string, required []string) (bool, error) {
	cols, err := c.Columns(ctx, table)
	if err != nil {
		return false, fmt.Errorf("describe %s: %w", table, err)
	}
	if !StreamNeedsRebuild(cols, required) {
		return false, nil
	}
	if err := c.Exec(ctx, "DROP STREAM IF EXISTS "+stream); err != nil && !SchemaErrorIsBenign(err) {
		return false, fmt.Errorf("drop stream %s: %w", stream, err)
	}
	if err := c.Exec(ctx, "DROP STABLE IF EXISTS iot."+table); err != nil {
		// 目标表可能是普通表（老版本流）
		if err2 := c.Exec(ctx, "DROP TABLE IF EXISTS iot."+table); err2 != nil {
			return false, fmt.Errorf("drop %s: %w", table, err)
		}
	}
	return true, nil
}
