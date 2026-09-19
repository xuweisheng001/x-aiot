package support

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// HTTPDeviceAPI 是 deviceapi 的 HTTP 客户端。
type HTTPDeviceAPI struct {
	Base string
	HTTP *http.Client
}

func NewHTTPDeviceAPI(base string) *HTTPDeviceAPI {
	return &HTTPDeviceAPI{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 5 * time.Second}}
}

type apiResp struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func (c *HTTPDeviceAPI) do(ctx context.Context, method, path string, body any, hdr map[string]string, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var r apiResp
	if err := json.Unmarshal(b, &r); err != nil {
		return resp.StatusCode, fmt.Errorf("deviceapi decode (%d): %s", resp.StatusCode, string(b))
	}
	if out != nil && len(r.Data) > 0 && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(r.Data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("deviceapi data decode: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func (c *HTTPDeviceAPI) Shadow(ctx context.Context, sn string) (ShadowResp, error) {
	var sh ShadowResp
	st, err := c.do(ctx, http.MethodGet, "/api/v1/devices/"+sn+"/shadow", nil, nil, &sh)
	if err != nil {
		return sh, err
	}
	if st != http.StatusOK {
		return sh, fmt.Errorf("deviceapi shadow status %d", st)
	}
	return sh, nil
}

func (c *HTTPDeviceAPI) PostCmd(ctx context.Context, sn, action string, params map[string]any, source, operator, grantID string) (string, int, error) {
	var out struct {
		CmdID string `json:"cmd_id"`
	}
	hdr := map[string]string{grantcheck.HeaderSource: source, grantcheck.HeaderOperator: operator}
	if grantID != "" {
		hdr[grantcheck.HeaderGrantID] = grantID
	}
	st, err := c.do(ctx, http.MethodPost, "/api/v1/devices/"+sn+"/cmd", map[string]any{"action": action, "params": params, "operator": operator}, hdr, &out)
	return out.CmdID, st, err
}

func (c *HTTPDeviceAPI) GetCmd(ctx context.Context, cmdID string) (CmdResult, error) {
	var r CmdResult
	st, err := c.do(ctx, http.MethodGet, "/api/v1/cmds/"+cmdID, nil, nil, &r)
	if err != nil {
		return r, err
	}
	if st != http.StatusOK {
		return r, fmt.Errorf("deviceapi cmds status %d", st)
	}
	return r, nil
}

// TDSource 用 TDengine REST 拉遥测摘要与事件。
type TDSource struct{ C *tdengine.Client }

func (t *TDSource) Telemetry(ctx context.Context, sn string, since time.Time) ([]TelemetryPoint, error) {
	res, err := t.C.Query(ctx, fmt.Sprintf("SELECT ts, temp_cavity, work_state, power_level FROM iot.telemetry WHERE sn='%s' AND ts >= %d ORDER BY ts ASC LIMIT 2000",
		strings.ReplaceAll(sn, "'", ""), since.UnixMilli()))
	if err != nil {
		return nil, err
	}
	return ParseTelemetryPoints(res), nil
}

func (t *TDSource) Events(ctx context.Context, sn string, since time.Time, limit int) ([]EventItem, error) {
	res, err := t.C.Query(ctx, fmt.Sprintf("SELECT ts, code, msg FROM iot.events WHERE sn='%s' AND ts >= %d ORDER BY ts DESC LIMIT %d",
		strings.ReplaceAll(sn, "'", ""), since.UnixMilli(), limit))
	if err != nil {
		return nil, err
	}
	return ParseEventItems(res), nil
}

// ParseTelemetryPoints 按列名解析（坏行跳过）。
func ParseTelemetryPoints(res *tdengine.Result) []TelemetryPoint {
	if res == nil {
		return nil
	}
	idx := colIndex(res)
	var out []TelemetryPoint
	for _, r := range res.Data {
		ts, ok := parseTs(at(r, idx, "ts", 0))
		if !ok {
			continue
		}
		p := TelemetryPoint{Ts: ts.UnixMilli()}
		if f, ok := toFloat(at(r, idx, "temp_cavity", 1)); ok {
			p.TempCavity = f
		}
		if n, ok := toInt(at(r, idx, "work_state", 2)); ok {
			p.WorkState = n
		}
		if n, ok := toInt(at(r, idx, "power_level", 3)); ok {
			p.PowerLevel = n
		}
		out = append(out, p)
	}
	return out
}

// ParseEventItems 按列名解析；msg 打 untrusted 标签并截断 128 字节。
func ParseEventItems(res *tdengine.Result) []EventItem {
	if res == nil {
		return nil
	}
	idx := colIndex(res)
	var out []EventItem
	for _, r := range res.Data {
		ts, ok := parseTs(at(r, idx, "ts", 0))
		code, ok2 := at(r, idx, "code", 1).(string)
		if !ok || !ok2 || code == "" {
			continue
		}
		e := EventItem{Ts: ts.UnixMilli(), Code: code}
		if m, ok := at(r, idx, "msg", 2).(string); ok {
			e.Msg = TagUntrusted(m, 128)
		}
		out = append(out, e)
	}
	return out
}

func colIndex(res *tdengine.Result) map[string]int {
	idx := map[string]int{}
	for i, m := range res.ColumnMeta {
		if len(m) > 0 {
			if name, ok := m[0].(string); ok {
				idx[strings.ToLower(name)] = i
			}
		}
	}
	return idx
}

func at(row []any, idx map[string]int, name string, def int) any {
	i, ok := idx[name]
	if !ok {
		i = def
	}
	if i < 0 || i >= len(row) {
		return nil
	}
	return row[i]
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}
