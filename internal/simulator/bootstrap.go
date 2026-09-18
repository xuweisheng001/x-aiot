package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// BootstrapResult 是 bootstrap-svc 的 data（兼容 {code,data} 包装与裸对象）。
type BootstrapResult struct {
	CellID     int    `json:"cell_id"`
	MQTTHost   string `json:"mqtt_host"`
	MQTTPort   int    `json:"mqtt_port"`
	RetryAfter int    `json:"retry_after"`
	CellMapVer int    `json:"cell_map_ver"`
}

// ParseBootstrap 解析响应体。
func ParseBootstrap(b []byte) (BootstrapResult, error) {
	var wrapped struct {
		Code int             `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &wrapped); err != nil {
		return BootstrapResult{}, err
	}
	var r BootstrapResult
	if len(wrapped.Data) > 0 {
		if wrapped.Code != 0 {
			return r, fmt.Errorf("bootstrap code %d: %s", wrapped.Code, wrapped.Msg)
		}
		if err := json.Unmarshal(wrapped.Data, &r); err != nil {
			return r, err
		}
	} else if err := json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	if r.MQTTHost == "" || r.MQTTPort == 0 {
		return r, fmt.Errorf("bootstrap: missing mqtt_host/mqtt_port")
	}
	return r, nil
}

var httpClient = &http.Client{Timeout: 3 * time.Second}

// FetchBootstrap 调一次 bootstrap-svc。
func FetchBootstrap(ctx context.Context, base, sn, pk string) (BootstrapResult, error) {
	u := base + "/api/v1/bootstrap?" + url.Values{"sn": {sn}, "pk": {pk}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return BootstrapResult{}, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return BootstrapResult{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusOK {
		return BootstrapResult{}, fmt.Errorf("bootstrap http %d: %s", resp.StatusCode, body)
	}
	return ParseBootstrap(body)
}

// ResolveBroker 先 bootstrap（尊重 retry_after，最多等 3 轮），不可达则回退到 fallback。
// 返回 host:port 与是否来自 bootstrap。
func ResolveBroker(ctx context.Context, cfg *Config, sn, fallback string) (string, bool) {
	if cfg.Bootstrap == "" {
		return fallback, false
	}
	for i := 0; i < 3; i++ {
		r, err := FetchBootstrap(ctx, cfg.Bootstrap, sn, cfg.PK)
		if err != nil {
			return fallback, false
		}
		addr := r.MQTTHost + ":" + strconv.Itoa(r.MQTTPort)
		if r.RetryAfter <= 0 {
			return addr, true
		}
		select {
		case <-ctx.Done():
			return addr, true
		case <-time.After(time.Duration(r.RetryAfter) * time.Second):
		}
	}
	// 连续 3 次被要求稍后再来：仍按最后一次给的地址试
	r, err := FetchBootstrap(ctx, cfg.Bootstrap, sn, cfg.PK)
	if err != nil {
		return fallback, false
	}
	return r.MQTTHost + ":" + strconv.Itoa(r.MQTTPort), true
}
