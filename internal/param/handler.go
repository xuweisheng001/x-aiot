package param

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// Routes 组装 param-svc 的 HTTP 路由。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("param-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	// 发布（管理台 / 运营，内部接口）
	mux.HandleFunc("POST /internal/params/releases", func(w http.ResponseWriter, r *http.Request) {
		var req PublishReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		rel, err := svc.Publish(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: rel})
	})

	// 回滚
	mux.HandleFunc("POST /internal/params/releases/{product_key}/rollback", func(w http.ResponseWriter, r *http.Request) {
		var req RollbackReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		rel, err := svc.Rollback(r.Context(), r.PathValue("product_key"), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: rel})
	})

	// 最新可见版本
	mux.HandleFunc("GET /api/v1/params/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		pk := r.URL.Query().Get("product_key")
		if !identRe.MatchString(pk) {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "product_key required")
			return
		}
		bucket, has, err := bucketOf(r)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
			return
		}
		res, err := svc.Latest(r.Context(), pk, bucket, has)
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, res.Version))
		httpx.OK(w, res)
	})

	// 增量同步
	mux.HandleFunc("GET /api/v1/params", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		pk := q.Get("product_key")
		if !identRe.MatchString(pk) {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "product_key required")
			return
		}
		since, _ := strconv.ParseInt(q.Get("since_version"), 10, 64)
		bucket, has, err := bucketOf(r)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
			return
		}
		latest, err := svc.Latest(r.Context(), pk, bucket, has)
		if err != nil {
			writeErr(w, err)
			return
		}
		etag := fmt.Sprintf(`"%d"`, latest.Version)
		if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && (inm == etag || inm == strings.Trim(etag, `"`)) {
			svc.M.Inc(MNotModified)
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		res, err := svc.Delta(r.Context(), pk, since, bucket, has)
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("ETag", etag)
		httpx.OK(w, res)
	})

	// 全量快照（原型本地目录代替 CDN；文件名含版本，immutable）
	mux.HandleFunc("GET /snapshots/{product_key}/{file}", func(w http.ResponseWriter, r *http.Request) {
		b, err := svc.ReadSnapshot(r.PathValue("product_key"), r.PathValue("file"))
		if err != nil {
			writeErr(w, err)
			return
		}
		svc.M.Inc(MSnapshotServed)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	// 用户自定义参数
	mux.HandleFunc("PUT /api/v1/user-params", func(w http.ResponseWriter, r *http.Request) {
		uid, ok := userID(w, r)
		if !ok {
			return
		}
		ifMatch, err := parseIfMatch(r.Header.Get("If-Match"))
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, `If-Match required ("0" for create)`)
			return
		}
		var up UserParam
		if err := httpx.Decode(r, &up); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		up.UserID = uid
		ver, cur, conflict, err := svc.PutUserParam(r.Context(), up, ifMatch)
		if err != nil {
			writeErr(w, err)
			return
		}
		if conflict {
			w.Header().Set("ETag", fmt.Sprintf(`"%d"`, cur))
			httpx.JSON(w, http.StatusConflict, httpx.Resp{Code: httpx.CodeConflict, Msg: "version mismatch", Data: map[string]any{"current_version": cur}})
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, ver))
		httpx.OK(w, map[string]any{"version": ver})
	})
	mux.HandleFunc("GET /api/v1/user-params", func(w http.ResponseWriter, r *http.Request) {
		uid, ok := userID(w, r)
		if !ok {
			return
		}
		pk := r.URL.Query().Get("product_key")
		if !identRe.MatchString(pk) {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "product_key required")
			return
		}
		list, err := svc.Store.UserParams(r.Context(), uid, pk)
		if err != nil {
			writeErr(w, err)
			return
		}
		if list == nil {
			list = []UserParam{}
		}
		httpx.OK(w, list)
	})

	// 校正系数（P1 预留，只读）
	mux.HandleFunc("GET /api/v1/devices/{sn}/correction", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		hours, hoursKnown := parseOptFloat(q.Get("laser_hours"))
		health, healthKnown := parseOptFloat(q.Get("health"))
		res, err := svc.Correct(r.Context(), q.Get("product_key"), q.Get("module_model"), hours, health, hoursKnown, healthKnown)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, res)
	})

	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

// bucketOf：?bucket=0.42 优先；否则从 X-User-Id 计算；两者都无 → hasBucket=false（只见 100% 版本）。
func bucketOf(r *http.Request) (float64, bool, error) {
	if b := r.URL.Query().Get("bucket"); b != "" {
		f, err := strconv.ParseFloat(b, 64)
		if err != nil || f < 0 || f >= 1 {
			return 0, false, errors.New("bucket must be in [0,1)")
		}
		return f, true, nil
	}
	if uid := strings.TrimSpace(r.Header.Get("X-User-Id")); uid != "" {
		return Bucket(uid), true, nil
	}
	return 0, false, nil
}

func userID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	uid, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get("X-User-Id")), 10, 64)
	if err != nil || uid <= 0 {
		httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-User-Id required")
		return 0, false
	}
	return uid, true
}

func parseIfMatch(h string) (int64, error) {
	h = strings.TrimSpace(strings.Trim(strings.TrimSpace(h), `"`))
	if h == "" {
		return 0, errors.New("missing")
	}
	v, err := strconv.ParseInt(h, 10, 64)
	if err != nil || v < 0 {
		return 0, errors.New("bad")
	}
	return v, nil
}

func parseOptFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

func writeErr(w http.ResponseWriter, err error) {
	var de *DiffError
	switch {
	case errors.As(err, &de):
		httpx.JSON(w, http.StatusConflict, httpx.Resp{Code: httpx.CodeBadParam, Msg: de.Error(), Data: map[string]any{"violations": de.Violations}})
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, err.Error())
	case errors.Is(err, ErrDenied):
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, err.Error())
	case errors.Is(err, ErrBadParam):
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
	case errors.Is(err, ErrConflict):
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, err.Error())
	default:
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
	}
}
