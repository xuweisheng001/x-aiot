package cf001

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNoQuota  = errors.New("quota exhausted or order not active") // 11010
	ErrNotFound = errors.New("not found")
	ErrBadParam = errors.New("bad param")
	ErrConflict = errors.New("conflict")
)

const seqTTL = 48 * time.Hour

type Service struct {
	DB  *pgxpool.Pool
	RDB *redis.Client
	Key *rsa.PrivateKey
	Now func() time.Time
	// NextSeq 返回某天的下一个流水号；默认 Redis INCR sn:seq:{yymmdd} EX 2 天。
	NextSeq func(ctx context.Context, yymmdd string) (int64, error)
}

func NewService(db *pgxpool.Pool, rdb *redis.Client, key *rsa.PrivateKey) *Service {
	s := &Service{DB: db, RDB: rdb, Key: key, Now: time.Now}
	s.NextSeq = func(ctx context.Context, yymmdd string) (int64, error) {
		k := "sn:seq:" + yymmdd
		n, err := rdb.Incr(ctx, k).Result()
		if err != nil {
			return 0, fmt.Errorf("sn seq: %w", err)
		}
		if n == 1 {
			_ = rdb.Expire(ctx, k, seqTTL).Err()
		}
		return n, nil
	}
	return s
}

// ---------- quotas ----------

type Quota struct {
	ID         int64     `json:"id"`
	OrderNo    string    `json:"order_no"`
	Supplier   string    `json:"supplier"`
	ProductKey string    `json:"product_key"`
	Quota      int       `json:"quota"`
	Registered int       `json:"registered"`
	Status     int16     `json:"status"` // 1 进行中 2 已完成
	CreatedAt  time.Time `json:"created_at"`
}

type QuotaReq struct {
	OrderNo    string `json:"order_no"`
	Supplier   string `json:"supplier"`
	ProductKey string `json:"product_key"`
	Quota      int    `json:"quota"`
}

func (s *Service) CreateQuota(ctx context.Context, r QuotaReq) (*Quota, error) {
	if r.OrderNo == "" || r.Supplier == "" || r.ProductKey == "" || r.Quota < 0 {
		return nil, fmt.Errorf("%w: order_no/supplier/product_key required, quota >= 0", ErrBadParam)
	}
	_, err := s.DB.Exec(ctx, `INSERT INTO cf001.oem_quotas(order_no,supplier,product_key,quota,status) VALUES($1,$2,$3,$4,1)`,
		r.OrderNo, r.Supplier, r.ProductKey, r.Quota)
	if err != nil {
		if sqlState(err) == "23505" {
			return nil, fmt.Errorf("%w: order_no exists", ErrConflict)
		}
		return nil, err
	}
	return s.GetQuota(ctx, r.OrderNo)
}

// AppendQuota 追加配额并把 status 回到 1（自动恢复已完成工单）。
func (s *Service) AppendQuota(ctx context.Context, orderNo string, delta int) (*Quota, error) {
	if delta <= 0 {
		return nil, fmt.Errorf("%w: delta must be > 0", ErrBadParam)
	}
	tag, err := s.DB.Exec(ctx, `UPDATE cf001.oem_quotas SET quota=quota+$2, status=1 WHERE order_no=$1`, orderNo, delta)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: order %s", ErrNotFound, orderNo)
	}
	return s.GetQuota(ctx, orderNo)
}

func (s *Service) GetQuota(ctx context.Context, orderNo string) (*Quota, error) {
	var q Quota
	err := s.DB.QueryRow(ctx, `SELECT id,order_no,supplier,product_key,quota,registered,status,created_at FROM cf001.oem_quotas WHERE order_no=$1`, orderNo).
		Scan(&q.ID, &q.OrderNo, &q.Supplier, &q.ProductKey, &q.Quota, &q.Registered, &q.Status, &q.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: order %s", ErrNotFound, orderNo)
	}
	return &q, err
}

// ---------- sign ----------

// SignInput 是解密后的注册请求；UUID/MCU/SOC/MAC 可选（digest 才是身份）。
type SignInput struct {
	OrderNo string
	Line    string
	Digest  string
	UUID    string
	MCUSN   string
	SOCSN   string
	MAC     string
}

type SignResult struct {
	SN           string `json:"sn"`
	SignatureB64 string `json:"signature_b64"`
	Existing     bool   `json:"existing"` // 幂等命中：未消耗配额
}

// Sign 是 HTTP 入口：base64 → OAEP 解密 → ParsePayload → SignDigest。
func (s *Service) Sign(ctx context.Context, orderNo, line, payloadB64 string, ids SignInput) (*SignResult, error) {
	ct, err := base64.StdEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, fmt.Errorf("%w: payload_b64 not base64", ErrBadParam)
	}
	pt, err := DecryptOAEP(s.Key, ct)
	if err != nil {
		return nil, fmt.Errorf("%w: OAEP decrypt failed", ErrBadParam)
	}
	p, err := ParsePayload(pt)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
	}
	ids.OrderNo, ids.Line, ids.Digest = orderNo, line, p.Digest
	return s.SignDigest(ctx, ids)
}

// SignDigest 执行幂等检查 + 配额事务 + SN 生成 + PSS 签名。
// 并发安全性由数据库保证：配额 UPDATE 的 WHERE 条件是原子的，digest 主键拦截同 digest 竞态。
func (s *Service) SignDigest(ctx context.Context, in SignInput) (*SignResult, error) {
	if in.OrderNo == "" || len(in.Digest) != 64 {
		return nil, fmt.Errorf("%w: order_no and 64-hex digest required", ErrBadParam)
	}
	// 幂等：已注册 → 返回既有 SN + 重新签名，不消耗配额
	if r, err := s.existing(ctx, in.Digest); err != nil {
		return nil, err
	} else if r != nil {
		return r, nil
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var productKey string
	err = tx.QueryRow(ctx,
		`UPDATE cf001.oem_quotas
		    SET registered = registered + 1,
		        status = CASE WHEN registered + 1 >= quota THEN 2 ELSE status END
		  WHERE order_no = $1 AND status = 1 AND registered < quota
		  RETURNING product_key`, in.OrderNo).Scan(&productKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoQuota // affected=0 → 回滚 → 11010
	}
	if err != nil {
		return nil, fmt.Errorf("quota update: %w", err)
	}

	day := s.Now().UTC().Format("060102")
	seq, err := s.NextSeq(ctx, day)
	if err != nil {
		return nil, err
	}
	sn := BuildSN(productKey, day, in.Line, seq)
	sig, err := SignDigestPSS(s.Key, in.Digest)
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO cf001.digest_maps(digest,uuid,mcu_sn,soc_sn,mac,sn,order_no) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		in.Digest, in.UUID, in.MCUSN, in.SOCSN, in.MAC, sn, in.OrderNo)
	if err != nil {
		if sqlState(err) == "23505" { // 同 digest 并发竞态：对方已提交，回滚本事务并返回既有
			_ = tx.Rollback(ctx)
			if r, err2 := s.existing(ctx, in.Digest); err2 == nil && r != nil {
				return r, nil
			}
			return nil, fmt.Errorf("%w: digest race", ErrConflict)
		}
		return nil, fmt.Errorf("insert digest_maps: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO cf001.oem_devices(sn,digest,signature,product_key) VALUES($1,$2,$3,$4)`,
		sn, in.Digest, sig, productKey); err != nil {
		return nil, fmt.Errorf("insert oem_devices: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	slog.Info("cf001 device signed", "order_no", in.OrderNo, "sn", sn)
	return &SignResult{SN: sn, SignatureB64: sig}, nil
}

func (s *Service) existing(ctx context.Context, digest string) (*SignResult, error) {
	var sn string
	err := s.DB.QueryRow(ctx, `SELECT sn FROM cf001.digest_maps WHERE digest=$1`, digest).Scan(&sn)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sig, err := SignDigestPSS(s.Key, digest)
	if err != nil {
		return nil, err
	}
	return &SignResult{SN: sn, SignatureB64: sig, Existing: true}, nil
}

// ---------- verify ----------

type VerifyResult struct {
	OK   bool   `json:"ok"`
	Step string `json:"step"` // exists | digest | signature
	SN   string `json:"sn"`
	Msg  string `json:"msg,omitempty"`
}

// Verify 三步自检：sn 存在 → digest 一致 → PSS 验签。返回的 Step 是最后执行（或失败）的步骤。
func (s *Service) Verify(ctx context.Context, sn, digest, sigB64 string) (*VerifyResult, error) {
	res := &VerifyResult{SN: sn, Step: "exists"}
	var stored string
	err := s.DB.QueryRow(ctx, `SELECT digest FROM cf001.oem_devices WHERE sn=$1`, sn).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		res.Msg = "sn not registered"
		return res, nil
	}
	if err != nil {
		return nil, err
	}
	res.Step = "digest"
	if stored != digest {
		res.Msg = "digest mismatch"
		return res, nil
	}
	res.Step = "signature"
	if err := VerifyDigestPSS(&s.Key.PublicKey, digest, sigB64); err != nil {
		res.Msg = "signature invalid"
		return res, nil
	}
	res.OK = true
	return res, nil
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
