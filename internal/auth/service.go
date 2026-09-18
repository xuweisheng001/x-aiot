package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 抽象 PG 查询，便于 handler 单测。
type Store interface {
	// DeviceStatus 返回设备状态；found=false 表示不存在。
	DeviceStatus(ctx context.Context, sn string) (status string, found bool, err error)
	// Cert 返回证书归属 SN 与状态；found=false 表示不存在。
	Cert(ctx context.Context, fp string) (sn, status string, found bool, err error)
}

type PGStore struct{ Pool *pgxpool.Pool }

func (s *PGStore) DeviceStatus(ctx context.Context, sn string) (string, bool, error) {
	var st string
	err := s.Pool.QueryRow(ctx, `SELECT status FROM iot_shard.device WHERE sn=$1`, sn).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("device status: %w", err)
	}
	return st, true, nil
}

func (s *PGStore) Cert(ctx context.Context, fp string) (string, string, bool, error) {
	var sn, st string
	err := s.Pool.QueryRow(ctx, `SELECT sn, status FROM iot_shard.device_cert WHERE cert_fp=$1`, fp).Scan(&sn, &st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("cert: %w", err)
	}
	return strings.TrimSpace(sn), st, true, nil
}

// Authenticate：SN 格式合法 && 设备 status='activated' && (cert_fp 为空 || 证书 active 且归属该 SN)。
// 出错时 fail-closed（deny）并返回 err 供记录。
func Authenticate(ctx context.Context, st Store, clientID, certFP string) (bool, string, error) {
	if !ValidSN(clientID) {
		return false, "invalid sn", nil
	}
	status, found, err := st.DeviceStatus(ctx, clientID)
	if err != nil {
		return false, "store error", err
	}
	if !found {
		return false, "device not found", nil
	}
	if status != "activated" {
		return false, "device status " + status, nil
	}
	if certFP = strings.TrimSpace(certFP); certFP != "" {
		sn, cst, found, err := st.Cert(ctx, certFP)
		if err != nil {
			return false, "store error", err
		}
		if !found {
			return false, "cert not found", nil
		}
		if cst != "active" {
			return false, "cert status " + cst, nil
		}
		if sn != clientID {
			return false, "cert sn mismatch", nil
		}
	}
	return true, "", nil
}
