package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound 表示设备不存在。
var ErrNotFound = errors.New("not found")

// DeviceRepo 与 CellRepo 抽象 PG 访问，便于 handler 单测。
type DeviceRepo interface {
	GetDevice(ctx context.Context, sn string) (*Device, error)
	// InsertDevice 幂等插入（ON CONFLICT DO NOTHING）。
	InsertDevice(ctx context.Context, d Device) error
}

type CellRepo interface {
	ListCells(ctx context.Context) ([]Cell, error)
	SetCellStatus(ctx context.Context, id int, status string) (bool, error)
}

// PGRepo 同时实现 DeviceRepo 与 CellRepo。
type PGRepo struct{ Pool *pgxpool.Pool }

func (r *PGRepo) GetDevice(ctx context.Context, sn string) (*Device, error) {
	var d Device
	err := r.Pool.QueryRow(ctx,
		`SELECT sn, product_key, region, cell_id, cell_map_ver, status FROM iot_shard.device WHERE sn=$1`, sn).
		Scan(&d.SN, &d.ProductKey, &d.Region, &d.CellID, &d.CellMapVer, &d.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get device: %w", err)
	}
	return &d, nil
}

func (r *PGRepo) InsertDevice(ctx context.Context, d Device) error {
	_, err := r.Pool.Exec(ctx,
		`INSERT INTO iot_shard.device(sn, product_key, region, cell_id, cell_map_ver, status, activated_at)
		 VALUES ($1,$2,$3,$4,$5,$6, now()) ON CONFLICT (sn) DO NOTHING`,
		d.SN, d.ProductKey, d.Region, d.CellID, d.CellMapVer, d.Status)
	if err != nil {
		return fmt.Errorf("insert device: %w", err)
	}
	return nil
}

func (r *PGRepo) ListCells(ctx context.Context) ([]Cell, error) {
	rows, err := r.Pool.Query(ctx, `SELECT cell_id, region, mqtt_host, mqtt_port, status FROM iot_global.cell ORDER BY cell_id`)
	if err != nil {
		return nil, fmt.Errorf("list cells: %w", err)
	}
	defer rows.Close()
	var out []Cell
	for rows.Next() {
		var c Cell
		if err := rows.Scan(&c.ID, &c.Region, &c.MQTTHost, &c.MQTTPort, &c.Status); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *PGRepo) SetCellStatus(ctx context.Context, id int, status string) (bool, error) {
	tag, err := r.Pool.Exec(ctx, `UPDATE iot_global.cell SET status=$2, updated_at=now() WHERE cell_id=$1`, id, status)
	if err != nil {
		return false, fmt.Errorf("set cell status: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
