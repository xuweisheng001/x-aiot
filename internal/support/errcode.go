package support

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// 错误码字典（§8）：按 (code, version) 版本化；发布双人审批由 PG ck_dict_approved 守。

type DictEntry struct {
	Code            string    `json:"code"`
	Version         int64     `json:"version"`
	ProductKeys     []string  `json:"product_keys"`
	Severity        string    `json:"severity"`
	Cause           string    `json:"cause"`
	Steps           string    `json:"steps"`
	NeedService     bool      `json:"need_service"`
	DefectThreshold *int      `json:"defect_threshold,omitempty"`
	CreatedBy       string    `json:"created_by,omitempty"`
	ApprovedBy      string    `json:"approved_by,omitempty"`
	ReleasedAt      time.Time `json:"released_at"`
}

var validSeverity = map[string]bool{"info": true, "warn": true, "critical": true}

// ValidDictEntry 纯校验：code 标识、severity 枚举、cause/steps 非空、审批人非空且 ≠ 创建人（PG 也会拒，这里快速失败）。
func ValidDictEntry(e DictEntry) error {
	if !identRe.MatchString(e.Code) {
		return fmt.Errorf("%w: code", ErrBadParam)
	}
	if !validSeverity[e.Severity] {
		return fmt.Errorf("%w: severity must be info|warn|critical", ErrBadParam)
	}
	if strings.TrimSpace(e.Cause) == "" || strings.TrimSpace(e.Steps) == "" {
		return fmt.Errorf("%w: cause and steps required", ErrBadParam)
	}
	if strings.TrimSpace(e.CreatedBy) == "" {
		return fmt.Errorf("%w: created_by", ErrBadParam)
	}
	if strings.TrimSpace(e.ApprovedBy) == "" || e.ApprovedBy == e.CreatedBy {
		return fmt.Errorf("%w: approved_by required and must differ from created_by", ErrDenied)
	}
	if e.DefectThreshold != nil && *e.DefectThreshold <= 0 {
		return fmt.Errorf("%w: defect_threshold must be > 0", ErrBadParam)
	}
	return nil
}

// PublishDict 发布一条新版本（version = 该 code 最大版本 + 1）。
func (s *Service) PublishDict(ctx context.Context, e DictEntry) (DictEntry, error) {
	if err := ValidDictEntry(e); err != nil {
		return DictEntry{}, err
	}
	e.ReleasedAt = s.now()
	v, err := s.Store.DictPublish(ctx, e)
	if err != nil {
		return DictEntry{}, err
	}
	e.Version = v
	s.M.Inc(MDictPublished)
	return e, nil
}

// GetDict 单条最新版本；未知码计数（看板作字典补全依据）。
func (s *Service) GetDict(ctx context.Context, code string) (DictEntry, error) {
	e, err := s.Store.DictGet(ctx, code)
	if err != nil {
		if err == ErrNotFound || strings.Contains(err.Error(), "not found") {
			s.M.Inc(MDictUnknown)
		}
		return DictEntry{}, err
	}
	s.M.Inc(MDictServed)
	return e, nil
}
