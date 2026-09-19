// Package support 实现 BL6 售后与保修：诊断包、客服授权自检、Agent 只读代理、错误码字典、
// 批次缺陷聚合、保修数据汇总（docs/tech-design-bl6-aftersales-warranty.md）。
//
// 四条不可协商约束：授权校验在 deviceapi 内直查 PG（grantcheck 子包）；Agent 只有授权内 self_check
// 一个写动作；诊断包按结构体白名单生成只含 SN；保修只给数据与依据不给判定。
package support

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("conflict")
	ErrDenied      = errors.New("denied")
	ErrBadParam    = errors.New("bad param")
	ErrGone        = errors.New("gone")
	ErrUnavailable = errors.New("unavailable")
)
