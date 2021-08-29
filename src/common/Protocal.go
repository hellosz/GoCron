package common

import (
	"encoding/json"
)

// Job 任务信息
type Job struct {
	Name    string `json:"name,omitempty"`     // 任务名称
	Command string `json:"command,omitempty"`  // 命令
	CronExp string `json:"cron_exp,omitempty"` // crontab 表达式
}

// 返回值
type Response struct {
	ErrCode int         `json:"err_code"`
	Msg     string      `json:"msg"`
	Data    interface{} `json:"data"`
}

// 创建返回值
func BuildReponse(ErrCode int, Msg string, Data interface{}) (response []byte, err error) {
	var (
		resp Response
	)

	resp = Response{
		ErrCode: ErrCode,
		Msg:     Msg,
		Data:    Data,
	}

	return json.Marshal(resp)
}
