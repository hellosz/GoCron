package common

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

// Job 任务信息
type Job struct {
	Name    string `json:"name,omitempty"`     // 任务名称
	Command string `json:"command,omitempty"`  // 命令
	CronExp string `json:"cron_exp,omitempty"` // crontab 表达式
}

// JobEvent 任务的操作信息
type JobEvent struct {
	Type int  `json:"type,omitempty"` // 任务的操作类型
	Job  *Job `json:"job,omitempty"`  // 任务明细
}

// 任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 // 任务信息
	CronExp  *cronexpr.Expression //执行时间表达式
	NextTime time.Time            // 下次执行时间
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

// 从 jobKey 中获取任务名称
func ParseJobName(jobKey string) string {
	return strings.TrimPrefix(CRON_JOB_DIR, jobKey)
}

// 将字节解析成 Job 对象
func UnpackJob(data []byte) (job *Job, err error) {
	var (
		tmpJob Job
	)

	// 解析数据保存到 tmpJob 中
	if err = json.Unmarshal(data, &tmpJob); err != nil {
		// 返回异常
		return
	}

	// 返回结果
	job = &tmpJob
	return
}

// 构造 JobEvent 结构体
func BuildJobEvent(jobEventType int, job *Job) (event *JobEvent) {
	return &JobEvent{
		Type: jobEventType,
		Job:  job,
	}
}

// 构造 JobSchedulePlan 数据结构
func BuildJobSchedulePlan(jobEvent *JobEvent) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		cronExp *cronexpr.Expression
	)

	// 解析 crontab 表达式
	if cronExp, err = cronexpr.Parse(jobEvent.Job.CronExp); err != nil {
		return
	}

	// 构建任务调度计划
	jobSchedulePlan = &JobSchedulePlan{
		Job:      jobEvent.Job,
		CronExp:  cronExp,
		NextTime: cronExp.Next(time.Now()),
	}

	return
}
