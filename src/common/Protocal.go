package common

import (
	"context"
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

// 任务执行状态信息
type JobExecuteInfo struct {
	Job        *Job
	PlanTime   time.Time          // 计划执行时间
	RealTime   time.Time          // 实际执行时间
	CancelCtx  context.Context    // 执行上下文
	CancelFunc context.CancelFunc // 执行上下文取消方法
}

// 任务执行结果
type JobExecuteResult struct {
	Job            *Job
	JobExecuteInfo *JobExecuteInfo // 任务执行信息
	Outout         []byte          // 输出结果
	Err            error           // 执行出错
	StartTime      time.Time       // 开始时间
	EndTime        time.Time       // 结束时间
}

// 任务执行日志
type JobLog struct {
	JobName      string `json:"job_name,omitempty" bson:"job_name,omitempty"`           // 任务名称
	Command      string `json:"command,omitempty" bson:"command,omitempty"`             // 执行命令
	Output       string `json:"output,omitempty bson:"output,omitempty"`                // 执行结果输出
	Err          string `json:"err,omitempty" bson:"err,omitempty"`                     // 错误日志
	PlanTime     int64  `json:"plan_time,omitempty" bson:"plan_time,omitempty"`         // 任务计划开始时间
	ScheduleTime int64  `json:"schedule_time,omitempty" bson:"schedule_time,omitempty"` // 任务调度开始时间
	StartTime    int64  `json:"start_time,omitempty" bson:"start_time,omitempty"`       // 任务执行开始时间
	EndTime      int64  `json:"end_time,omitempty" bson:"end_time,omitempty"`           // 任务执行结束时间
}

// 任务名称查询
type JobLogFilter struct {
	JobName string `bson:"job_name"` // 任务名称
}

// 批量日志
type LogBatch struct {
	Logs []interface{}
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
	return strings.TrimPrefix(jobKey, CRON_JOB_DIR)
}

// 从 jobKey 中获取任务名称
func ParseKillJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, CRON_KILL_JOB)
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

// 构造 JobExecuteInfo 数据结构
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) *JobExecuteInfo {
	var (
		jobExecuteInfo *JobExecuteInfo
	)

	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}

	// 添加取消信息
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return jobExecuteInfo
}

// 纳秒转毫秒
func NanoToMillSecs(nano int64) int64 {
	return nano / 1000 / 1000
}
