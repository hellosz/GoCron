package worker

import (
	"context"
	"os/exec"
	"time"

	"github.com/GoCron/src/common"
)

// 任务执行器
type Executor struct {
}

var (
	G_Executor *Executor
)

// 初始化任务执行器
func InitExecutor() (err error) {
	G_Executor = &Executor{}
	return
}

// 执行任务
func (executor *Executor) ExcuteJob(jobExecuteInfo *common.JobExecuteInfo) {
	go func() {
		// 执行任务
		var (
			executeResult *common.JobExecuteResult
			err           error
			output        []byte
			cmd           *exec.Cmd
		)

		// 记录执行结果
		executeResult = &common.JobExecuteResult{
			Job: jobExecuteInfo.Job,
		}
		executeResult.StartTime = time.Now()

		// 执行命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", jobExecuteInfo.Job.Command)
		output, err = cmd.CombinedOutput()

		// 记录执行结果
		executeResult.EndTime = time.Now()
		executeResult.Outout = output
		executeResult.Err = err

		// 返回结果
		G_Scheduler.PushJobResult(executeResult)
	}()
}
