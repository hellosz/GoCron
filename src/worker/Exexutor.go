package worker

import (
	"math/rand"
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
			jobLock       *JobLock
		)

		// 记录执行结果
		executeResult = &common.JobExecuteResult{
			Job:            jobExecuteInfo.Job,
			JobExecuteInfo: jobExecuteInfo,
		}

		// 开始执行之前，随机睡眠(最多一秒钟)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		// 获取锁
		jobLock = G_jobMgr.CreateJobLock(jobExecuteInfo.Job.Name)
		err = jobLock.TryLock()
		defer jobLock.Unlock() // 结束后自动释放锁

		executeResult.StartTime = time.Now() // 记录任务开始时间
		if err != nil {                      // 没有获取到锁
			executeResult.EndTime = time.Now()
			executeResult.Err = err
		} else { // 获取到锁
			// 执行命令
			cmd = exec.CommandContext(jobExecuteInfo.CancelCtx, "/bin/bash", "-c", jobExecuteInfo.Job.Command)
			output, err = cmd.CombinedOutput()

			// 记录执行结果
			executeResult.EndTime = time.Now()
			executeResult.Outout = output
			executeResult.Err = err
		}

		// 返回结果
		G_Scheduler.PushJobResult(executeResult)
	}()
}
