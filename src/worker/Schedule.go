package worker

import (
	"fmt"
	"time"

	"github.com/GoCron/src/common"
)

// 任务调度器
type Scheduler struct {
	JobEventChan chan *common.JobEvent              // job 操作事件通道
	JobPlanTable map[string]*common.JobSchedulePlan // 任务调度计划表
}

var (
	G_Scheduler *Scheduler // 调度器全局单例
)

// 初始化调度器
func InitScheduler() error {
	G_Scheduler = &Scheduler{
		JobEventChan: make(chan *common.JobEvent, 1000),
		JobPlanTable: make(map[string]*common.JobSchedulePlan),
	}

	// 启动任务监听工具
	go scheduleLoop()
	return nil
}

// 任务监听工具
func scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		timer         *time.Timer
		scheduleAfter time.Duration
	)

	// 下次执行时间
	scheduleAfter = G_Scheduler.TrySchedule()
	timer = time.NewTimer(scheduleAfter)

	for {
		// 任务发生更新，或者定时到期
		select {
		case jobEvent = <-G_Scheduler.JobEventChan:
			handleJobEvent(jobEvent)
		case <-timer.C:
		}

		// 执行任务并计算下次执行时间
		scheduleAfter = G_Scheduler.TrySchedule()
		timer.Reset(scheduleAfter)
	}

}

// 处理 Job 事件
func handleJobEvent(jobEvent *common.JobEvent) error {
	var (
		err             error
		jobExisted      bool
		jobSchedulePlan *common.JobSchedulePlan
	)
	println("handle jobEvent" + jobEvent.Job.Name)
	switch jobEvent.Type {
	case common.JOB_EVENT_PUT:
		// 保存到任务调度表中
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent); err == nil {
			G_Scheduler.JobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
			println("将任务加入到任务调度计划表中" + jobEvent.Job.Name)
		}

	case common.JOB_EVENT_DELETE:
		// 删除指定的任务调度计划
		if _, jobExisted = G_Scheduler.JobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(G_Scheduler.JobPlanTable, jobEvent.Job.Name)
			println("删除任务计划表中的任务" + jobEvent.Job.Name)
		}
	}
	// 当前的任务调度表
	fmt.Printf("当前任务调度表：%v\n", G_Scheduler.JobPlanTable)

	// 返回错误
	return err
}

// 推送任务事件
func (Scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	// 推送时间变化到队列中
	Scheduler.JobEventChan <- jobEvent
}

// 尝试进行任务调度
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		now             time.Time
		nearTime        *time.Time
		jobScheudlePlan *common.JobSchedulePlan
	)

	// 没有任务
	if len(scheduler.JobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// 记录当前时间
	now = time.Now()

	// 遍历所有的任务
	// 执行已经过期或者立即过期的任务
	for _, jobScheudlePlan = range scheduler.JobPlanTable {
		if jobScheudlePlan.NextTime.Before(now) || jobScheudlePlan.NextTime.Equal(now) {
			// TODO: 执行当前任务
			fmt.Println("执行任务" + jobScheudlePlan.Job.Name)
		}

		// 更新下次执行时间
		jobScheudlePlan.NextTime = jobScheudlePlan.CronExp.Next(now)

		// 最近一次需要执行的时间
		if nearTime == nil || nearTime.After(jobScheudlePlan.NextTime) {
			nearTime = &jobScheudlePlan.NextTime
		}
	}

	// 计算下次过期时间
	scheduleAfter = nearTime.Sub(now)
	return
}
