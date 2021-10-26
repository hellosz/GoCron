package worker

import (
	"fmt"

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
		jobEvent *common.JobEvent
	)

	for {
		select {
		case jobEvent = <-G_Scheduler.JobEventChan:
			handleJobEvent(jobEvent)
		}
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
			fmt.Println("delete event if case")
		} else {
			fmt.Println("delete event else case")

		}

		fmt.Println(jobExisted)
	}
	// 当前的任务调度表
	fmt.Printf("当前任务调度表：%v", G_Scheduler.JobPlanTable)

	// 返回错误
	return err
}

// 推送任务事件
func (Scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	// 推送时间变化到队列中
	Scheduler.JobEventChan <- jobEvent
}
