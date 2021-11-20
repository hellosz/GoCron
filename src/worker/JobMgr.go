package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GoCron/src/common"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

// Etcd 任务管理
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

// 初始化任务管理
func InitJobMgr() (err error) {
	var (
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		config  clientv3.Config
		watcher clientv3.Watcher
	)

	// 设置连接配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDiaTimeout) * time.Millisecond,
	}

	// 创建连接
	if client, err = clientv3.New(config); err != nil {
		fmt.Println(err)
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 设置全局单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	return
}

// 监听任务
func (jobMgr *JobMgr) WatchJobs() (err error) {
	var (
		jobDir             string
		job                *common.Job
		getResp            *clientv3.GetResponse
		kv                 *mvccpb.KeyValue
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		event              *clientv3.Event
		jobEventType       int
		jobEvent           *common.JobEvent
		startWatchRevision int64
	)

	// 获取当前所有任务，加入任务队列中
	jobDir = common.CRON_JOB_DIR
	if getResp, err = G_jobMgr.kv.Get(context.TODO(), jobDir, clientv3.WithPrefix()); err != nil {
		return
	}

	// 遍历当前的任务
	for _, kv = range getResp.Kvs {
		if job, err = common.UnpackJob(kv.Value); err == nil {
			// 初始化任务到任务调度表中
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_PUT, job)

			// 推送任务
			G_Scheduler.PushJobEvent(jobEvent)
		}
	}

	// 开启协程，监听任务
	go func() {
		// 监听任务事件
		// 指定监控目录以及版本
		startWatchRevision = getResp.Header.GetRevision() + 1
		if watchChan = G_jobMgr.client.Watch(context.TODO(), jobDir,
			clientv3.WithRev(startWatchRevision), clientv3.WithPrefix()); err != nil {
			return
		}

		for watchResp = range watchChan {
			for _, event = range watchResp.Events {
				fmt.Println(string(event.Kv.Key))
				switch event.Type {
				case clientv3.EventTypePut:
					// put 事件处理
					jobEventType = common.JOB_EVENT_PUT

					// 解析出错，静默处理
					if job, err = common.UnpackJob(event.Kv.Value); err != nil {
						err = nil
						continue
					}

				case clientv3.EventTypeDelete:
					// delete 事件处理
					jobEventType = common.JOB_EVENT_DELETE

					// 构造 job（去掉前缀目录）
					job = &common.Job{Name: strings.TrimPrefix(string(event.Kv.Key), common.CRON_JOB_DIR+"/")}
				}

				// 构造事件，给到任务调度中心
				jobEvent = common.BuildJobEvent(jobEventType, job)

				// 事件推送到任务调度中心
				G_Scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return nil
}

// 创建任务锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	return InitJobLock(jobName, G_jobMgr.kv, G_jobMgr.lease)
}
