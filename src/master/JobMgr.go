package master

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/GoCron/src/common"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

// Etcd 任务管理
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

// 初始化任务管理
func InitJobMgr() (err error) {
	var (
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
		config clientv3.Config
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

	// 设置全局单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存 job 到 etcd 中
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey       string
		jobValue     []byte
		putResp      *clientv3.PutResponse
		oldJobObject common.Job
	)

	// 获取 key 和 value
	jobKey = common.CRON_JOB_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	// 创建/更新数据
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	log.Println(jobKey, jobValue)

	// 保存成功（如果是更新，则获取历史值）
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObject); err != nil {
			log.Println("second unmarshal failed")
			err = nil
			return
		}
	}

	oldJob = &oldJobObject

	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (job *common.Job, err error) {
	var (
		delResp *clientv3.DeleteResponse
		prevKV  []*mvccpb.KeyValue
		jobKey  string
		oldJob  common.Job
	)

	// 删除 etcd 中的结果
	jobKey = common.CRON_JOB_DIR + name
	if delResp, err = G_jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 解析历史数据
	prevKV = delResp.PrevKvs
	if len(prevKV) > 0 {
		if err = json.Unmarshal(prevKV[0].Value, &oldJob); err == nil {
			job = &oldJob
			return
			// 抑制因为解析旧的 json 报错导致的程序异常
			err = nil
		}
	}

	return
}

// 读取任务列表
func (jobMgr *JobMgr) ListJobs() (listJobs []*common.Job, err error) {
	var (
		getResp  *clientv3.GetResponse
		job      *common.Job
		jobDir   string
		keyValue *mvccpb.KeyValue
	)

	// 任务调度的主目录
	jobDir = common.CRON_JOB_DIR
	if getResp, err = G_jobMgr.kv.Get(context.TODO(), jobDir, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化 listJobs
	listJobs = make([]*common.Job, 0)
	if len(getResp.Kvs) > 0 {
		for _, keyValue = range getResp.Kvs {
			job = &common.Job{}
			// 解析成对象
			if err = json.Unmarshal(keyValue.Value, job); err != nil {
				err = nil
				continue
			}

			// 将数据添加到列表
			listJobs = append(listJobs, job)
		}
	}
	return
}

// 强杀任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	var (
		// putResp   *clientv3.PutResponse
		leaseResp *clientv3.LeaseGrantResponse
		leaseId   clientv3.LeaseID
		jobKey    string
	)

	// 任务的 key 值
	jobKey = common.CRON_KILL_JOB + name

	// 申请 1 秒的租约
	if leaseResp, err = G_jobMgr.client.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 通知进行强杀
	leaseId = leaseResp.ID
	fmt.Println("kill job key", jobKey)
	if _, err = G_jobMgr.client.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}
