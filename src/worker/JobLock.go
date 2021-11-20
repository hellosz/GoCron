package worker

import (
	"context"
	"fmt"

	"github.com/GoCron/src/common"
	"go.etcd.io/etcd/clientv3"
)

// 任务锁
type JobLock struct {
	Kv      clientv3.KV
	Lease   clientv3.Lease
	JobName string // 任务名称

	LeaseId    clientv3.LeaseID   // 租约ID
	CancelFunc context.CancelFunc // 自动续租取消方法
	IsLocked   bool               // 是否已经上锁
}

// 初始化任务锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		Kv:      kv,
		Lease:   lease,
		JobName: jobName,
	}

	return
}

// 尝试获取锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseResp    *clientv3.LeaseGrantResponse
		cancelCtx    context.Context
		cancelFunc   context.CancelFunc
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		txn          clientv3.Txn
		txnResp      *clientv3.TxnResponse
		lockName     string
	)

	// 创建租约
	if leaseResp, err = jobLock.Lease.Grant(context.TODO(), 5); err != nil {
		goto FAIL
	}

	// 自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	if keepRespChan, err = jobLock.Lease.KeepAlive(cancelCtx, leaseResp.ID); err != nil {
		goto FAIL
	}

	// 响应自动续租
	go func() {
		var (
			keepAliveResp *clientv3.LeaseKeepAliveResponse
		)
		for keepAliveResp = range keepRespChan {
			fmt.Printf("自动续租：%d\n", keepAliveResp.GetRevision())
		}
	}()

	// 在事务中申请锁
	lockName = common.CRON_LOCK_DIR + jobLock.JobName
	txn = jobLock.Kv.Txn(context.TODO())
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockName), "=", 0)).
		Then(clientv3.OpPut(lockName, "", clientv3.WithLease(leaseResp.ID))).
		Else(clientv3.OpGet(lockName))

	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	if !txnResp.Succeeded {
		// 没有获取到锁
		err = common.ERR_LOCK_ALREADDY_REQUIRED
		goto FAIL
	}

	// 获取到锁，设置属性
	jobLock.CancelFunc = cancelFunc
	jobLock.LeaseId = leaseResp.ID
	jobLock.IsLocked = true // 已经上锁标识

	return

FAIL:
	// 取消续组，且回收租约
	cancelFunc()
	jobLock.Lease.Revoke(context.TODO(), jobLock.LeaseId) // TODO 是否会出现不存在的情况
	return
}

// 释放锁
func (jobLock *JobLock) Unlock() {
	if jobLock.IsLocked {
		// 取消续组，且回收租约
		jobLock.CancelFunc()
		jobLock.Lease.Revoke(context.TODO(), jobLock.LeaseId)
	}
}
