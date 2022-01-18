package master

import (
	"context"
	"time"

	"github.com/GoCron/src/common"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

// 显示所有注册的 Worker 节点
func (wokerMgr *WorkerMgr) ListWorkers() (workerIpList []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		ipStr   string
	)
	// 初始化字符串数组
	workerIpList = make([]string, 0)

	// 获取所有节点
	if getResp, err = wokerMgr.kv.Get(context.TODO(),
		common.CRON_WORKER_DIR,
		clientv3.WithPrefix()); err != nil {
		return
	}

	// 解析 IP
	for _, kv = range getResp.Kvs {
		ipStr = common.ParseWorkerIP(string(kv.Key))
		workerIpList = append(workerIpList, ipStr)
	}

	// 返回结果
	return
}

// 初始化工作管理者
func InitWokerMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// 连接 ETCD
	config = clientv3.Config{Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDiaTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	// 获取本地地址
	return nil
}
