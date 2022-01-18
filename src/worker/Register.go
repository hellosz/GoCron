package worker

import (
	"context"
	"time"

	"github.com/GoCron/src/common"
	"go.etcd.io/etcd/clientv3"
)

// 注册发现中心
type Register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIP string
}

var (
	G_register *Register
)

// 注册工作者(并自动续租)
func (register *Register) keepOnline(ip string) {
	var (
		leaseGrantResponse *clientv3.LeaseGrantResponse
		keepAliveChan      <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp      *clientv3.LeaseKeepAliveResponse
		cancelCtx          context.Context
		cancelFunc         context.CancelFunc
		regKey             string
		err                error
	)
	for {
		// 初始化取消方法
		cancelFunc = nil

		// 将当前主机的 IP 注册到 ETCD 中
		// 申请租约
		if leaseGrantResponse, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		// 租约续租
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())
		if keepAliveChan, err = register.lease.KeepAlive(cancelCtx, leaseGrantResponse.ID); err != nil {
			goto RETRY
		}

		// 注册工作者节点
		regKey = common.CRON_WORKER_DIR + ip
		if _, err = register.client.Put(context.TODO(), regKey, "", clientv3.WithLease(leaseGrantResponse.ID)); err != nil {
			goto RETRY
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil {
					goto RETRY
				}
			}
		}

		// 异常处理(重试)
	RETRY:
		time.Sleep(1 * time.Second)

		// 取消续租
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

// 初始化注册中心
func InitRegister() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIP string
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
	G_register = &Register{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	// 获取本地地址
	if localIP, err = common.GetLocalIP(); err != nil {
		return
	}
	G_register.localIP = localIP

	// 注册工作者节点
	G_register.keepOnline(localIP)

	return nil
}
