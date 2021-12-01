package master

import (
	"context"
	"time"

	"github.com/GoCron/src/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 日志管理
type LogMgr struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// 日志全局单例
var (
	G_logMgr *LogMgr
)

// 初始化日志管理
func InitLogMgr() (err error) {
	var (
		client     *mongo.Client
		collection *mongo.Collection
	)

	// 初始化客户端连接
	if client, err = mongo.Connect(context.TODO(),
		options.Client().ApplyURI(G_config.MongoConnectionUri),
		options.Client().SetConnectTimeout(time.Duration(G_config.MongoConnectionTimeout)*time.Millisecond),
	); err != nil {
		return
	}

	// 连接测试
	if err = client.Ping(context.TODO(), nil); err != nil {
		return
	}

	collection = client.Database("cron").Collection("records")
	G_logMgr = &LogMgr{
		client:     client,
		collection: collection,
	}
	return
}

// 返回任务执行日志
func (logMgr *LogMgr) ListLog(jobName string, skip int, limit int) (jobLogArr []*common.JobLog, err error) {
	var (
		jobLogFilter *common.JobLogFilter
		cursor       *mongo.Cursor
		jobLog       *common.JobLog
	)
	// 初始化日志结果列表（避免空指针引起的数据异常）
	jobLogArr = make([]*common.JobLog, 0)

	// 数据查询
	jobLogFilter = &common.JobLogFilter{JobName: jobName}
	if cursor, err = G_logMgr.collection.Find(context.TODO(),
		jobLogFilter,
		options.Find().SetSkip(int64(skip)),
		options.Find().SetLimit(int64(limit))); err != nil {
		return
	}

	// 解析数据
	for cursor.Next(context.TODO()) {
		if err = cursor.Decode(&jobLog); err != nil {
			continue
		}

		jobLogArr = append(jobLogArr, jobLog)
	}

	return
}
