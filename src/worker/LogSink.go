package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/GoCron/src/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 日志处理类
type LogSink struct {
	client     *mongo.Client       // mongo 客户端
	collection *mongo.Collection   // mongo 集合
	logChan    chan *common.JobLog // 日志通道
	batchLog   *common.LogBatch    // 批量日志缓存
}

var (
	G_LogSink *LogSink
)

// 初始化
func InitLogSink() (err error) {
	var (
		client     *mongo.Client
		collection *mongo.Collection
	)

	// 初始化 mongo 连接
	if client, err = mongo.Connect(context.TODO(),
		options.Client().ApplyURI(G_config.MongoConnectionUri),
		options.Client().SetConnectTimeout(5*time.Second),
	); err != nil {
		return
	}

	// 验证是否连接成功
	if err = client.Ping(context.TODO(), nil); err != nil {
		return
	}

	collection = client.Database(G_config.MongoDefaultDatabase).Collection("records")

	// 初始化 LogSink
	G_LogSink = &LogSink{
		client:     client,
		collection: collection,
		logChan:    make(chan *common.JobLog, 1000),
	}

	// 写入日志到 mongo
	go writeLoop()

	return
}

// 写入日志到 mongo
func writeLoop() {
	var (
		jobLog *common.JobLog
		ticker *time.Ticker
	)

	ticker = time.NewTicker(time.Second * 1) // 一秒钟执行一次
	for {
		select {
		case jobLog = <-G_LogSink.logChan:
			// 保存日志
			if G_LogSink.batchLog == nil {
				// TODO 这里初始化的 1000 是否正确
				G_LogSink.batchLog = &common.LogBatch{}
			}

			// 日志加入缓存
			G_LogSink.batchLog.Logs = append(G_LogSink.batchLog.Logs, jobLog)

			// 批量保存缓存数据
			if len(G_LogSink.batchLog.Logs) > int(G_config.LogBatchSize) {
				fmt.Println("批处理保存日志")
				// 保存数据 数据保存需要异步处理
				go func(logs []interface{}) {
					G_LogSink.batchSaveLog(logs)
				}(G_LogSink.batchLog.Logs)

				// 清空数据
				G_LogSink.batchLog = nil
			}

		case <-ticker.C:
			if G_LogSink.batchLog != nil {
				fmt.Println("定时保存日志")
				// 保存数据 数据保存需要异步处理
				go func(logs []interface{}) {
					G_LogSink.batchSaveLog(logs)
				}(G_LogSink.batchLog.Logs)

				// 清空数据
				G_LogSink.batchLog = nil
			}
		}
	}

}

// 执行完成，通知写日志操作
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	logSink.logChan <- jobLog
}

// 批量保存日志
func (logSink *LogSink) batchSaveLog(logs []interface{}) (err error) {
	var (
		insertId interface{}
	)
	fmt.Println("打印日志内容")
	for _, v := range logs {
		fmt.Printf("保存数据:%#v\n", v)
	}
	var (
		result *mongo.InsertManyResult
	)
	if result, err = logSink.collection.InsertMany(context.TODO(), logs); err != nil {
		fmt.Printf("保存数据到 mongo出错:%s\n", err.Error())
		return
	}

	for _, insertId = range result.InsertedIDs {
		fmt.Printf("插入数据的ID为:%#v", insertId)

	}

	fmt.Println("保存数据插入ID", result.InsertedIDs)
	return nil
}
