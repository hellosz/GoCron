package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/GoCron/src/worker"
)

var (
	configPath string
)

func main() {
	var (
		err error
	)
	// 初始化环境
	InitEnv()

	// 初始化参数
	InitArgs()

	// 初始化配置
	if err = worker.InitConf(configPath); err != nil {
		goto ERR
	}

	// 初始化任务管理
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	// 监控任务
	if err = worker.G_jobMgr.WatchJobs(); err != nil {
		goto ERR
	}

	time.Sleep(100 * time.Minute)

	return
ERR:
	fmt.Println(err)
}

// 初始化环境变量
func InitEnv() {
	// 设置进程数与 cpu 的数量一致
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// 初始化命令行参数
func InitArgs() {
	// 设置并解析参数
	flag.StringVar(&configPath, "config", "/private/var/www/GoCron/src/worker/main/config.json", "please input config file path")
	flag.Parse()
}
