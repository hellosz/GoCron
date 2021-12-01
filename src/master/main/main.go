package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/GoCron/src/master"
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
	if err = master.InitConf(configPath); err != nil {
		goto ERR
	}

	// 初始化任务管理
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 初始化服务器
	if err = master.InitServer(); err != nil {
		goto ERR
	}

	// 初始化日志
	if err = master.InitLogMgr(); err != nil {
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
	flag.StringVar(&configPath, "config", "/private/var/www/GoCron/src/master/main/config.json", "please input config file path")
	flag.Parse()
}
