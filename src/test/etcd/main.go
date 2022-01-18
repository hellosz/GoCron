package main

import (
	"fmt"

	"github.com/GoCron/src/common"
)

func main() {
	var (
		localIP string
		err     error
	)

	if localIP, err = common.GetLocalIP(); err != nil {
		fmt.Errorf("获取 IPV4 地址报错，原因：%s", err.Error())
	}

	fmt.Println("本地 IPV4 地址:" + localIP)
}
