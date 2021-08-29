package master

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort         int      `json:"api_port,omitempty"`
	ApiReadTimeout  int      `json:"api_read_timeout,omitempty"`
	ApiWriteTimeout int      `json:"api_write_timeout,omitempty"`
	EtcdEndpoints   []string `json:"etcd_endpoints,omitempty"`
	EtcdDiaTimeout  int      `json:"etcd_dia_timeout,omitempty"`
}

var (
	// 全局单例配置
	G_config *Config
)

// 初始化项目代码
func InitConf(filepath string) error {
	var (
		data   []byte
		config Config
		err    error
	)

	// 读取文件配置
	if data, err = ioutil.ReadFile(filepath); err != nil {
		return err
	}

	// 解析参数
	config = Config{}
	if err = json.Unmarshal(data, &config); err != nil {
		return err
	}

	// 赋值单利变量
	G_config = &config
	return nil
}
