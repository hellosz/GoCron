package master

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/GoCron/src/common"
)

var (
	G_apiServer *ApiServer
)

// 初始化路由
func saveJob(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobForm string
		job     common.Job
		oldJob  *common.Job
		resp    []byte
	)

	// 解析参数
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	// 初始化 Job
	jobForm = r.PostFormValue("job")
	if err = json.Unmarshal([]byte(jobForm), &job); err != nil {
		goto ERR
	}

	// 保存数据到 etcd 中
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	if resp, err = common.BuildReponse(0, "success", &oldJob); err != nil {
		goto ERR
	}

	w.Header().Add("content-type", "application/json")
	w.Write(resp)
	return

ERR:
	resp, _ = common.BuildReponse(-1, err.Error(), nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
}

// 删除任务
func deleteJob(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		resp   []byte
		jobKey string
		oldJob *common.Job
	)

	// 解析参数
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	jobKey = r.PostForm.Get("name")

	// 删除 etcd 中的任务，并且返回历史的结果
	if oldJob, err = G_jobMgr.DeleteJob(jobKey); err != nil {
		goto ERR
	}

	resp, _ = common.BuildReponse(0, "success", oldJob)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
	return

ERR:
	resp, _ = common.BuildReponse(-1, err.Error(), nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)

}

// 列出所有的任务
func listJobs(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobList []*common.Job
		resp    []byte
	)

	// 获取列表任务
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}

	// 返回结果
	resp, _ = common.BuildReponse(0, "success", jobList)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
	return

ERR:
	resp, _ = common.BuildReponse(-1, err.Error(), nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)

}

// 强杀任务
func killJob(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobName string
		resp    []byte
	)

	// 解析参数
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	jobName = r.PostForm.Get("name")

	// 通知进行任务强杀
	if err = G_jobMgr.KillJob(jobName); err != nil {
		goto ERR
	}

	// 返回成功响应
	resp, _ = common.BuildReponse(0, "success", nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
	return
ERR:
	// 返回失败原因
	resp, _ = common.BuildReponse(-1, err.Error(), nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
}

// 日志列表
func listLog(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		logList    []*common.JobLog
		jobName    string
		skipParam  string
		skip       int
		limitParam string
		limit      int
		resp       []byte
	)

	// 解析请求参数
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	jobName = r.Form.Get("name")
	fmt.Println("job_name", jobName)
	skipParam = r.Form.Get("skip")
	if skipParam == "" {
		skipParam = "0" // 设置默认值为 "0"
	}
	if skip, err = strconv.Atoi(skipParam); err != nil {
		goto ERR
	}
	limitParam = r.Form.Get("limit")
	if limitParam == "" {
		limitParam = "20" // 设置默认为 "20"
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		goto ERR
	}

	fmt.Printf("请求参数:%s, %d, %d", jobName, skip, limit)

	// 获取列表任务
	if logList, err = G_logMgr.ListLog(jobName, skip, limit); err != nil {
		goto ERR
	}

	// 返回结果
	resp, _ = common.BuildReponse(0, "success", logList)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)
	return

ERR:
	resp, _ = common.BuildReponse(-1, err.Error(), nil)
	w.Header().Add("content-type", "application/json")
	w.Write(resp)

}

// http 服务器
type ApiServer struct {
	httpServer *http.Server
}

// 初始化服务器
func InitServer() (err error) {
	var (
		httpServer *http.Server
		mux        *http.ServeMux
		listener   net.Listener
		publicDir  http.Dir
	)

	// 初始化路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", saveJob)
	mux.HandleFunc("/job/delete", deleteJob)
	mux.HandleFunc("/job/list", listJobs)
	mux.HandleFunc("/job/kill", killJob)
	mux.HandleFunc("/job/log", listLog)

	// 访问静态资源
	publicDir = http.Dir(G_config.Webroot)
	mux.Handle("/", http.FileServer(publicDir))

	// 注册监听器
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return err
	}

	// 设置服务器
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	// 开启协程监听请求
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}
	go httpServer.Serve(listener)

	return nil
}
