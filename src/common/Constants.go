package common

// 调度任务目录
const CRON_JOB_DIR = "/cron/job/"

// 调度任务待杀任务
const CRON_KILL_JOB = "/cron/kill/"

// 任务调度锁目录
const CRON_LOCK_DIR = "/cron/lock/"

// 任务工作和目录
const CRON_WORKER_DIR = "/cron/worker/"

// 任务 PUT 操作
const JOB_EVENT_PUT = 1

// 任务 DELETE 操作
const JOB_EVENT_DELETE = 2

// 任务 KILL 操作
const JOB_EVENT_KILL = 3
