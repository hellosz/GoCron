package common

import "errors"

var (
	ERR_LOCK_ALREADDY_REQUIRED = errors.New("锁已经被占用")
)
