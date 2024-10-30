package worker

import "errors"

var (
	ErrWorkRequestNotFound = errors.New("work request not found on worker node")
)
