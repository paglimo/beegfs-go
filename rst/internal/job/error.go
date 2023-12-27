package job

import (
	"errors"
)

var (
	ErrUnknownJobOp = errors.New("an unknown job operation was requested")
)
