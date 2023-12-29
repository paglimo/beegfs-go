package job

import (
	"errors"
)

var (
	ErrUnknownJobOp           = errors.New("an unknown job operation was requested")
	ErrIncompatibleNodeAndRST = errors.New("node type does not support the requested RST")
)
