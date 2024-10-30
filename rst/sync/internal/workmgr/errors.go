package workmgr

import "errors"

var (
	ErrNotReady               = errors.New("work manager is not ready yet")
	ErrConfigUpdateNotAllowed = errors.New("updating BeeSync configuration after it was initially set is not currently supported")
)
