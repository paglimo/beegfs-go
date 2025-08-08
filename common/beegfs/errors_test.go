package beegfs

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrMappings(t *testing.T) {
	// OpsErrs can be matched against standard error numbers:
	assert.True(t, errors.Is(OpsErr_PATHNOTEXISTS, syscall.ENOENT))
	assert.True(t, errors.Is(OpsErr_PATHNOTEXISTS, fs.ErrNotExist))
	assert.True(t, errors.Is(OpsErr_PATHNOTEXISTS, os.ErrNotExist))
	assert.True(t, errors.Is(OpsErr_INUSE, syscall.EBUSY))
	// EAGAIN / EWOULDBLOCK are the same on most Unix systems.
	assert.True(t, errors.Is(OpsErr_WOULDBLOCK, syscall.EAGAIN))
	assert.True(t, errors.Is(OpsErr_WOULDBLOCK, syscall.EWOULDBLOCK))
	assert.True(t, errors.Is(OpsErr_AGAIN, syscall.EAGAIN))
	assert.True(t, errors.Is(OpsErr_AGAIN, syscall.EWOULDBLOCK))
	//lint:ignore SA1032 testing forward-only matching
	assert.False(t, errors.Is(os.ErrNotExist, OpsErr_PATHNOTEXISTS))
	// OpsErrs only match standard errors they are mapped to:
	assert.False(t, errors.Is(OpsErr_PATHNOTEXISTS, os.ErrPermission))
}
