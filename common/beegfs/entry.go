package beegfs

import (
	"fmt"
	"strconv"
	"strings"
)

// Go representation of the BeeGFS `DirEntryType` enum defined in:
//   - client_module/source/common/storage/StorageDefinitions.h
//   - common/source/common/storage/StorageDefinitions.h
type EntryType uint32

const (
	EntryUnknown EntryType = iota
	EntryDirectory
	EntryRegularFile
	EntrySymlink
	EntryBlockDev
	EntryCharDev
	EntryFIFO
	EntrySOCKET
)

// Is file returns true for any kind of file, including symlinks and special files.
func (t EntryType) IsFile() bool {
	return t >= 2 && t <= 7
}

func (t EntryType) String() string {
	switch t {
	case EntryDirectory:
		return "directory"
	case EntryRegularFile:
		return "file"
	case EntrySymlink:
		return "symlink"
	case EntryBlockDev:
		return "block device node"
	case EntryCharDev:
		return "character device node"
	case EntryFIFO:
		return "pipe"
	case EntrySOCKET:
		return "unix domain socket"
	default:
		return "invalid"
	}
}

// Equivalent of StripePatternType in C++.
type StripePatternType uint32

const (
	StripePatternInvalid StripePatternType = iota
	StripePatternRaid0
	StripePatternRaid10
	StripePatternBuddyMirror
)

func (p StripePatternType) String() string {
	switch p {
	case StripePatternRaid0:
		return "RAID0"
	case StripePatternRaid10:
		return "RAID10"
	case StripePatternBuddyMirror:
		return "Buddy Mirror"
	default:
		return "invalid"
	}
}

type EntryFeatureFlags int32

const (
	// Equivalent of ENTRYINFO_FEATURE_INLINED in C++.
	entryFeatureFlagInlined EntryFeatureFlags = 1
	// Equivalent of ENTRYINFO_FEATURE_BUDDYMIRRORED in C++.
	entryFeatureFlagBuddyMirrored EntryFeatureFlags = 2
)

func (f EntryFeatureFlags) IsInlined() bool {
	return f&entryFeatureFlagInlined != 0
}

func (f *EntryFeatureFlags) SetInlined() {
	*f |= entryFeatureFlagInlined
}

func (f EntryFeatureFlags) IsBuddyMirrored() bool {
	return f&entryFeatureFlagBuddyMirrored != 0
}

func (f *EntryFeatureFlags) SetBuddyMirrored() {
	*f |= entryFeatureFlagBuddyMirrored
}

// FileState represents the combined AccessState (lower 5 bits) and HsmState (upper 3 bits).
type FileState uint8

// AccessState represents the access control settings in the lower 5 bits of FileState
type AccessState uint8

// Constants for AccessState
const (
	// No access restrictions (default access mode)
	AccessStateUnlocked AccessState = 0x00
	// Block read operations (file is "write-only")
	AccessStateReadLock AccessState = 0x01
	// Block write operations (file is "read-only")
	AccessStateWriteLock AccessState = 0x02
	// Reserved for future use
	AccessStateReserved3 AccessState = 0x04
	// Reserved for future use
	AccessStateReserved4 AccessState = 0x08
)

// HsmState represents an HSM-defined state (upper 3 bits of FileState)
type HsmState uint8

// Masks for extracting parts of the state
const (
	// Mask for extracting access state (lower 5 bits)
	AccessStateMask FileState = 0x1F
	// Mask for extracting HSM state (upper 3 bits)
	HsmStateMask FileState = 0xE0
	// Number of bits to shift for HSM state
	HsmStateShift = 5
)

// NewFileState combines AccessState and HsmState into a single byte
// Lower 5 bits represent access state, upper 3 bits represent HSM state
func NewFileState(accessState AccessState, hsmState HsmState) FileState {
	return (FileState(accessState) & AccessStateMask) |
		(FileState(hsmState<<HsmStateShift) & HsmStateMask)
}

// GetAccessState returns the access state part of the file state.
func (fs FileState) GetAccessState() AccessState {
	return AccessState(fs & AccessStateMask)
}

// GetHsmState returns the HSM state part of the file state.
func (fs FileState) GetHsmState() HsmState {
	return HsmState((fs & HsmStateMask) >> HsmStateShift) // Shift right to get the original value
}

// Helper methods for checking access restrictions
func (fs FileState) IsUnlocked() bool {
	return fs.GetAccessState() == AccessStateUnlocked
}

func (fs FileState) IsReadLocked() bool {
	return (fs.GetAccessState() & AccessStateReadLock) != 0
}

func (fs FileState) IsWriteLocked() bool {
	return (fs.GetAccessState() & AccessStateWriteLock) != 0
}

func (fs FileState) IsReadWriteLocked() bool {
	return fs.IsReadLocked() && fs.IsWriteLocked()
}

// GetRawValue returns the raw byte value of the file state
func (fs FileState) GetRawValue() uint8 {
	return uint8(fs)
}

// String returns a human-readable representation of the file state.
func (state FileState) String() string {
	accessStr := accessStateToString(state.GetAccessState())
	hsmStr := fmt.Sprintf("%d", state.GetHsmState())
	return fmt.Sprintf("%s : %s", accessStr, hsmStr)
}

// Helper function to convert access state to a string.
func accessStateToString(state AccessState) string {
	switch state {
	case AccessStateUnlocked:
		return "Unlocked"
	case AccessStateReadLock:
		return "Locked (read)" // Indicates READ operations are blocked (write-only)
	case AccessStateWriteLock:
		return "Locked (write)" // Indicates WRITE operations are blocked (read-only)
	case AccessStateReadLock | AccessStateWriteLock:
		return "Locked (read+write)" // All access blocked
	default:
		// For combinations with reserved bits
		return fmt.Sprintf("Unknown(%d)", state)
	}
}

// ParseFileState parses a string representation into a FileState.
func ParseFileState(s string) (FileState, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid file state format: %s (expected format: access:state)", s)
	}

	accessState, err := parseAccessState(parts[0])
	if err != nil {
		return 0, err
	}

	hsmState, err := parseHsmState(parts[1])
	if err != nil {
		return 0, err
	}

	return NewFileState(accessState, hsmState), nil
}

// Helper function to parse access state from a string.
func parseAccessState(s string) (AccessState, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "unlocked", "none":
		return AccessStateUnlocked, nil
	case "locked (read)", "read-lock":
		return AccessStateReadLock, nil
	case "locked (write)", "write-lock":
		return AccessStateWriteLock, nil
	case "locked (read+write)", "read-write-lock":
		return AccessStateReadLock | AccessStateWriteLock, nil
	default:
		return 0, fmt.Errorf("invalid access state: %s (valid values: unlocked, locked (read), locked (write), locked (read+write))", s)
	}
}

// Helper function to parse HSM state from a string.
func parseHsmState(s string) (HsmState, error) {
	s = strings.TrimSpace(s)

	// Parse as a number (0-7)
	if val, err := strconv.ParseUint(s, 10, 8); err == nil {
		if val > 7 {
			return 0, fmt.Errorf("invalid HSM state value: %d (must be 0-7)", val)
		}
		return HsmState(val), nil
	}

	return 0, fmt.Errorf("invalid HSM state: %s (must be a numeric value between 0-7)", s)
}
