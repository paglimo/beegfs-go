package types

import (
	"encoding/binary"
	"fmt"
)

type FileEventType uint32

const (
	FLUSH       FileEventType = 0
	TRUNCATE    FileEventType = 1
	SETATTR     FileEventType = 2
	CLOSE_WRITE FileEventType = 3
	CREATE      FileEventType = 4
	MKDIR       FileEventType = 5
	MKNOD       FileEventType = 6
	SYMLINK     FileEventType = 7
	RMDIR       FileEventType = 8
	UNLINK      FileEventType = 9
	HARDLINK    FileEventType = 10
	RENAME      FileEventType = 11
	READ        FileEventType = 12
)

func (f FileEventType) String() string {
	switch f {
	case FLUSH:
		return "Flush"
	case TRUNCATE:
		return "Truncate"
	case SETATTR:
		return "SetAttr"
	case CLOSE_WRITE:
		return "CloseAfterWrite"
	case CREATE:
		return "Create"
	case MKDIR:
		return "MKdir"
	case MKNOD:
		return "MKnod"
	case SYMLINK:
		return "Symlink"
	case RMDIR:
		return "RMdir"
	case UNLINK:
		return "Unlink"
	case HARDLINK:
		return "Hardlink"
	case RENAME:
		return "Rename"
	case READ:
		return "Read"
	}
	return ""
}

type Packet struct {
	FormatVersionMajor uint16
	FormatVersionMinor uint16
	Size               uint32
	DroppedSeq         uint64
	MissedSeq          uint64
	Type               FileEventType
	Path               string
	EntryId            string
	ParentEntryId      string
	TargetPath         string
	TargetParentId     string
}

// Deserialize takes a byte slice containing a Packet and converts it into a struct.
// At this time it does not have any way of verifying the format of the packet is what is expected.
// It only performs basis checks to ensure the provided buffer is large enough to deserialize to avoid a panic.
// If there is an issue with the packet size the packet will be returned with some or all fields empty.
// It is up to the caller to decide what to do with the packet in this scenario (i.e., drop or keep anyway).
func (p *Packet) Deserialize(buf []byte) error {

	actualSize := len(buf)
	if actualSize < 28 {
		// If for some reason we received a packet that doesn't contain enough bytes for a header we should bail out.
		return fmt.Errorf("unable to deserialize any packet fields because the provided buffer was smaller than the expected header size (packet size: %d)", actualSize)
	}

	// Integer values are easy to deserialize based on their sizes:
	p.FormatVersionMajor = binary.LittleEndian.Uint16(buf[0:2])
	p.FormatVersionMinor = binary.LittleEndian.Uint16(buf[2:4])
	p.Size = binary.LittleEndian.Uint32(buf[4:8])
	p.DroppedSeq = binary.LittleEndian.Uint64(buf[8:16])
	p.MissedSeq = binary.LittleEndian.Uint64(buf[16:24])
	p.Type = FileEventType(binary.LittleEndian.Uint32(buf[24:28]))

	if actualSize < int(p.Size) {
		// If the packet size we parsed is smaller than the buffer we were provided we should bail out to avoid a panic.
		return fmt.Errorf("only the packet header could be deserialized because the provided buffer was smaller than the indicated packet size (expected size: %d, actual size: %d)", p.Size, actualSize)
	}

	// The rest of the packet is an array of strings.
	// Each entry starts with a uint32 value indicating the length of the string to follow.
	// Each string also ends with a null character (\x00).
	// While we could deserialize the length of each string then parse the actual string,
	// we can also just ignore the length values and parse this as a null-terminated character array.
	// Probably there is little actual difference in performance between these two approaches.
	// However deserializing the length of each string then parsing may require more memory allocations depending on implementation.
	start := 32 // We known the first string is here:

	for end := start + 1; end < int(p.Size); end++ {
		if buf[end] == '\x00' {
			if p.EntryId == "" {
				p.EntryId = string(buf[start:end])
			} else if p.ParentEntryId == "" {
				p.ParentEntryId = string(buf[start:end])
			} else if p.Path == "" {
				p.Path = string(buf[start:end])
			} else if p.TargetPath == "" {
				p.TargetPath = string(buf[start:end])
			} else if p.TargetParentId == "" {
				p.TargetParentId = string(buf[start:end])
			}
			// We know each entry is padded with a uint32 so we'll just skip to the expected start of the string portion:
			start = end + 5
			end = start + 1
			// Check if we have additional strings to parse.
			// Notably TargetPath and TargetParentId are only included with some event types.
			if buf[start] == '\x00' {
				break // Terminate early so we don't loop over empty bytes.
			}
		}
	}
	return nil
}
