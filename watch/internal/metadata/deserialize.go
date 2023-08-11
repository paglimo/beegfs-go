package metadata

import (
	"encoding/binary"
	"fmt"

	pb "github.com/thinkparq/bee-protos/beewatch/go"
)

var (
	lenStart  uint32 = 28
	lenEnd    uint32 = 28 + 4
	strLength uint32
)

const (
	enableLengthBasedStringDeserialization = true
)

// deserialize takes a buffer and the number of bytes to deserialize from that buffer into an event.
// This allows it to work with a reusable fixed sized buffer to minimize allocations.
// If successful it will return a pointer to a new event that should be reused until sent to all subscribers.
// The the format of the deserialized event is defined as part of the external API at api/proto.
func deserialize(buf []byte, numBytes int) (*pb.Event, error) {

	var event pb.Event

	if numBytes < 28 {
		// If for some reason we received a packet that doesn't contain enough bytes for a header we should bail out.
		return &event, fmt.Errorf("unable to deserialize any packet fields because the provided buffer was smaller than the expected header size (packet size: %d)", numBytes)
	}

	// Integer values are easy to deserialize based on their sizes:
	event.FormatVersionMajor = uint32(binary.LittleEndian.Uint16(buf[0:2]))
	event.FormatVersionMinor = uint32(binary.LittleEndian.Uint16(buf[2:4]))
	event.Size = binary.LittleEndian.Uint32(buf[4:8])
	event.DroppedSeq = binary.LittleEndian.Uint64(buf[8:16])
	event.MissedSeq = binary.LittleEndian.Uint64(buf[16:24])
	event.Type = pb.Event_Type(binary.LittleEndian.Uint32(buf[24:28]))

	if numBytes < int(event.Size) {
		// If the packet size we parsed is smaller than the buffer we were provided we should bail out to avoid a panic.
		return &event, fmt.Errorf("only the packet header could be deserialized because the provided buffer was smaller than the indicated packet size (expected size: %d, actual size: %d)", event.Size, numBytes)
	}

	// TODO: Evaluate if we want to keep or remove this.
	// If we're not sure could leave in and expose a better way to enable/disable it.
	// It seems to be faster if the event size is larger. So we could also use it based on event size.
	// While at it evaluate if the global variables are actually helping improve performance.
	if enableLengthBasedStringDeserialization {
		// The remaining strings are separated by a uint32 value indicating the length of the string to follow.
		// Use that to calculate the start and end index of each string.
		lenStart = 28
		lenEnd = 28 + 4

		// Get the entry ID:
		strLength = binary.LittleEndian.Uint32(buf[lenStart:lenEnd])
		event.EntryId = string(buf[lenEnd : lenEnd+strLength])

		// Get the parent entry ID:
		lenStart = lenEnd + strLength + 1 // The length doesn't include the null terminator.
		lenEnd = lenStart + 4
		strLength = binary.LittleEndian.Uint32(buf[lenStart:lenEnd])
		event.ParentEntryId = string(buf[lenEnd : lenEnd+strLength])

		// Get the path:
		lenStart = lenEnd + strLength + 1 // The length doesn't include the null terminator.
		lenEnd = lenStart + 4
		strLength = binary.LittleEndian.Uint32(buf[lenStart:lenEnd])
		event.Path = string(buf[lenEnd : lenEnd+strLength])

		// Get the target path:
		lenStart = lenEnd + strLength + 1 // The length doesn't include the null terminator.
		lenEnd = lenStart + 4
		strLength = binary.LittleEndian.Uint32(buf[lenStart:lenEnd])
		if strLength == 0 {
			return &event, nil
		}
		event.TargetPath = string(buf[lenEnd : lenEnd+strLength])

		// Get the target parent ID:
		lenStart = lenEnd + strLength + 1 // The length doesn't include the null terminator.
		lenEnd = lenStart + 4
		strLength = binary.LittleEndian.Uint32(buf[lenStart:lenEnd])
		event.TargetParentId = string(buf[lenEnd : lenEnd+strLength])

		return &event, nil
	}

	// The rest of the packet is an array of strings.
	// Each entry starts with a uint32 value indicating the length of the string to follow.
	// Each string also ends with a null character (\x00).
	// While we could deserialize the length of each string then parse the actual string,
	// we can also just ignore the length values and parse this as a null-terminated character array.
	// Probably there is little actual difference in performance between these two approaches.
	// However deserializing the length of each string then parsing may require more memory allocations depending on implementation.
	start := 32 // We known the first string is here:

	for end := start + 1; end < int(event.Size); end++ {
		if buf[end] == '\x00' {
			if event.EntryId == "" {
				event.EntryId = string(buf[start:end])
			} else if event.ParentEntryId == "" {
				event.ParentEntryId = string(buf[start:end])
			} else if event.Path == "" {
				event.Path = string(buf[start:end])
			} else if event.TargetPath == "" {
				event.TargetPath = string(buf[start:end])
			} else if event.TargetParentId == "" {
				event.TargetParentId = string(buf[start:end])
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

	return &event, nil
}
