package eventlog

import (
	"encoding/binary"
	"fmt"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
)

// Types for BeeGFS events are defined as part of the external API at api/proto.
// This file contains functions that deserialize BeeGFS modification events packets into this format.

func deserialize(buf []byte) (*pb.Event, error) {

	var event pb.Event

	actualSize := len(buf)
	if actualSize < 28 {
		// If for some reason we received a packet that doesn't contain enough bytes for a header we should bail out.
		return &event, fmt.Errorf("unable to deserialize any packet fields because the provided buffer was smaller than the expected header size (packet size: %d)", actualSize)
	}

	// Integer values are easy to deserialize based on their sizes:
	event.FormatVersionMajor = uint32(binary.LittleEndian.Uint16(buf[0:2]))
	event.FormatVersionMinor = uint32(binary.LittleEndian.Uint16(buf[2:4]))
	event.Size = binary.LittleEndian.Uint32(buf[4:8])
	event.DroppedSeq = binary.LittleEndian.Uint64(buf[8:16])
	event.MissedSeq = binary.LittleEndian.Uint64(buf[16:24])
	event.Type = pb.Event_Type(binary.LittleEndian.Uint32(buf[24:28]))

	if actualSize < int(event.Size) {
		// If the packet size we parsed is smaller than the buffer we were provided we should bail out to avoid a panic.
		return &event, fmt.Errorf("only the packet header could be deserialized because the provided buffer was smaller than the indicated packet size (expected size: %d, actual size: %d)", event.Size, actualSize)
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
