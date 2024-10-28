package metadata

import (
	"encoding/binary"
	"fmt"

	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// deserialize takes a buffer and the number of bytes to deserialize from that buffer into an event.
// This allows it to work with a reusable fixed sized buffer to minimize allocations. If successful
// it will return a pointer to a new event that should be reused until sent to all subscribers. The
// the format of the deserialized event is defined as part of the external API at:
// https://github.com/ThinkParQ/protobuf/blob/main/proto/beewatch.proto.
func deserialize(buf []byte, numBytes int) (*pb.Event, error) {

	var event pb.Event
	// If for some reason we received a packet that doesn't contain enough bytes for a header (28) we should bail out.
	if numBytes < 28 {
		return &event, fmt.Errorf("unable to deserialize any packet fields because the provided buffer was smaller than the expected header size (packet size: %d)", numBytes)
	}

	// Deserialize the header to determine the event version:
	major := binary.LittleEndian.Uint16(buf[0:2])
	minor := binary.LittleEndian.Uint16(buf[2:4])
	size := binary.LittleEndian.Uint32(buf[4:8])

	if numBytes < int(size) {
		// If the size parsed from the packet is smaller than bytes read into the buffer we should
		// bail, otherwise stale data in the buffer may be interpreted as fields for this event.
		return &event, fmt.Errorf("only the packet header could be deserialized because the provided buffer was smaller than the indicated packet size (expected size: %d, actual size: %d)", size, numBytes)
	}

	event.DroppedSeq = binary.LittleEndian.Uint64(buf[8:16])
	if major == 2 && minor == 0 {
		event.EventData = parseV2Event(buf)
	} else if major == 1 && minor == 0 {
		event.EventData = parseV1Event(buf)
	} else {
		return &event, fmt.Errorf("received unsupported event version (major version: %d, minor version: %d)", major, minor)
	}
	return &event, nil
}

func parseV2Event(buf []byte) *pb.Event_V2 {
	entryID, ParentEntryID, path, targetPath, targetParentID, nextOffset := parseCStrings(buf)
	eventData := &pb.Event_V2{
		V2: &pb.V2Event{
			NumLinks:       binary.LittleEndian.Uint64(buf[16:24]),
			Type:           pb.V2Event_Type(binary.LittleEndian.Uint32(buf[24:28])),
			EntryId:        entryID,
			ParentEntryId:  ParentEntryID,
			Path:           path,
			TargetPath:     targetPath,
			TargetParentId: targetParentID,
			MsgUserId:      binary.LittleEndian.Uint32(buf[nextOffset : nextOffset+4]),
			Timestamp:      int64(binary.LittleEndian.Uint64(buf[nextOffset+4 : nextOffset+12])),
			// The end of the uint64 is 8 bytes plus 4 bytes for the previous uint32. Note the
			// casting from uint64 to int64 is intentional, Go will handle the interpretation
			// correctly if the original value was supposed to represent a negative number.
		},
	}
	return eventData
}

func parseV1Event(buf []byte) *pb.Event_V1 {
	entryID, ParentEntryID, path, targetPath, targetParentID, _ := parseCStrings(buf)
	eventData := &pb.Event_V1{
		V1: &pb.V1Event{
			MissedSeq:      binary.LittleEndian.Uint64(buf[16:24]),
			Type:           pb.V1Event_Type(binary.LittleEndian.Uint32(buf[24:28])),
			EntryId:        entryID,
			ParentEntryId:  ParentEntryID,
			Path:           path,
			TargetPath:     targetPath,
			TargetParentId: targetParentID,
		},
	}
	return eventData
}

const (
	// eventPacketHeaderSize is the location at which data specific to this event type is encoded.
	eventPacketHeaderSize = 28
	uint32Size            = 4
)

// parseCStrings is used to parse several strings encoded as c-strings from both V1 and V2 event
// buffers. Each string starts with a uint32 value indicating the length of the string to follow.
// Each string is terminated by a NULL character (\x00). The starting offset of these strings inside
// the buffer is the same for V1 and V2. However V2 also needs to known the offset of the next byte
// to continue parsing the remaining fields which is returned as nextOffset.
func parseCStrings(buf []byte) (entryID, parentEntryID, path, targetPath, targetParentID string, nextOffset uint32) {
	nextOffset = uint32(eventPacketHeaderSize)
	entryID = parseCString(buf, &nextOffset)
	parentEntryID = parseCString(buf, &nextOffset)
	path = parseCString(buf, &nextOffset)
	// While only some event types like rename have a targetPath/targetParentID, we still need to
	// parse the remaining sizes so we can set nextOffset correctly.
	targetPath = parseCString(buf, &nextOffset)
	targetParentID = parseCString(buf, &nextOffset)
	return
}

func parseCString(buf []byte, offset *uint32) string {
	strLength := binary.LittleEndian.Uint32(buf[*offset : *offset+uint32Size])
	*offset += uint32Size
	str := string(buf[*offset : *offset+strLength])
	// The length doesn't include the null terminator so we need to move the offset to the location
	// where any remaining data in the buffer is expected.
	*offset += strLength + 1
	return str
}
