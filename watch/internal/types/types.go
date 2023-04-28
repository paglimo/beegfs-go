package types

import (
	"encoding/binary"
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
func (p *Packet) Deserialize(buf []byte) {

	// Integer values are easy to deserialize based on their sizes:
	p.FormatVersionMajor = binary.LittleEndian.Uint16(buf[0:2])
	p.FormatVersionMinor = binary.LittleEndian.Uint16(buf[2:4])
	p.Size = binary.LittleEndian.Uint32(buf[4:8])
	p.DroppedSeq = binary.LittleEndian.Uint64(buf[8:16])
	p.MissedSeq = binary.LittleEndian.Uint64(buf[16:24])
	p.Type = FileEventType(binary.LittleEndian.Uint32(buf[24:28]))

	// The rest of the packet is a null-terminated character array:
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
			// We know each string has four bytes of padding after it.
			// Skip to the expected start of the next string:
			start = end + 5
			end = start + 1
			// Check if we have additional strings to parse.
			// Notably TargetPath and TargetParentId are only included with some event types.
			if buf[start] == '\x00' {
				break // Terminate early so we don't loop over empty bytes.
			}
		}
	}

	// TODO: Better understand the structure of each message.
	// In addition to the strings we care about there are some extra
	// bytes send that are either empty or seem to include unicode control chars.
	// We get data in this order:
	// * Control character: Form Feed (ff)
	// * Empty Byte (x2)
	// * Entry ID
	// * Control character: End Of Transmission
	// * Empty Byte (x2)
	// * Parent Entry ID
	// * Control character: Line Tabulation
	// * Empty Byte (x2)
	// * Path
	// We'll then get (x11) empty bytes unless it was an op like RENAME then we get:
	// * Control character: Device Control Three
	// * Empty Byte (x2)
	// * Target Path
	// * Control character: Form Feed (ff)
	// * Target Parent ID
	// * Empty byte (x1)

	// Uncomment for debugging:
	// stringSlice := bytes.Split(buf[0:p.Size], []byte{0})
	// for _, s := range stringSlice {
	// 	fmt.Printf("%b = %s\n", s, s)
	// }
	// packet := buf[0 : p.Size+10]
	// fmt.Printf("%q\n", packet)
	// fmt.Printf("%#v\n\n", packet)
}
