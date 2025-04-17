package msg

import (
	"encoding/binary"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// MkDirRequest (MkDirMsg) is currently only used for creating files using CTL. That use case does
// not need to trigger a file system modification event, and does not set the Flag_BuddyMirrorSecond
// flag. If this message needs to be used for other purposes serialization of those fields may need
// to be implemented.
type MkDirRequest struct {
	UserID         uint32
	GroupID        uint32
	Mode           int32
	Umask          int32
	ParentInfo     EntryInfo
	NewDirName     []byte
	PreferredNodes []uint16
	NoMirror       bool
	// Not implemented:
	// FileEvent        struct{}
	// CreatedEntryInfo struct{}
	// ParentTimestamps struct{}
}

func (m *MkDirRequest) MsgId() uint16 {
	return 2001
}
func (m *MkDirRequest) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.UserID)
	beeserde.SerializeInt(s, m.GroupID)
	beeserde.SerializeInt(s, m.Mode)
	beeserde.SerializeInt(s, m.Umask)
	m.ParentInfo.Serialize(s)
	beeserde.SerializeCStr(s, m.NewDirName, 4)
	beeserde.SerializeSeq(s, m.PreferredNodes, true, func(out uint16) {
		beeserde.SerializeInt(s, out)
	})
	if m.NoMirror {
		// Equivalent of MKDIRMSG_FLAG_NOMIRROR in C++
		s.MsgFeatureFlags |= 1
	}
}

type MkDirResp struct {
	Result    beegfs.OpsErr
	EntryInfo EntryInfo
}

func (m *MkDirResp) MsgId() uint16 {
	return 2002
}

func (m *MkDirResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
	m.EntryInfo.Deserialize(d)
}

// UnlinkFileRequest is currently only used for cleaning up disposal files. That use case does not
// need to trigger a file system modification event, and does not set the Flag_BuddyMirrorSecond
// flag. If this message needs to be used for other purposes serialization of those fields may need
// to be implemented.
type UnlinkFileRequest struct {
	ParentInfo  EntryInfo
	DelFileName []byte
	// Not implemented:
	// FileEvent      struct{}
	// DirTimestamps  struct{}
	// FileInfo       EntryInfo
	// FileTimestamps struct{}
}

func (m *UnlinkFileRequest) MsgId() uint16 {
	return 2007
}

func (m *UnlinkFileRequest) Serialize(s *beeserde.Serializer) {
	m.ParentInfo.Serialize(s)
	beeserde.SerializeCStr(s, m.DelFileName, 4)
}

type UnlinkFileResponse struct {
	Result beegfs.OpsErr
}

func (m *UnlinkFileResponse) MsgId() uint16 {
	return 2008
}

func (m *UnlinkFileResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
}

type ListDirFromOffsetRequest struct {
	ServerOffset int64
	MaxOutNames  uint32
	EntryInfo    EntryInfo
	FilterDots   bool
}

func (m *ListDirFromOffsetRequest) MsgId() uint16 {
	return 2029
}

func (m *ListDirFromOffsetRequest) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.ServerOffset)
	beeserde.SerializeInt(s, m.MaxOutNames)
	m.EntryInfo.Serialize(s)
	beeserde.SerializeInt(s, m.FilterDots)
}

type ListDirFromOffsetResponse struct {
	NewServerOffset int64
	ServerOffsets   []int64
	Result          int32
	EntryTypes      []uint8
	EntryIDs        [][]byte
	Names           [][]byte
}

func (m *ListDirFromOffsetResponse) MsgId() uint16 {
	return 2030
}

func (m *ListDirFromOffsetResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.NewServerOffset)
	beeserde.DeserializeSeq(d, &m.ServerOffsets, true, func(out *int64) {
		beeserde.DeserializeInt(d, out)
	})
	beeserde.DeserializeInt(d, &m.Result)
	beeserde.DeserializeSeq(d, &m.EntryTypes, true, func(out *uint8) {
		beeserde.DeserializeInt(d, out)
	})
	beeserde.DeserializeStringSeq(d, &m.EntryIDs)
	beeserde.DeserializeStringSeq(d, &m.Names)
	typesLen, entryIDsLen, namesLen := len(m.EntryTypes), len(m.EntryIDs), len(m.Names)
	if typesLen != entryIDsLen && entryIDsLen != namesLen {
		d.Fail(fmt.Errorf("sanity check failed, number of elements in sequences do not match (num types: %d, num entries: %d, num names: %d)", typesLen, entryIDsLen, namesLen))
	}
}

type FindOwnerRequest struct {
	SearchDepth  uint32
	CurrentDepth uint32
	EntryInfo    EntryInfo
	Path         Path
}

func (m *FindOwnerRequest) MsgId() uint16 {
	return 2035
}

func (m *FindOwnerRequest) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.SearchDepth)
	beeserde.SerializeInt(s, m.CurrentDepth)
	m.EntryInfo.Serialize(s)
	m.Path.Serialize(s)
}

type FindOwnerResponse struct {
	Result             beegfs.OpsErr
	EntryInfoWithDepth EntryInfoWithDepth
}

func (m *FindOwnerResponse) MsgId() uint16 {
	return 2036
}

func (m *FindOwnerResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
	m.EntryInfoWithDepth.Deserialize(d)
}

type EntryInfo struct {
	// The equivalent of OwnerNodeID in C++. This is either the ID of the metadata node that owns
	// this entry or the ID of the buddy mirror group if the metadata for this entry is mirrored.
	// The field name was changed here to avoid confusion/misuse.
	OwnerID       uint32
	ParentEntryID []byte
	EntryID       []byte
	FileName      []byte
	EntryType     beegfs.EntryType
	FeatureFlags  beegfs.EntryFeatureFlags
}

func (m *EntryInfo) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.EntryType)
	beeserde.SerializeInt(s, m.FeatureFlags)
	beeserde.SerializeCStr(s, m.ParentEntryID, 4)
	beeserde.SerializeCStr(s, m.EntryID, 4)
	beeserde.SerializeCStr(s, m.FileName, 4)
	beeserde.SerializeInt(s, m.OwnerID)
	beeserde.Zeroes(s, 2)
}

func (m *EntryInfo) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.EntryType)
	beeserde.DeserializeInt(d, &m.FeatureFlags)
	beeserde.DeserializeCStr(d, &m.ParentEntryID, 4)
	beeserde.DeserializeCStr(d, &m.EntryID, 4)
	beeserde.DeserializeCStr(d, &m.FileName, 4)
	beeserde.DeserializeInt(d, &m.OwnerID)
	beeserde.Skip(d, 2)
}

type EntryInfoWithDepth struct {
	EntryInfo  EntryInfo
	EntryDepth uint32
}

func (m *EntryInfoWithDepth) Deserialize(d *beeserde.Deserializer) {
	m.EntryInfo.Deserialize(d)
	beeserde.DeserializeInt(d, &m.EntryDepth)
}

type Path struct {
	PathStr []byte
	// Note Serialization doesn't include DirSeparators.
}

func (m *Path) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeCStr(s, m.PathStr, 0)
}

func (m *Path) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeCStr(d, &m.PathStr, 0)
}

type GetEntryInfoRequest struct {
	EntryInfo EntryInfo
}

func (m *GetEntryInfoRequest) MsgId() uint16 {
	return 2045
}

func (m *GetEntryInfoRequest) Serialize(s *beeserde.Serializer) {
	m.EntryInfo.Serialize(s)
}

type GetEntryInfoResponse struct {
	Result  int32
	Pattern StripePattern
	Path    PathInfo
	RST     RemoteStorageTarget
	// Metadata mirror node (0 means none). Note this is no longer in use and thus always set to 0.
	MirrorNodeID     uint16
	NumSessionsRead  uint32
	NumSessionsWrite uint32
	FileState        beegfs.FileState
}

func (m *GetEntryInfoResponse) MsgId() uint16 {
	return 2046
}

func (m *GetEntryInfoResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
	m.Pattern.Deserialize(d)
	m.Path.Deserialize(d)
	m.RST.Deserialize(d)
	beeserde.DeserializeInt(d, &m.MirrorNodeID)
	beeserde.DeserializeInt(d, &m.NumSessionsRead)
	beeserde.DeserializeInt(d, &m.NumSessionsWrite)
	beeserde.DeserializeInt(d, &m.FileState)
}

// The Go equivalent of a BeeGFS StripePattern. Deserializing stripe patterns in the C++ code is
// fairly complicated. The StripePattern has a deserialize() method that first deserializes the
// StripePatternHeader using its serialize method, then uses this to determine the actual pattern
// type, and initialize either a Raid0Pattern, Raid10Pattern, or BuddyMirrorPattern. Each of these
// extend the StripePattern class to implement the semantics of a specific striping pattern. Here we
// aren't interested in recreating all the logic associated with different stripe patterns, so we
// just deserialize into a common StripePattern struct.
type StripePattern struct {
	Length            uint32
	Type              beegfs.StripePatternType
	HasPoolID         bool
	Chunksize         uint32
	StoragePoolID     uint16
	DefaultNumTargets uint32
	// For StripePatternType=RAID0 this is the equivalent of stripeTargetIDs. For
	// StripePatternType=BuddyMirror, this is mirrorBuddyGroupIDs. While the C++ classes internally
	// distinguish between stripe targets and buddy group IDs, this distinction is less important
	// externally because they are usually accessed using a common getStripeTargetIDs getter anyway.
	// This is why there aren't separate fields depending if the type is RAID0 or buddy mirror.
	// The only time this appears to matter is for the now defunct RAID10 stripe pattern type.
	TargetIDs []uint16
}

// Equivalent of HasNoPoolFlag in C++.
const hasNoPoolFlag uint32 = 1 << 24

func (m *StripePattern) Serialize(s *beeserde.Serializer) {
	// Length is populated at the end once we know the size of the message.
	lengthPos := s.Buf.Len()
	beeserde.SerializeInt(s, int32(0))
	// This flag is for compatibility with really old versions of BeeGFS (e.g., 2014). If we
	// find a stripe pattern with the no pool flag set, most likely something went wrong or
	// someone is trying to use CTL with an unsupported version of BeeGFS.
	if !m.HasPoolID {
		s.Fail(fmt.Errorf("unsupported message (has no storage pool ID)"))
	}
	beeserde.SerializeInt(s, m.Type)
	beeserde.SerializeInt(s, m.Chunksize)
	beeserde.SerializeInt(s, m.StoragePoolID)
	beeserde.SerializeInt(s, m.DefaultNumTargets)
	switch m.Type {
	case beegfs.StripePatternRaid0, beegfs.StripePatternBuddyMirror:
		beeserde.SerializeSeq(s, m.TargetIDs, true, func(out uint16) {
			beeserde.SerializeInt(s, out)
		})
	case beegfs.StripePatternRaid10:
		s.Fail(fmt.Errorf("unsupported stripe pattern found: %s", beegfs.StripePatternRaid10))
	default:
		s.Fail(fmt.Errorf("unknown stripe pattern: %s", m.Type))
	}
	// Now populate length:
	finalLength := s.Buf.Len()
	binary.LittleEndian.PutUint32(s.Buf.Bytes()[lengthPos:], uint32(finalLength))
}

func (m *StripePattern) Deserialize(d *beeserde.Deserializer) {

	// First deserialize the StripePatternHeader:
	beeserde.DeserializeInt(d, &m.Length)
	var typeWithFlags uint32
	beeserde.DeserializeInt(d, &typeWithFlags)
	m.HasPoolID = (typeWithFlags & hasNoPoolFlag) == 0
	if !m.HasPoolID {
		// This flag is for compatibility with really old versions of BeeGFS (e.g., 2014). If we
		// find a stripe pattern with the no pool flag set, most likely something went wrong or
		// someone is trying to use CTL with an unsupported version of BeeGFS.
		d.Fail(fmt.Errorf("unsupported message (has no storage pool ID)"))
	}
	m.Type = beegfs.StripePatternType((typeWithFlags & ^hasNoPoolFlag))
	beeserde.DeserializeInt(d, &m.Chunksize)
	if m.HasPoolID {
		beeserde.DeserializeInt(d, &m.StoragePoolID)
	}

	// Then deserialize the actual pattern.
	beeserde.DeserializeInt(d, &m.DefaultNumTargets)
	switch m.Type {
	case beegfs.StripePatternRaid0, beegfs.StripePatternBuddyMirror:
		beeserde.DeserializeSeq(d, &m.TargetIDs, true, func(out *uint16) {
			beeserde.DeserializeInt(d, out)
		})
	case beegfs.StripePatternRaid10:
		// RAID10 is deprecated and CTL prevents setting RAID10 as the pattern type. If there is a
		// message with this pattern type we should fail because it is likely something went wrong,
		// or someone is trying to use CTL with an unsupported version of BeeGFS.
		d.Fail(fmt.Errorf("unsupported stripe pattern found: %s", beegfs.StripePatternRaid10))
	default:
		d.Fail(fmt.Errorf("unknown stripe pattern: %d", m.Type))
	}
}

type PathInfo struct {
	Flags             int32
	OrigParentUID     uint32
	OrigParentEntryID []byte
}

const (
	// Equivalent of PATHINFO_FEATURE_ORIG in C++.
	pathInfoFeatureOriginal = 1
	// Equivalent of PATHINFO_FEATURE_ORIG_UNKNOWN in C++ (unused at present).
	// pathInfoFeatureOrigUnknown = 2
)

func (m *PathInfo) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Flags)
	if (m.Flags & pathInfoFeatureOriginal) == 1 {
		beeserde.DeserializeInt(d, &m.OrigParentUID)
		beeserde.DeserializeCStr(d, &m.OrigParentEntryID, 4)
	}
}

type RemoteStorageTarget struct {
	majorVersion   uint8
	minorVersion   uint8
	CoolDownPeriod uint16
	Reserved       uint16
	FilePolicies   uint16
	RSTIDs         []uint32
}

func (m *RemoteStorageTarget) Serialize(s *beeserde.Serializer) {
	if m.majorVersion == 0 && m.minorVersion == 0 {
		if m.CoolDownPeriod != 0 || m.Reserved != 0 || m.FilePolicies != 0 || len(m.RSTIDs) > 0 {
			// Handle setting the initial major and minor versions
			m.majorVersion = 1
			m.minorVersion = 0
		}
	}
	beeserde.SerializeInt(s, m.majorVersion)
	beeserde.SerializeInt(s, m.minorVersion)
	beeserde.SerializeInt(s, m.CoolDownPeriod)
	beeserde.SerializeInt(s, m.Reserved)
	beeserde.SerializeInt(s, m.FilePolicies)
	beeserde.SerializeSeq(s, m.RSTIDs, true, func(out uint32) {
		beeserde.SerializeInt(s, out)
	})
}

func (m *RemoteStorageTarget) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.majorVersion)
	beeserde.DeserializeInt(d, &m.minorVersion)
	if m.majorVersion > 1 || m.minorVersion != 0 {
		d.Fail(fmt.Errorf("unsupported RST format (major: %d, minor: %d)", m.majorVersion, m.minorVersion))
	}
	beeserde.DeserializeInt(d, &m.CoolDownPeriod)
	beeserde.DeserializeInt(d, &m.Reserved)
	beeserde.DeserializeInt(d, &m.FilePolicies)
	beeserde.DeserializeSeq(d, &m.RSTIDs, true, func(out *uint32) {
		beeserde.DeserializeInt(d, out)
	})
}

type SetDirPatternRequest struct {
	EntryInfo EntryInfo
	Pattern   StripePattern
	RST       RemoteStorageTarget
	// Uid is an internal field to force callers to use the SetUID method so the hasUid flag is
	// always set correctly. This is required for serialization to work correctly.
	uid uint32
	// Equivalent of the HAS_UID flag from the original SetDirPatternMsg.
	hasUid bool
}

func (m *SetDirPatternRequest) MsgId() uint16 {
	return 2047
}

func (m *SetDirPatternRequest) Serialize(s *beeserde.Serializer) {
	m.EntryInfo.Serialize(s)
	m.Pattern.Serialize(s)
	m.RST.Serialize(s)
	if m.hasUid {
		beeserde.SerializeInt(s, m.uid)
		s.MsgFeatureFlags = 1
	}
}

func (m *SetDirPatternRequest) Deserialize(d *beeserde.Deserializer) {
	m.EntryInfo.Deserialize(d)
	m.Pattern.Deserialize(d)
	m.RST.Deserialize(d)
	if d.MsgFeatureFlags&1 != 0 {
		beeserde.DeserializeInt(d, &m.uid)
		m.hasUid = true
	}
}

func (m *SetDirPatternRequest) SetUID(uid uint32) {
	m.hasUid = true
	m.uid = uid
}

func (m *SetDirPatternRequest) GetUID() *uint32 {
	if m.hasUid {
		return &m.uid
	}
	return nil
}

type SetDirPatternResponse struct {
	Result beegfs.OpsErr
}

func (m *SetDirPatternResponse) MsgId() uint16 {
	return 2048
}

func (m *SetDirPatternResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
}

type MakeFileWithPatternRequest struct {
	UserID      uint32
	GroupID     uint32
	Mode        int32
	Umask       int32
	ParentInfo  EntryInfo
	NewFileName []byte
	Pattern     StripePattern
	RST         RemoteStorageTarget
}

func (m *MakeFileWithPatternRequest) MsgId() uint16 {
	return 2053
}

func (m *MakeFileWithPatternRequest) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.UserID)
	beeserde.SerializeInt(s, m.GroupID)
	beeserde.SerializeInt(s, m.Mode)
	beeserde.SerializeInt(s, m.Umask)
	m.ParentInfo.Serialize(s)
	beeserde.SerializeCStr(s, m.NewFileName, 4)
	m.Pattern.Serialize(s)
	m.RST.Serialize(s)
}

type MakeFileWithPatternResponse struct {
	Result    beegfs.OpsErr
	EntryInfo EntryInfo
}

func (m *MakeFileWithPatternResponse) MsgId() uint16 {
	return 2054
}

func (m *MakeFileWithPatternResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
	m.EntryInfo.Deserialize(d)
}

type SetFilePatternRequest struct {
	EntryInfo EntryInfo
	RST       RemoteStorageTarget
}

func (m *SetFilePatternRequest) MsgId() uint16 {
	return 2123
}

func (m *SetFilePatternRequest) Serialize(s *beeserde.Serializer) {
	m.EntryInfo.Serialize(s)
	m.RST.Serialize(s)
}

type SetFilePatternResponse struct {
	Result beegfs.OpsErr
}

func (m *SetFilePatternResponse) MsgId() uint16 {
	return 2124
}

func (m *SetFilePatternResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
}

type SetFileStateRequest struct {
	EntryInfo EntryInfo
	FileState beegfs.FileState
}

func (m *SetFileStateRequest) MsgId() uint16 {
	return 2131
}

func (m *SetFileStateRequest) Serialize(s *beeserde.Serializer) {
	m.EntryInfo.Serialize(s)
	beeserde.SerializeInt(s, m.FileState)
}

type SetFileStateResponse struct {
	Result beegfs.OpsErr
}

func (m *SetFileStateResponse) MsgId() uint16 {
	return 2132
}

func (m *SetFileStateResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
}

type RefreshEntryInfoRequest struct {
	EntryInfo EntryInfo
}

func (r *RefreshEntryInfoRequest) MsgId() uint16 {
	return 2055
}

func (r *RefreshEntryInfoRequest) Serialize(s *beeserde.Serializer) {
	r.EntryInfo.Serialize(s)
}

type RefreshEntryInfoResponse struct {
	Result beegfs.OpsErr
}

func (r *RefreshEntryInfoResponse) MsgId() uint16 {
	return 2056
}

func (r *RefreshEntryInfoResponse) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &r.Result)
}
