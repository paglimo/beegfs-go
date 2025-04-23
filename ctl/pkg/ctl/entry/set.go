package entry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

// Equivalent of the original MODESETPATTERN args. Fields that are nil are unmodified.
//
// IMPORTANT: When updating this struct, add any fields that can only be modified by root to the
// checks in Validate().
type SetEntryCfg struct {
	// actorUID must be initialized by calling SetEntryCfg.setAndValidateEUID().
	actorEUID *uint32
	// Allow bypassing some configuration checks.
	Force bool
	// Entry metadata updates:
	Chunksize          *uint32
	Pool               *beegfs.EntityId
	DefaultNumTargets  *uint32
	StripePattern      *beegfs.StripePatternType
	RemoteTargets      []uint32
	RemoteCooldownSecs *uint16
	AccessFlags        *beegfs.AccessFlags
	DataState          *beegfs.DataState
}

// Validates the effective UID has permissions to make the requested updates.
func (config *SetEntryCfg) setAndValidateEUID() error {

	euid := syscall.Geteuid()
	if euid < 0 || euid > math.MaxUint32 {
		return fmt.Errorf("effective user ID %d is out of bounds (not a uint32)", euid)
	}
	u32euid := uint32(euid)
	config.actorEUID = &u32euid
	// Checking user permissions when the chunksize or pattern changes is left up to the
	// metadata server which may allow users to update their own directories if
	// sysAllowUserSetPattern=true. All other configuration updates are rejected by the meta
	// server, but currently may be silently discarded which may confuse users about why changes
	// were not applied. Note if sysAllowUserSetPattern=false all updates by a non-root user are
	// rejected with a meaningful error, however we have no way of knowing what the server's
	// sysAllowUserSetPattern option is set to, so we always also check permissions locally.
	if *config.actorEUID != 0 {
		if config.Pool != nil {
			return fmt.Errorf("only root can configure pools")
		}
		if config.StripePattern != nil {
			return fmt.Errorf("only root can configure stripe patterns")
		}
		if len(config.RemoteTargets) > 0 {
			return fmt.Errorf("only root can configure remote targets")
		}
		if config.RemoteCooldownSecs != nil {
			return fmt.Errorf("only root can configure the remote cooldown")
		}
		if config.AccessFlags != nil {
			return fmt.Errorf("only root can configure access flags")
		}
		if config.DataState != nil {
			return fmt.Errorf("only root can configure data state")
		}
	}
	return nil
}

type SetEntryResult struct {
	Path    string
	Status  beegfs.OpsErr
	Updates SetEntryCfg
}

func SetEntries(ctx context.Context, pm util.PathInputMethod, cfg SetEntryCfg) (<-chan SetEntryResult, <-chan error, error) {
	log, _ := config.GetLogger()

	// Validate new configuration once:
	if err := cfg.setAndValidateEUID(); err != nil {
		return nil, nil, err
	}

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, nil, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
		log.Debug("remote storage mappings are not available (ignoring)", zap.Any("error", err))
	}

	processEntry := func(path string) (SetEntryResult, error) {
		return setEntry(ctx, mappings, cfg, path)
	}

	return util.ProcessPaths(ctx, pm, false, processEntry)
}

// setEntry applies the SetEntryRequest to the specified searchPath. WARNING: This function is meant
// to be called through SetEntries() and is not safe to call directly as it relies on SetEntries()
// for some config validation to avoid duplicate checking of the request for each entry.
func setEntry(ctx context.Context, mappings *util.Mappings, cfg SetEntryCfg, path string) (SetEntryResult, error) {
	entry, err := GetEntry(ctx, mappings, GetEntriesCfg{
		Verbose:        false,
		IncludeOrigMsg: true,
	}, path)
	if err != nil {
		return SetEntryResult{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return SetEntryResult{}, err
	}

	// Handle file state updates separately from other updates.
	if cfg.AccessFlags != nil || cfg.DataState != nil {
		// Return error if the entry is NOT a regular file.
		if entry.Entry.Type != beegfs.EntryRegularFile {
			return SetEntryResult{
				Path:    path,
				Status:  beegfs.OpsErr_NOTSUPP,
				Updates: SetEntryCfg{},
			}, nil
		}
		return handleFileStateUpdate(ctx, store, entry, cfg, path)
	}

	if entry.Entry.Type == beegfs.EntryDirectory {
		return handleDirectory(ctx, mappings, store, entry, cfg, path)
	}
	return handleFile(ctx, store, entry, cfg, path)
}
func handleDirectory(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, entry *GetEntryCombinedInfo, cfg SetEntryCfg, path string) (SetEntryResult, error) {
	// Start with the current settings for this entry:
	request := &msg.SetDirPatternRequest{
		EntryInfo: *entry.Entry.origEntryInfoMsg,
		Pattern:   entry.Entry.Pattern.StripePattern,
		RST:       entry.Entry.Remote.RemoteStorageTarget,
	}
	request.SetUID(*cfg.actorEUID)

	// Only update any settings defined in newCfg:
	if cfg.Chunksize != nil {
		request.Pattern.Chunksize = *cfg.Chunksize
	}
	if cfg.DefaultNumTargets != nil {
		request.Pattern.DefaultNumTargets = *cfg.DefaultNumTargets
	}
	// Important to check if the pool was updated before determining the stripe pattern
	// configuration because eligible patterns are determined based on the pool configuration.
	if cfg.Pool != nil {
		pool, err := mappings.StoragePoolToConfig.Get(*cfg.Pool)
		if err != nil {
			return SetEntryResult{}, fmt.Errorf("unable to retrieve the specified storage pool %v: %w", *cfg.Pool, err)
		}
		// We don't need to check both the targets and buddy groups because targets should always be
		// assigned to the same pool as their buddy groups.
		if !cfg.Force && len(pool.Targets) == 0 {
			return SetEntryResult{}, fmt.Errorf("storage pool with ID %d does not contain any targets (use force to override)", pool.Pool.LegacyId.NumId)
		}
		request.Pattern.StoragePoolID = uint16(pool.Pool.LegacyId.NumId)
	}
	if cfg.StripePattern != nil {
		if !cfg.Force {
			// To avoid later errors creating files in this directory, ensure the assigned pool is
			// eligible for use with the requested stripe pattern.
			var poolID beegfs.EntityId
			if cfg.Pool != nil {
				poolID = *cfg.Pool
			} else {
				poolID = beegfs.LegacyId{
					NumId:    beegfs.NumId(entry.Entry.Pattern.StoragePoolID),
					NodeType: beegfs.Storage,
				}
			}
			if pool, err := mappings.StoragePoolToConfig.Get(poolID); err == nil {
				if *cfg.StripePattern == beegfs.StripePatternBuddyMirror && len(pool.BuddyGroups) == 0 {
					return SetEntryResult{}, fmt.Errorf("refusing to set stripe pattern %s on path %s because its assigned storage pool %s does not contain any buddy groups (use force to override)", cfg.StripePattern, path, pool.Pool.String())
				} else if *cfg.StripePattern == beegfs.StripePatternRaid0 && len(pool.Targets) == 0 {
					return SetEntryResult{}, fmt.Errorf("refusing to set stripe pattern %s on path %s because its assigned storage pool %s does not contain any targets (use force to override)", cfg.StripePattern, path, pool.Pool.String())
				}
				// Otherwise allow the pattern to be updated.
			} else {
				return SetEntryResult{}, fmt.Errorf("error looking up pool with ID %s for path %s: %w", poolID, path, err)
			}
		}
		request.Pattern.Type = *cfg.StripePattern
	}
	if cfg.RemoteTargets != nil {
		request.RST.RSTIDs = cfg.RemoteTargets
	}
	if cfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *cfg.RemoteCooldownSecs
	}

	var resp = &msg.SetDirPatternResponse{}
	err := store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return SetEntryResult{}, err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS && resp.Result != beegfs.OpsErr_NOTADIR {
		return SetEntryResult{}, fmt.Errorf("server returned an error performing the requested updates for path %s: %w", path, resp.Result)
	}

	return SetEntryResult{
		Path:    path,
		Status:  resp.Result,
		Updates: cfg,
	}, nil
}

// Handles regular files and all non-directory types (e.g., symlinks).
func handleFile(ctx context.Context, store *beemsg.NodeStore, entry *GetEntryCombinedInfo, cfg SetEntryCfg, searchPath string) (SetEntryResult, error) {

	// Start with the current settings for this entry
	request := &msg.SetFilePatternRequest{
		EntryInfo: *entry.Entry.origEntryInfoMsg,
		RST:       entry.Entry.Remote.RemoteStorageTarget,
	}

	// Determine whether any RST related fields are provided in newCfg
	isRSTConfigSpecified := false
	if cfg.RemoteTargets != nil {
		request.RST.RSTIDs = cfg.RemoteTargets
		isRSTConfigSpecified = true
	}
	if cfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *cfg.RemoteCooldownSecs
		isRSTConfigSpecified = true
	}
	// IMPORTANT: If new configuration is added here also add it to the Updates returned in the
	// results below. Since not all configuration can be updated for files, we only return
	// configuration updates that are allowed.

	// Return early if no RST configuration is specified, because
	// at present only RST updates are allowed for regular files.
	if !isRSTConfigSpecified {
		return SetEntryResult{
			Path:    searchPath,
			Status:  beegfs.OpsErr_NOTSUPP,
			Updates: cfg,
		}, nil
	}

	// Send the request and handle the response
	var resp = &msg.SetFilePatternResponse{}
	err := store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return SetEntryResult{}, err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS {
		return SetEntryResult{}, fmt.Errorf("server returned an error performing the requested updates on path %s: %w", searchPath, resp.Result)
	}

	return SetEntryResult{
		Path:   searchPath,
		Status: resp.Result,
		Updates: SetEntryCfg{
			// Only return configuration that is allowed to be updated in the results:
			RemoteTargets:      cfg.RemoteTargets,
			RemoteCooldownSecs: cfg.RemoteCooldownSecs,
		},
	}, nil
}

func handleFileStateUpdate(ctx context.Context, store *beemsg.NodeStore, entry *GetEntryCombinedInfo, cfg SetEntryCfg, searchPath string) (SetEntryResult, error) {
	// Get current file state from entry
	currentFileState := entry.Entry.FileState

	// Start with current access flags and data state
	newAccessFlags := currentFileState.GetAccessFlags()
	newDataState := currentFileState.GetDataState()

	// Update access flags and data state if specified by the user
	if cfg.AccessFlags != nil {
		newAccessFlags = *cfg.AccessFlags
	}
	if cfg.DataState != nil {
		newDataState = *cfg.DataState
	}

	// Return early if no changes are required
	if newAccessFlags == currentFileState.GetAccessFlags() && newDataState == currentFileState.GetDataState() {
		return SetEntryResult{
			Path:    searchPath,
			Status:  beegfs.OpsErr_SUCCESS,
			Updates: SetEntryCfg{},
		}, nil
	}

	// Create new file state by combining the access flags and data state
	newFileState := beegfs.NewFileState(newAccessFlags, newDataState)
	request := &msg.SetFileStateRequest{
		EntryInfo: *entry.Entry.origEntryInfoMsg,
		FileState: newFileState,
	}

	// send the request and handle the response
	var resp = &msg.SetFileStateResponse{}
	err := store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return SetEntryResult{}, err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS {
		return SetEntryResult{}, fmt.Errorf("server returned an error performing the requested updates on path %s: %w", searchPath, resp.Result)
	}

	return SetEntryResult{
		Path:   searchPath,
		Status: resp.Result,
		Updates: SetEntryCfg{
			AccessFlags: cfg.AccessFlags,
			DataState:   cfg.DataState,
		},
	}, nil
}
