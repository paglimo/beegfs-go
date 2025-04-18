package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
}

var _ Provider = &JobBuilderClient{}

func NewJobBuilderClient(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider) *JobBuilderClient {
	return &JobBuilderClient{
		ctx:        ctx,
		rstMap:     rstMap,
		mountPoint: mountPoint,
	}
}

// GenerateJobRequest is not implemented and should never be called.
func (r *JobBuilderClient) GenerateJobRequest(inMountPath string, cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return nil
}

// GenerateWorkRequests for JobBuilderClient should simply pass a single
func (r *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, canRetry bool, err error) {
	workRequests := RecreateWorkRequests(job, nil)
	return workRequests, true, nil
}

func (r *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	defer close(jobSubmissionChan)

	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	walkChanSize := len(jobSubmissionChan)
	var walkChan <-chan *WalkResponse
	var client Provider
	var ok bool
	var err error
	if cfg.Download {
		if !isValidRstId(cfg.RemoteStorageTarget) {

			// TODO: Walk local path to see if the files exist.
			//  - If the file exists, get for rst ids and, if there's only one, use it.

			return fmt.Errorf("download without Cfg.RemoteStorageTarget is not implemented")
		}

		if client, ok = r.rstMap[cfg.RemoteStorageTarget]; !ok {
			return fmt.Errorf("failed to determine rst client")
		}

		if walkChan, err = client.GetWalk(ctx, cfg.RemotePath, walkChanSize); err != nil {
			return err
		}
	} else {
		if walkChan, err = WalkLocalDirectory(ctx, r.mountPoint, workRequest.Path, walkChanSize); err != nil {
			return err
		}
	}

	return r.executeJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan)
}

// PrepareExecuteWorkRequests is not implemented and should never be called.
func (r *JobBuilderClient) PrepareExecuteWorkRequests(ctx context.Context, request *flex.WorkRequest) (canRetry bool, err error) {
	return true, ErrUnsupportedOpForRST
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (r *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

// ConcludeExecuteWorkRequests is not implemented and should never be called.
func (r *JobBuilderClient) ConcludeExecuteWorkRequests(ctx context.Context, request *flex.WorkRequest, workResults []*flex.Work, abort bool) (canRetry bool, err error) {
	return true, ErrUnsupportedOpForRST
}

func (r *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return nil
}

// GetConfig is not implemented and should never be called.
func (r *JobBuilderClient) GetConfig() *flex.RemoteStorageTarget {
	return nil
}

// GetWalk is not implemented and should never be called.
func (r *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int) (<-chan *WalkResponse, error) {
	return nil, ErrUnsupportedOpForRST
}

// SanitizeRemotePath should never be called.
func (r *JobBuilderClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

// GetRemoteInfo is not implemented and should never be called.
func (r *JobBuilderClient) GetRemoteInfo(ctx context.Context, remotePath string, keyMustExist bool) (remoteSize int64, remoteMtime time.Time, err error) {
	return 0, time.Time{}, ErrUnsupportedOpForRST
}

func (r *JobBuilderClient) executeJobBuilderRequest(ctx context.Context, request *flex.WorkRequest, walkChan <-chan *WalkResponse, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	builder := request.GetBuilder()
	cfg := builder.GetCfg()

	store, err := config.NodeStore(ctx)
	if err != nil {
		return fmt.Errorf("upload successful but failed to create stub file: %w", err)
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		return fmt.Errorf("upload successful but failed to create stub file: %w", err)
	}

	var inMountPath string
	var remotePath string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case walkResp, ok := <-walkChan:
			if !ok {
				return nil
			}
			if walkResp.FatalErr {
				return walkResp.Err
			}

			if cfg.Download {
				remotePath = walkResp.Path
				if cfg.RemotePath == "" {
					inMountPath = MapRemoteToLocalPath("/", remotePath, cfg.Flatten)
				} else {
					inMountPath = MapRemoteToLocalPath(request.Path, remotePath, cfg.Flatten)
				}
				if err := r.mountPoint.CreateDir(filepath.Dir(inMountPath)); err != nil {
					return err
				}
			} else {
				inMountPath = walkResp.Path
				remotePath = inMountPath
			}
		}

		var client Provider
		if isValidRstId(cfg.RemoteStorageTarget) {
			client = r.rstMap[cfg.RemoteStorageTarget]
		}

		jobRequests, err := BuildJobRequest(ctx, client, r.rstMap, r.mountPoint, store, mappings, inMountPath, remotePath, cfg)
		if err != nil {
			continue // TODO: What should we do here? Create a failed job request so the status or job list can represent the failure? Yes
		}

		for _, jobRequest := range jobRequests {
			jobSubmissionChan <- jobRequest
		}
	}
}

func isValidRstId(rstId uint32) bool {
	return rstId != 0
}
