package rst

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
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

// GetJobRequest is not implemented and should never be called.
func (c *JobBuilderClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: 0,
		StubLocal:           cfg.StubLocal,
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Builder{
			Builder: &flex.BuilderJob{
				Cfg: cfg,
			},
		},
	}
}

// GenerateWorkRequests for JobBuilderClient should simply pass a single
func (c *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, canRetry bool, err error) {
	workRequests := RecreateWorkRequests(job, nil)
	return workRequests, true, nil
}

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	defer close(jobSubmissionChan)
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()

	walkChanSize := len(jobSubmissionChan)
	var walkChan <-chan *WalkResponse
	var err error
	if cfg.Download {
		if !IsValidRstId(cfg.RemoteStorageTarget) && cfg.RemotePath == "" {
			// Since cfg.RemoteStorageTarget and remote path is not specified, walk the local path.
			// Jobs will be created for each file with a single rstId. Files with zero rstIds will
			// be ignored and those with multiple rstIds will fail since the source is ambiguous.
			if walkChan, err = walkPath(ctx, c.mountPoint, workRequest.Path, walkChanSize); err != nil {
				return err
			}
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				return fmt.Errorf("failed to determine rst client")
			}
			if walkChan, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.RemotePath), walkChanSize); err != nil {
				return err
			}
		}
	} else if walkChan, err = walkPath(ctx, c.mountPoint, workRequest.Path, walkChanSize); err != nil {
		return err
	}

	return c.executeJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan)
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return nil
}

// GetConfig is not implemented and should never be called.
func (c *JobBuilderClient) GetConfig() *flex.RemoteStorageTarget {
	return nil
}

// GetWalk is not implemented and should never be called.
func (c *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int) (<-chan *WalkResponse, error) {
	return nil, ErrUnsupportedOpForRST
}

// SanitizeRemotePath should never be called.
func (c *JobBuilderClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

// GetRemoteInfo is not implemented and should never be called.
func (r *JobBuilderClient) GetRemoteInfo(ctx context.Context, remotePath string, cfg *flex.JobRequestCfg, lockedInfo *flex.JobLockedInfo) (remoteSize int64, remoteMtime time.Time, externalId string, err error) {
	return 0, time.Time{}, "", ErrUnsupportedOpForRST
}

func (r *JobBuilderClient) executeJobBuilderRequest(ctx context.Context, request *flex.WorkRequest, walkChan <-chan *WalkResponse, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	builder := request.GetBuilder()
	cfg := builder.GetCfg()
	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		return err
	}

	var walkingPath bool
	var remotePathDir string
	var remotePathIsGlob bool
	var remotePathDirName string
	if cfg.Download {
		walkingPath = !IsValidRstId(cfg.RemoteStorageTarget) && cfg.RemotePath == ""
		remotePathDir, remotePathIsGlob = getRemotePathDirectory(cfg.RemotePath)
		remotePathDirName = filepath.Base(remotePathDir)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	var errCount atomic.Uint32
	workers := 2
	for range workers {
		g.Go(func() error {

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
						cancel()
						return walkResp.Err
					}

					if cfg.Download {
						if walkingPath {
							// Walking cfg.Path to support stub file download.
							inMountPath = walkResp.Path
						} else {
							// Walking cfg.RemotePath
							remotePath = normalizePath(walkResp.Path)
							relPath, _ := filepath.Rel(remotePathDir, remotePath)
							if cfg.Flatten {
								relPath = strings.Replace(relPath, "/", "_", -1)
							}

							if relPath == "." {
								// remote-path is a key for a non-existing file.
								inMountPath = filepath.Join(cfg.Path, remotePath)
							} else if remotePathIsGlob {
								// remote-path is file glob pattern
								inMountPath = filepath.Join(cfg.Path, relPath) // No directory when glob pattern is used
							} else {
								// remote-path is a prefix
								inMountPath = filepath.Join(cfg.Path, remotePathDirName, relPath)
							}

							// Ensure the local directory structure supports the object downloads
							if err := r.mountPoint.CreateDir(filepath.Dir(inMountPath)); err != nil {
								return err
							}
						}
					} else {
						inMountPath = walkResp.Path
						remotePath = inMountPath
					}
				}

				var client Provider
				if IsValidRstId(cfg.RemoteStorageTarget) {
					client = r.rstMap[cfg.RemoteStorageTarget]
				}

				jobRequests, err := BuildJobRequests(ctx, client, r.rstMap, r.mountPoint, store, mappings, inMountPath, remotePath, cfg)

				if err != nil {
					cancel()
					return fmt.Errorf("fatal error occurred while building job requests: %w", err)
				}

				for _, jobRequest := range jobRequests {
					if jobRequest.Err != nil {
						// TODO: What should we do with this error?
						continue
					}
					jobSubmissionChan <- jobRequest.Request
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		if errCount.Load() > 0 {
			return fmt.Errorf("failed to create %d job request(s)", errCount.Load())
		}
	}
	return nil
}

// normalizePath simply ensures that there is a single lead forward-slash. This is expected for all
// in-mount BeeGFS paths. When mapping between local and remote paths it's important to be
// consistent.
func normalizePath(path string) string {
	return "/" + strings.TrimLeft(path, "/")
}

// getRemotePathDirectory returns the directory part of remotePath before any globbing pattern.
func getRemotePathDirectory(remotePath string) (directory string, isGlob bool) {
	directory = StripGlobPattern(normalizePath(remotePath))
	isGlob = directory != remotePath
	if isGlob && !strings.HasSuffix(directory, "/") {
		directory = filepath.Dir(directory)
	}

	return
}
