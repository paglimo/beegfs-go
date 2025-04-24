package rst

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thinkparq/beegfs-go/common/filesystem"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type JobResponse struct {
	Path     string
	Result   *beeremote.JobResult
	Status   beeremote.SubmitJobResponse_ResponseStatus
	Err      error
	FatalErr bool
}

// SubmitJobRequest asynchronously submits jobs returning the responses over the JobResponse
// channel. It will immediately return an error if anything went wrong during setup. Subsequent
// errors that occur while submitting requests will be returned over the JobResponse channel. Most
// errors returned over the channel are likely limited to the specific request (i.e., an invalid
// request) and it is up to the caller to decide to continue making requests or cancel the context.
// If a fatal error occurs that is likely to prevent scheduling all future requests (such as errors
// walking a directory or connecting to BeeRemote) then the response will have FatalError=true and
// all outstanding goroutines will be immediately cancelled. In all cases the channel is closed once
// there are no more responses to receive.
func SubmitJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, chanSize int) (<-chan *JobResponse, error) {
	requests, err := buildJobRequests(ctx, cfg)
	if err != nil {
		return nil, err
	}

	remote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}

	respChan := make(chan *JobResponse, len(requests))
	go func() {
		defer close(respChan)

		for _, request := range requests {
			resp := &JobResponse{Path: request.Path}
			submission, err := remote.SubmitJob(ctx, &beeremote.SubmitJobRequest{Request: request})
			if err != nil {
				resp.Err = err
				// We have to check the error because submission could be nil so we shouldn't try and
				// deference it to set the resp.Result and some errors are handled specially.
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.Unavailable:
						resp.Err = fmt.Errorf("fatal error sending request to BeeRemote: %w", err)
						resp.FatalErr = true
						respChan <- resp
						return
					}
				}
			} else {
				resp.Result = submission.Result
				resp.Status = submission.GetStatus()
			}

			respChan <- resp
		}
	}()

	return respChan, nil
}

// buildJobRequests creates all job requests required. If the path does not exist, unknown or a
// directory the a job-builder request will be returned. Otherwise, the supplied cfg.rstId, stub
// file url, or the file's rstIds will be used to generate rst specific job requests.
func buildJobRequests(ctx context.Context, cfg *flex.JobRequestCfg) ([]*beeremote.JobRequest, error) {
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve RST mappings: %w", err)
	}
	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil {
		return nil, err
	}

	pathInfo, err := getMountPathInfo(beegfs, cfg.Path)
	if err != nil {
		return nil, err
	}

	cfg.SetPath(pathInfo.Path)
	if !cfg.Download && cfg.RemotePath == "" {
		cfg.SetRemotePath(pathInfo.Path)
	}

	jobBuilder := false
	if !pathInfo.Exists {
		jobBuilder = true
		if !IsValidRstId(cfg.RemoteStorageTarget) {
			return nil, fmt.Errorf("unable to send job requests! Invalid RST identifier")
		}
	} else if pathInfo.IsDir || pathInfo.IsGlob {
		jobBuilder = true
	}

	if jobBuilder {
		client := NewJobBuilderClient(ctx, nil, nil)
		request := client.GetJobRequest(cfg)
		return []*beeremote.JobRequest{request}, nil
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, err
	}

	var errs []error
	var requests []*beeremote.JobRequest
	baseLockedInfo, rstIds, err := GetLockedInfo(ctx, beegfs, store, mappings, cfg, pathInfo.Path)
	if err != nil {
		return nil, err
	}

	for _, rstId := range rstIds {
		config := mappings.RstIdToConfig[rstId]
		client, err := New(ctx, config, beegfs)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		lockedInfo := proto.Clone(baseLockedInfo).(*flex.JobLockedInfo)
		request, err, fatal := PrepareAndBuildJobRequest(ctx, client, beegfs, store, mappings, cfg.Path, cfg.RemotePath, cfg, lockedInfo)
		if err != nil {
			if fatal {
				return nil, fmt.Errorf("fatal error occurred while building job requests: %w", err)
			}
			errs = append(errs, err)
		}
		requests = append(requests, request)
	}

	return requests, errors.Join(errs...)
}

type mountPathInfo struct {
	Path   string
	Exists bool
	IsDir  bool
	IsGlob bool
}

func getMountPathInfo(beegfsProvider filesystem.Provider, path string) (mountPathInfo, error) {
	result := mountPathInfo{Path: path}
	pathInMount, err := beegfsProvider.GetRelativePathWithinMount(path)
	if err != nil {
		return result, err
	}
	result.Path = pathInMount

	info, err := beegfsProvider.Lstat(pathInMount)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
		if _, err := filepath.Glob(path); err != nil {
			result.IsGlob = true
		}
		return result, nil
	}
	result.Exists = true
	result.IsDir = info.IsDir()
	return result, nil
}
