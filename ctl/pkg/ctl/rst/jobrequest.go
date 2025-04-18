package rst

import (
	"context"
	"fmt"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// To add a new job request type, update the following components:
// * Job Request Logic:
// 	  - Update the JobRequestCfg struct if additional configuration parameters are needed.
// 	  - Extend the JobType constants and update JobTypeString() to include the new type.
// 	  - Implement a new getXXXJobRequest() function (similar to getSyncJobRequest())
// 	  	to construct the specific job request.
// 	  - Modify sendJobRequest() to handle the new JobType and call the corresponding
// 	  	getXXXJobRequest() function.
// 	  - Adjust getJobType() logic to correctly select and return the new type based on
// 	  	the configuration and file system state.
//
// * Protobuf Definitions:
// 	  - Add the new job request message type to the flex package.
// 	  - Update the JobRequest message enumeration in the protobuf definitions to include
// 	  	the new job request type.

type JobType int32

const (
	JobTypeUnspecified JobType = 0
	JobTypeSync        JobType = 1
)

func JobTypeString(jobType JobType) string {
	var jobTypeString string
	switch jobType {
	case JobTypeUnspecified:
		jobTypeString = "unspecified"
	case JobTypeSync:
		jobTypeString = "sync"
	}
	return jobTypeString
}

type JobRequestCfg struct {
	RSTID      uint32
	Path       string
	RemotePath string
	Download   bool // Whether the operation should be a download operation
	StubLocal  bool // Whether the local file should be a stub file after the operation
	Overwrite  bool // Whether existing files should be overwritten
	Flatten    bool // Whether the directory path should be preserved
	Force      bool // Attempt to force the job request
}

type JobResponse struct {
	Path     string
	Result   *beeremote.JobResult
	Status   beeremote.SubmitJobResponse_ResponseStatus
	Err      error
	FatalErr bool
}

type jobRequestWithError struct {
	Request  *beeremote.JobRequest
	Err      error
	FatalErr bool
}

// SubmitJobRequest asynchronously submits jobs returning the responses over the provided
// channel. It will immediately return an error if anything went wrong during setup. Subsequent
// errors that occur while submitting requests will be returned over the JobResponse channel. Most
// errors returned over the channel are likely limited to the specific request (i.e., an invalid
// request) and it is up to the caller to decide to continue making requests or cancel the context.
// If a fatal error occurs that is likely to prevent scheduling all future requests (such as errors
// walking a directory or connecting to BeeRemote) then the response will have FatalError=true and
// all outstanding goroutines will be immediately cancelled. In all cases the channel is closed once
// there are no more responses to receive.
func SubmitJobRequest(ctx context.Context, cfg *flex.JobRequestCfg, chanSize int) (<-chan *JobResponse, context.CancelFunc, error) {
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to retrieve RST mappings: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	beegfs, err := config.BeeGFSClient(cfg.Path)
	if err != nil {
		return nil, cancel, err
	}

	requestChan := make(chan *jobRequestWithError, chanSize)
	go func() {
		defer close(requestChan)
		sendJobRequest(ctx, beegfs, mappings, cfg, requestChan)
	}()

	respChan, err := submitJobRequests(ctx, cancel, requestChan, false)
	if err != nil {
		return nil, cancel, err
	}
	return respChan, cancel, nil
}

// StreamSubmitJobRequests asynchronously streams job requests from the caller while returning
// responses over the response channel unless ignoreResponses==true. It will immediately return an
// error if anything went wrong during setup. It is the responsibility of the caller to close the
// *flex.JobRequestCfg channel.
//
// If a fatal error occurs that is likely to prevent scheduling any future requests (such as
// connecting to BeeRemote) then the response will have FatalError=true and the stream will be
// cancelled immediately.
//
//	jobRequestCfgChan, jobResponseChan, closeStream, err := rst.StreamSubmitJobRequests(ctx, beegfs, 2048)
//	if err != nil {
//	    return fmt.Errorf("unable to stream job requests: %w", err)
//	}
//	jobRequestCfgChan <- (...)
//	close(jobRequestCfgChan)
func StreamSubmitJobRequests(ctx context.Context, beegfs filesystem.Provider, chanSize int, ignoreResponses bool) (chan<- *flex.JobRequestCfg, <-chan *JobResponse, error) {
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to retrieve RST mappings: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	cfgChan := make(chan *flex.JobRequestCfg, chanSize)
	requestChan := make(chan *jobRequestWithError, chanSize)
	go func() {
		defer close(requestChan)
		for {
			select {
			case <-ctx.Done():
				// Drain cfgChan to avoid blocking the caller
				for range cfgChan {
				}
				return
			case cfg, ok := <-cfgChan:
				if !ok {
					return
				}
				sendJobRequest(ctx, beegfs, mappings, cfg, requestChan)
			}
		}
	}()

	respChan, err := submitJobRequests(ctx, cancel, requestChan, ignoreResponses)
	if err != nil {
		return nil, nil, err
	}
	return cfgChan, respChan, nil
}

func submitJobRequests(ctx context.Context, cancel context.CancelFunc, requestChan <-chan *jobRequestWithError, ignoreResponses bool) (<-chan *JobResponse, error) {
	remote, err := config.BeeRemoteClient()
	if err != nil {
		cancel()
		return nil, err
	}

	if ignoreResponses {
		for range numWorkers() {
			go func() {
				submitJobRequestWorker(ctx, cancel, remote, requestChan, nil)
			}()
		}
		return nil, nil
	}

	wg := sync.WaitGroup{}
	respChan := make(chan *JobResponse, cap(requestChan))
	for range numWorkers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			submitJobRequestWorker(ctx, cancel, remote, requestChan, respChan)
		}()
	}

	// Start a go routine to wait for the workers to finish before closing the response channel.
	// This allows the responses to be streamed to the caller.
	go func() {
		wg.Wait()
		close(respChan)
	}()
	return respChan, nil
}

func sendJobRequest(ctx context.Context, beegfs filesystem.Provider, mappings *util.Mappings, cfg *flex.JobRequestCfg, requestChan chan<- *jobRequestWithError) {
	var err error
	var pathInfo mountPathInfo
	sendError := func(err error) {
		requestChan <- &jobRequestWithError{
			Request: &beeremote.JobRequest{Path: pathInfo.Path, RemoteStorageTarget: cfg.RemoteStorageTarget},
			Err:     err,
		}
	}

	pathInfo, err = getMountPathInfo(beegfs, cfg.Path)
	if err != nil {
		sendError(err)
		return
	}
	cfg.Path = pathInfo.Path

	var rstIds []uint32
	jobBuilder := false
	if !pathInfo.Exists {
		jobBuilder = true
		if !validRstId(cfg.RemoteStorageTarget) {
			sendError(fmt.Errorf("unable to send job requests! Invalid RST identifier"))
		}
		rstIds = []uint32{cfg.RemoteStorageTarget}
	} else if pathInfo.IsDir {
		jobBuilder = true
	} else {
		entry, err := getEntry(ctx, mappings, pathInfo.Path)
		if err != nil {
			sendError(err)
		}

		ignoreReaders := cfg.Force || !cfg.Download
		ignoreWriters := cfg.Force
		err = checkEntry(entry.Entry, ignoreReaders, ignoreWriters)
		if err != nil {
			sendError(err)
		}

		if validRstId(cfg.RemoteStorageTarget) {
			rstIds = []uint32{cfg.RemoteStorageTarget}
		} else if rstIds, err = getEntryRSTs(entry.Entry); err != nil {
			sendError(err)
		}
	}

	if jobBuilder {
		jobRequest := getJobBuilderRequest(pathInfo.Path, 0, cfg)
		requestChan <- &jobRequestWithError{Request: jobRequest}
		return
	}

	var jobRequest *beeremote.JobRequest
	for _, rstId := range rstIds {
		rst := mappings.RstIdToConfig[rstId]
		switch rst.WhichType() {
		case flex.RemoteStorageTarget_S3_case:
			jobRequest = getSyncJobRequest(pathInfo.Path, rstId, cfg)
		default:
			sendError(ErrFileTypeUnsupported)
			continue
		}
		requestChan <- &jobRequestWithError{Request: jobRequest}
	}
}

func submitJobRequest(ctx context.Context, cancel context.CancelFunc, remote beeremote.BeeRemoteClient, request *beeremote.JobRequest) *JobResponse {
	resp := &JobResponse{Path: request.Path}
	select {
	case <-ctx.Done():
		return nil
	default:
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
					// This would happen if there is an error connecting to BeeRemote.
					// Don't keep trying and signal other goroutines to shutdown as well.
					cancel()
				}
			}
		} else {
			resp.Result = submission.Result
			resp.Status = submission.GetStatus()
		}
	}
	return resp
}

func submitJobRequestWorker(ctx context.Context, cancel context.CancelFunc, remote beeremote.BeeRemoteClient, requestChan <-chan *jobRequestWithError, respChan chan<- *JobResponse) {
	send := func(jobResp *JobResponse) {
		if respChan != nil {
			respChan <- jobResp
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case request, ok := <-requestChan:
			if !ok {
				return
			}

			if request.Err != nil {
				if request.FatalErr {
					send(&JobResponse{Path: request.Request.Path, Err: request.Err, FatalErr: request.FatalErr})
					cancel()
					return
				}
				send(&JobResponse{Path: request.Request.Path, Err: request.Err})
				continue
			}

			resp := submitJobRequest(ctx, cancel, remote, request.Request)
			if resp != nil {
				send(resp)
			}
		}
	}
}

func getJobBuilderRequest(inMountPath string, rstId uint32, cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                inMountPath,
		RemoteStorageTarget: rstId,
		StubLocal:           cfg.StubLocal,
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Builder{
			Builder: &flex.BuilderJob{
				Cfg: cfg,
			},
		},
	}
}

func getSyncJobRequest(inMountPath string, rstId uint32, cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	var operation flex.SyncJob_Operation
	if cfg.Download {
		operation = flex.SyncJob_DOWNLOAD
	} else {
		operation = flex.SyncJob_UPLOAD
	}

	return &beeremote.JobRequest{
		Path:                inMountPath,
		RemoteStorageTarget: rstId,
		StubLocal:           cfg.StubLocal,
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				RemotePath: cfg.RemotePath,
				Operation:  operation,
				Overwrite:  cfg.Overwrite,
				Flatten:    cfg.Flatten,
				LockedInfo: cfg.LockedInfo,
			}}}
}

type mountPathInfo struct {
	Path   string
	Exists bool
	IsDir  bool
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
		// ANY ERROR WILL BE TREATED AS THE FILE DOES NOT EXIST
		// TODO: this is error is not correct
		//	- Error: lstat /mnt/beegfs/test/1: not a directory: unable to initialize file system client from the specified path
		//  - `not a directory` is beegfs.OpsErr_NOTADIR but is not being recognized as the error
		// if !errors.Is(err, os.ErrNotExist) {
		// 	return result, err
		// }
		return result, nil
	}
	result.Exists = true
	result.IsDir = info.IsDir()
	return result, nil
}

func getEntry(ctx context.Context, mappings *util.Mappings, path string) (*entry.GetEntryCombinedInfo, error) {
	entry, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{}, path)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func checkEntry(entry entry.Entry, ignoreReaders bool, ignoreWriters bool) error {
	var err error
	if !ignoreWriters && entry.NumSessionsWrite > 0 {
		err = ErrFileOpenForWriting
	}
	if !ignoreReaders && entry.NumSessionsRead > 0 {
		// Not using errors.Join because it adds a newline when printing each error which looks
		// awkward in the CTL output.
		if err != nil {
			err = ErrFileOpenForReadingAndWriting
		} else {
			err = ErrFileOpenForReading
		}
	}
	return err
}

func validRstId(rstId uint32) bool {
	return rstId != 0
}

func getEntryRSTs(entry entry.Entry) ([]uint32, error) {
	if len(entry.Remote.RSTIDs) == 0 {
		return nil, ErrFileHasNoRSTs
	}
	rstIDs := make([]uint32, len(entry.Remote.RSTIDs))
	copy(rstIDs, entry.Remote.RSTIDs)
	return rstIDs, nil
}

func numWorkers() int {
	count := viper.GetInt(config.NumWorkersKey)
	if count > 1 {
		return count - 1
	}
	return 1
}
