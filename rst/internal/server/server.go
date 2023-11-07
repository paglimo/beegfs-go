package server

import (
	"context"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/bee-remote/internal/job"
	"github.com/thinkparq/protobuf/go/beeremote"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	Address        string `mapstructure:"address"`
	TlsCertificate string `mapstructure:"tlsCertificate"`
	TlsKey         string `mapstructure:"tlsKey"`
}

// Verify interfaces are satisfied:
var _ beeremote.BeeRemoteServer = &BeeRemoteServer{}

type BeeRemoteServer struct {
	beeremote.UnimplementedBeeRemoteServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	jobMgr     *job.Manager
}

// New() creates a new BeeRemoteServer that can be used with ListenAndServe().
// It requires a channel where it can send job requests to JobMgr.
func New(log *zap.Logger, config Config, jobMgr *job.Manager) (*BeeRemoteServer, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(BeeRemoteServer{}).PkgPath())))

	s := BeeRemoteServer{
		log:    log,
		Config: config,
		wg:     new(sync.WaitGroup),
		jobMgr: jobMgr,
	}

	var grpcServerOpts []grpc.ServerOption
	if s.TlsCertificate != "" && s.TlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(s.TlsCertificate, s.TlsKey)
		if err != nil {
			return nil, err
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	} else {
		s.log.Warn("not using TLS because certificate and/or key were not specified")
	}
	s.grpcServer = grpc.NewServer(grpcServerOpts...)
	beeremote.RegisterBeeRemoteServer(s.grpcServer, &s)

	return &s, nil
}

// ListenAndServe should be called against a BeeRemoteServer initialized with
// New(). The caller can decide if it should run asynchronously as a goroutine,
// or synchronously. It will block and serve requests until an error occurs or
// Stop() is called against the BeeRemoteServer.
func (s *BeeRemoteServer) ListenAndServe() {

	// TODO: Whenever we support dynamically updating the server configuration
	// we'll want to make this retry if there is an error listening or serving
	// requests in case a configuration update fixes things.

	s.log.Debug("listening on local network address", zap.Any("address", s.Address))
	lis, err := net.Listen("tcp", s.Address)
	if err != nil {
		s.log.Error("")
	}

	s.log.Info("serving gRPC requests")
	err = s.grpcServer.Serve(lis)
	if err != nil {
		s.log.Error("unable to serve gRPC requests", zap.Error(err))
	}
}

// Stop should be called to gracefully terminate the server. It will stop the
// server then wait for outstanding RPCs to complete before returning.
func (s *BeeRemoteServer) Stop() {
	s.log.Info("attempting to stop gRPC server")
	s.grpcServer.Stop()
	s.wg.Wait()
}

func (s *BeeRemoteServer) SubmitJob(ctx context.Context, job *beeremote.JobRequest) (*beeremote.JobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	// Consider adding a wait flag to the JobRequest then polling GetJobs to return a real response.
	// Or we could add a synchronous JobMgr function to submit jobs then people could submit lots of
	// jobs and not wait for them all to get scheduled if they don't set the wait flag, or get a response.
	return nil, nil
}

func (s *BeeRemoteServer) GetJobs(ctx context.Context, job *beeremote.GetJobsRequest) (*beeremote.GetJobsResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	response, err := s.jobMgr.GetJobs(job)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (s *BeeRemoteServer) UpdateJob(ctx context.Context, job *beeremote.UpdateJobRequest) (*beeremote.UpdateJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	return nil, nil
}
