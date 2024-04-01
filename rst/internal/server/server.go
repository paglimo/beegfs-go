package server

import (
	"context"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/bee-remote/internal/job"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s *BeeRemoteServer) SubmitJob(ctx context.Context, request *beeremote.JobRequest) (*beeremote.JobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return s.jobMgr.SubmitJobRequest(request)
}

func (s *BeeRemoteServer) GetJobs(ctx context.Context, request *beeremote.GetJobsRequest) (*beeremote.GetJobsResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	response, err := s.jobMgr.GetJobs(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (s *BeeRemoteServer) UpdateJob(ctx context.Context, request *beeremote.UpdateJobRequest) (*beeremote.UpdateJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if request.Wait {
		return s.jobMgr.UpdateJob(request)
	}
	s.jobMgr.JobUpdates <- request
	return &beeremote.UpdateJobResponse{
		Ok:      true,
		Message: "asynchronous update requested (query the path or job ID later to check the result)",
	}, nil
}

func (s *BeeRemoteServer) UpdateWorkRequest(ctx context.Context, workResponse *flex.WorkResponse) (*emptypb.Empty, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return &emptypb.Empty{}, s.jobMgr.UpdateJobResults(workResponse)
}
