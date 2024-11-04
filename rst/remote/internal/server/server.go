package server

import (
	"context"
	"errors"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/bee-remote/remote/internal/job"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/protobuf/go/beeremote"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Config struct {
	Address     string `mapstructure:"address"`
	TlsCertFile string `mapstructure:"tls-cert-file"`
	TlsKeyFile  string `mapstructure:"tls-key-file"`
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
	if s.TlsCertFile != "" && s.TlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(s.TlsCertFile, s.TlsKeyFile)
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

func (s *BeeRemoteServer) SubmitJob(ctx context.Context, request *beeremote.SubmitJobRequest) (*beeremote.SubmitJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	result, err := s.jobMgr.SubmitJobRequest(request.Request)
	if err != nil {
		return nil, err
	}
	return &beeremote.SubmitJobResponse{
		Result: result,
	}, nil
}

func (s *BeeRemoteServer) GetJobs(request *beeremote.GetJobsRequest, stream beeremote.BeeRemote_GetJobsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	responses := make(chan *beeremote.GetJobsResponse, 1024)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
	sendResponses:
		for {
			select {
			case <-stream.Context().Done():
				return
			case resp, ok := <-responses:
				if !ok {
					break sendResponses
				}
				stream.Send(resp)
			}
		}
	}()

	err := s.jobMgr.GetJobs(stream.Context(), request, responses)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s *BeeRemoteServer) UpdateJob(ctx context.Context, request *beeremote.UpdateJobRequest) (*beeremote.UpdateJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	resp, err := s.jobMgr.UpdateJob(request)
	if err != nil {
		if errors.Is(err, kvstore.ErrEntryNotInDB) {
			return nil, status.Errorf(codes.NotFound, "%s", err)
		}
		return nil, err
	}
	return resp, nil
}

func (s *BeeRemoteServer) GetRSTConfig(ctx context.Context, request *beeremote.GetRSTConfigRequest) (*beeremote.GetRSTConfigResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	rsts, err := s.jobMgr.GetRSTConfig()
	if err != nil {
		return nil, err
	}

	return &beeremote.GetRSTConfigResponse{
		Rsts: rsts,
	}, nil
}

func (s *BeeRemoteServer) UpdateWork(ctx context.Context, request *beeremote.UpdateWorkRequest) (*beeremote.UpdateWorkResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return &beeremote.UpdateWorkResponse{}, s.jobMgr.UpdateWork(request.Work)
}
