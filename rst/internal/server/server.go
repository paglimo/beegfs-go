package server

import (
	"context"
	"net"
	"path"
	"reflect"
	"sync"

	beeremote "github.com/thinkparq/protobuf/beeremote/go"
	br "github.com/thinkparq/protobuf/beeremote/go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	address        string
	tlsCertificate string
	tlsKey         string
}

// Verify interfaces are satisfied:
var _ br.BeeRemoteServer = &BeeRemoteServer{}

type BeeRemoteServer struct {
	br.UnimplementedBeeRemoteServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer  *grpc.Server
	jobRequests chan<- *beeremote.JobRequest
}

// New() creates a new BeeRemoteServer that can be used with ListenAndServe().
// It requires a channel where it can send job requests to JobMgr.
func New(log *zap.Logger, config Config, jobRequests chan<- *beeremote.JobRequest) (*BeeRemoteServer, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(BeeRemoteServer{}).PkgPath())))

	s := BeeRemoteServer{
		log:         log,
		Config:      config,
		wg:          new(sync.WaitGroup),
		jobRequests: jobRequests,
	}

	var grpcServerOpts []grpc.ServerOption
	if s.tlsCertificate != "" && s.tlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(s.tlsCertificate, s.tlsKey)
		if err != nil {
			return nil, err
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	} else {
		s.log.Warn("not using TLS because certificate and/or key were not specified")
	}
	s.grpcServer = grpc.NewServer(grpcServerOpts...)
	br.RegisterBeeRemoteServer(s.grpcServer, &s)

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

	s.log.Debug("listening on local network address", zap.Any("address", s.address))
	lis, err := net.Listen("tcp", s.address)
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

func (s *BeeRemoteServer) SubmitJob(ctx context.Context, job *br.JobRequest) (*br.JobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	return nil, nil
}

func (s *BeeRemoteServer) GetJobs(ctx context.Context, job *br.MultiJobRequest) (*br.MultiJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	return nil, nil
}

func (s *BeeRemoteServer) PauseJobs(ctx context.Context, job *br.MultiJobRequest) (*br.MultiJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	return nil, nil
}

func (s *BeeRemoteServer) CancelJobs(ctx context.Context, job *br.MultiJobRequest) (*br.MultiJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// TODO: Implement
	return nil, nil
}
