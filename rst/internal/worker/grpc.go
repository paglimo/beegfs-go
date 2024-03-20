package worker

// Common helper methods for gRPC related functionality.

import (
	"crypto/tls"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func getGRPCClientConnection(config Config) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	// We specify the option to use a self-signed certificate first so it take
	// precedence. We also ensure these options are mutually exclusive, for
	// example if we allowed a self-signed cert and AllowInsecure to be
	// specified, a vague `error reading server preface: EOF` will be returned.
	if config.SelfSignedTLSCertPath != "" {
		creds, err := credentials.NewClientTLSFromFile(config.SelfSignedTLSCertPath, "")
		if err != nil {
			// If something goes wrong setting up TLS there is no point to retrying.
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else if config.AllowInsecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// By default we'll use the system-wide store of trusted root
		// certificates on the system running the application. This requires the
		// certificate the subscriber is using to have been signed by a trusted
		// root CA. Or the certificate would have needed to been added manually
		// as a trusted certificate.
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	var err error
	conn, err := grpc.Dial(config.Hostname+":"+strconv.Itoa(config.Port), opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to the worker node: %w", err)
	}
	return conn, nil
}
