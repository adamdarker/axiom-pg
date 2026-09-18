package grpc

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	
	"github.com/axiom/axiom-agent/internal/agent"
	agentv1 "github.com/axiom/axiom-agent/pkg/api/agent/v1"
	bootstrapv1 "github.com/axiom/axiom-agent/pkg/api/bootstrap/v1"
)

// Server holds the gRPC server and its dependencies.
type Server struct {
	grpcServer *grpc.Server
	agent      *agent.Agent
	port       int
}

// NewServer initializes a new gRPC server and registers Axiom and Bootstrap services.
func NewServer(a *agent.Agent, port int, useTLS bool) (*Server, error) {
	var opts []grpc.ServerOption

	if useTLS {
		log.Println("mTLS enabled (placeholder configuration)")
		// Future implementation:
		// tlsConfig, err := loadTLSConfig()
		// opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	s := grpc.NewServer(opts...)
	
	// Register the official architect-defined services
	agentv1.RegisterAxiomServiceServer(s, a)
	bootstrapv1.RegisterBootstrapServiceServer(s, a)

	return &Server{
		grpcServer: s,
		agent:      a,
		port:       port,
	}, nil
}

// Start runs the gRPC server on the specified port.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	log.Printf("Axiom Agent gRPC server listening on %v", lis.Addr())
	return s.grpcServer.Serve(lis)
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() {
	log.Println("Shutting down gRPC server...")
	s.grpcServer.GracefulStop()
}
