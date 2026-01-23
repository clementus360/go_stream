package api

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
)

// DialService creates a secure, robust connection to a dependency service
func DialService(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	// 1. Load mTLS credentials (ensure your certs are in the project path)
	creds, err := LoadClientTLSCredentials()
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS: %w", err)
	}

	// 2. Configure the client with production-grade options
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithAuthority("localhost"), // Matches the SNI in your certificates

		// Force TCP4 and apply a strict connection timeout
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, "tcp4", s)
		}),

		// Default call options for high-performance/large payloads
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024), // 10MB limit for large user/session data
		),
	)
}
