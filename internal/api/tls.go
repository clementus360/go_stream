package api

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// LoadServerTLSCredentials sets up mTLS for the Database Server
func LoadServerTLSCredentials() (credentials.TransportCredentials, error) {
	// 1. Load the Database Server's certificate and private key
	// This allows the server to identify itself to clients
	serverCert, err := tls.LoadX509KeyPair("certs/stream-cert.pem", "certs/stream-key.pem")
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %w", err)
	}

	// 2. Load the Root CA certificate
	// This is the "Master Stamp" used to verify incoming client certificates
	pemClientCA, err := os.ReadFile("certs/ca-cert.pem")
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemClientCA) {
		return nil, fmt.Errorf("failed to append client CA cert")
	}

	// 3. Create the TLS configuration
	config := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		// RequireAndVerifyClientCert is the "m" in mTLS.
		// It forces the client to provide a certificate signed by our CA.
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  certPool,
		MinVersion: tls.VersionTLS13, // Modern, secure TLS version
	}

	return credentials.NewTLS(config), nil
}

func LoadClientTLSCredentials() (credentials.TransportCredentials, error) {
	// 1. Load Auth Service's own cert/key (to prove who it is to the DB)
	cert, err := tls.LoadX509KeyPair("certs/stream-cert.pem", "certs/stream-key.pem")
	if err != nil {
		return nil, err
	}

	// 2. Load Root CA (to verify the DB's certificate)
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile("certs/ca-cert.pem")
	if err != nil {
		return nil, err
	}
	certPool.AppendCertsFromPEM(ca)

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS13,
	}

	return credentials.NewTLS(config), nil
}
