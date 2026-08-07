package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerTLSConfig builds the scheduler side of worker<->scheduler mTLS
// (docs/09-design-rationale.md 9.4): it presents certFile/keyFile as its
// own server certificate and requires every connecting client to present a
// certificate signed by the CA in caFile, verified on every connection,
// not just at registration. This is what actually prevents rogue worker
// registration: a stolen bootstrap token can be replayed from anywhere, a
// client certificate tied to mTLS cannot.
func ServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("auth: load server keypair: %w", err)
	}

	caPool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ClientTLSConfig builds the worker-agent side: it presents certFile/keyFile
// as its own client certificate (so the scheduler's RequireAndVerifyClientCert
// above has something valid to check) and verifies the scheduler's server
// certificate against the same CA.
func ClientTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("auth: load client keypair: %w", err)
	}

	caPool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("auth: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("auth: no valid certificates found in %s", caFile)
	}
	return pool, nil
}
