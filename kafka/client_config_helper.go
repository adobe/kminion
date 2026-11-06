package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/youmark/pkcs8"
	"go.uber.org/zap"

	krbconfig "github.com/jcmturner/gokrb5/v8/config"
)

var legacyPEMWarnOnce sync.Once

// buildGSSAPIMechanism constructs the Kerberos/GSSAPI SASL mechanism for the given config. It
// returns an error if authType is not one of USER_AUTH or KEYTAB_AUTH, or if the Kerberos config or
// keytab file cannot be loaded.
func buildGSSAPIMechanism(cfg SASLGSSAPIConfig) (sasl.Mechanism, error) {
	kerbCfg, err := krbconfig.Load(cfg.KerberosConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create kerberos config from specified config filepath: %w", err)
	}

	var krbClient *client.Client
	switch cfg.AuthType {
	case "USER_AUTH":
		krbClient = client.NewWithPassword(
			cfg.Username,
			cfg.Realm,
			cfg.Password,
			kerbCfg,
			client.DisablePAFXFAST(!cfg.EnableFast))
	case "KEYTAB_AUTH":
		ktb, err := keytab.Load(cfg.KeyTabPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load keytab: %w", err)
		}
		krbClient = client.NewWithKeytab(
			cfg.Username,
			cfg.Realm,
			ktb,
			kerbCfg,
			client.DisablePAFXFAST(!cfg.EnableFast))
	default:
		return nil, fmt.Errorf("kafka.sasl.gssapi.authType must be one of USER_AUTH or KEYTAB_AUTH")
	}

	return kerberos.Auth{
		Client:           krbClient,
		Service:          cfg.ServiceName,
		PersistAfterAuth: true,
	}.AsMechanism(), nil
}

// NewKgoConfig creates a new Config for the Kafka Client as exposed by the franz-go library.
// If TLS certificates can't be read an error will be returned.
// logger is only used to print warnings about TLS.
func NewKgoConfig(cfg Config, logger *zap.Logger) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.FetchMaxBytes(5 * 1000 * 1000), // 5MB
		kgo.MaxConcurrentFetches(cfg.MaxConcurrentFetches),
		// Allow metadata to be refreshed more often than 5s (default) if needed.
		// That will mitigate issues with unknown partitions shortly after creating
		// them.
		kgo.MetadataMinAge(time.Second),
	}

	// Create Logger
	kgoLogger := KgoZapLogger{
		logger: logger.Sugar(),
	}
	opts = append(opts, kgo.WithLogger(kgoLogger))

	// Add Rack Awareness if configured
	if cfg.RackID != "" {
		opts = append(opts, kgo.Rack(cfg.RackID))
	}

	// Configure SASL
	if cfg.SASL.Enabled {
		// SASL Plain
		if cfg.SASL.Mechanism == "PLAIN" {
			mechanism := plain.Auth{
				User: cfg.SASL.Username,
				Pass: cfg.SASL.Password,
			}.AsMechanism()
			opts = append(opts, kgo.SASL(mechanism))
		}

		// SASL SCRAM
		if cfg.SASL.Mechanism == "SCRAM-SHA-256" || cfg.SASL.Mechanism == "SCRAM-SHA-512" {
			var mechanism sasl.Mechanism
			scramAuth := scram.Auth{
				User: cfg.SASL.Username,
				Pass: cfg.SASL.Password,
			}
			if cfg.SASL.Mechanism == "SCRAM-SHA-256" {
				mechanism = scramAuth.AsSha256Mechanism()
			}
			if cfg.SASL.Mechanism == "SCRAM-SHA-512" {
				mechanism = scramAuth.AsSha512Mechanism()
			}
			opts = append(opts, kgo.SASL(mechanism))
		}

		// Kerberos
		if cfg.SASL.Mechanism == "GSSAPI" {
			mechanism, err := buildGSSAPIMechanism(cfg.SASL.GSSAPI)
			if err != nil {
				return nil, err
			}
			opts = append(opts, kgo.SASL(mechanism))
		}

		// OAuthBearer
		if cfg.SASL.Mechanism == SASLMechanismOAuthBearer {
			switch cfg.SASL.OAuthBearer.Type {
			case OAuthBearerTypeAdobeIMS:
				bearer, err := NewAdobeImsOAuthBearer(
					cfg.SASL.OAuthBearer.ClientID,
					cfg.SASL.OAuthBearer.ClientSecret,
					cfg.SASL.OAuthBearer.Additional.ClientCode,
					cfg.SASL.OAuthBearer.TokenEndpoint,
					logger,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to create Adobe IMS OAuth bearer: %w", err)
				}
				opts = append(opts, bearer.Opt())
			default:
				// Generic OAuth
				mechanism := oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
					token, err := cfg.SASL.OAuthBearer.getToken(ctx)
					return oauth.Auth{
						Zid:   cfg.SASL.OAuthBearer.ClientID,
						Token: token,
					}, err
				})
				opts = append(opts, kgo.SASL(mechanism))
			}
		}
	}

	// Configure TLS
	var caCertPool *x509.CertPool
	if cfg.TLS.Enabled {
		// Root CA
		if cfg.TLS.CaFilepath != "" || len(cfg.TLS.Ca) > 0 {
			ca := []byte(cfg.TLS.Ca)
			if cfg.TLS.CaFilepath != "" {
				caBytes, err := os.ReadFile(cfg.TLS.CaFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to load ca cert: %w", err)
				}
				ca = caBytes
			}
			caCertPool = x509.NewCertPool()
			isSuccessful := caCertPool.AppendCertsFromPEM(ca)
			if !isSuccessful {
				logger.Warn("failed to append ca file to cert pool, is this a valid PEM format?")
			}
		}

		// If configured load TLS cert & key - Mutual TLS
		var certificates []tls.Certificate
		hasCertFile := cfg.TLS.CertFilepath != "" || len(cfg.TLS.Cert) > 0
		hasKeyFile := cfg.TLS.KeyFilepath != "" || len(cfg.TLS.Key) > 0
		if hasCertFile || hasKeyFile {
			cert := []byte(cfg.TLS.Cert)
			privateKey := []byte(cfg.TLS.Key)
			// 1. Read certificates
			if cfg.TLS.CertFilepath != "" {
				certBytes, err := os.ReadFile(cfg.TLS.CertFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to TLS certificate: %w", err)
				}
				cert = certBytes
			}

			if cfg.TLS.KeyFilepath != "" {
				keyBytes, err := os.ReadFile(cfg.TLS.KeyFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to read TLS key: %w", err)
				}
				privateKey = keyBytes
			}

			// 2. Check if private key needs to be decrypted. Decrypt it if passphrase is given, otherwise return error
			pemBlock, _ := pem.Decode(privateKey)
			if pemBlock == nil {
				return nil, fmt.Errorf("no valid private key found")
			}

			// Without a passphrase both decrypt functions return unclear errors.
			isEncrypted := pemBlock.Type == "ENCRYPTED PRIVATE KEY" || x509.IsEncryptedPEMBlock(pemBlock) //nolint:staticcheck // Supporting legacy keys for backward compatibility
			if isEncrypted && cfg.TLS.Passphrase == "" {
				return nil, fmt.Errorf("private key is encrypted, but kafka.tls.passphrase is not set")
			}

			if pemBlock.Type == "ENCRYPTED PRIVATE KEY" {
				key, err := pkcs8.ParsePKCS8PrivateKey(pemBlock.Bytes, []byte(cfg.TLS.Passphrase))
				if err != nil {
					return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: %w", err)
				}
				der, err := x509.MarshalPKCS8PrivateKey(key)
				if err != nil {
					return nil, fmt.Errorf("failed to encode decrypted private key: %w", err)
				}
				privateKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
			} else if x509.IsEncryptedPEMBlock(pemBlock) { //nolint:staticcheck // Supporting legacy keys for backward compatibility
				// NewKgoConfig runs for every client and every health check tick, so warn only once.
				legacyPEMWarnOnce.Do(func() {
					logger.Warn("the TLS private key uses legacy PEM encryption, which is insecure. Use a PKCS#8 encrypted key instead")
				})
				decryptedKey, err := x509.DecryptPEMBlock(pemBlock, []byte(cfg.TLS.Passphrase)) //nolint:staticcheck // Supporting legacy keys for backward compatibility
				if err != nil {
					return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: %w", err)
				}
				// DecryptPEMBlock does not always detect a wrong passphrase, so check that the result is a valid key.
				_, errPKCS1 := x509.ParsePKCS1PrivateKey(decryptedKey)
				_, errPKCS8 := x509.ParsePKCS8PrivateKey(decryptedKey)
				_, errEC := x509.ParseECPrivateKey(decryptedKey)
				if errPKCS1 != nil && errPKCS8 != nil && errEC != nil {
					return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: %w", x509.IncorrectPasswordError)
				}
				privateKey = pem.EncodeToMemory(&pem.Block{Type: pemBlock.Type, Bytes: decryptedKey})
			}

			// 3. Parse the certificate and key pair
			tlsCert, err := tls.X509KeyPair(cert, privateKey)
			if err != nil {
				return nil, fmt.Errorf("cannot parse pem: %w", err)
			}
			certificates = []tls.Certificate{tlsCert}
		}

		if cfg.TLS.InsecureSkipTLSVerify {
			logger.Warn("TLS certificate verification is disabled (insecureSkipTlsVerify=true); " +
				"the connection to Kafka is vulnerable to man-in-the-middle attacks")
		}
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config: &tls.Config{
				InsecureSkipVerify: cfg.TLS.InsecureSkipTLSVerify,
				Certificates:       certificates,
				RootCAs:            caCertPool,
			},
		}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	return opts, nil
}
