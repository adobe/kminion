package kafka

import "fmt"

// SASLGSSAPIConfig represents the Kafka Kerberos config
type SASLGSSAPIConfig struct {
	AuthType           string `koanf:"authType"`
	KeyTabPath         string `koanf:"keyTabPath"`
	KerberosConfigPath string `koanf:"kerberosConfigPath"`
	ServiceName        string `koanf:"serviceName"`
	Username           string `koanf:"username"`
	Password           string `koanf:"password"`
	Realm              string `koanf:"realm"`

	// EnableFAST enables FAST, which is a pre-authentication framework for Kerberos.
	// It includes a mechanism for tunneling pre-authentication exchanges using armoured KDC messages.
	// FAST provides increased resistance to passive password guessing attacks.
	EnableFast bool `koanf:"enableFast"`
}

func (s *SASLGSSAPIConfig) SetDefaults() {
	s.EnableFast = true
}

func (s *SASLGSSAPIConfig) Validate() error {
	switch s.AuthType {
	case "USER_AUTH", "KEYTAB_AUTH":
	default:
		return fmt.Errorf("kafka.sasl.gssapi.authType must be one of USER_AUTH or KEYTAB_AUTH, got '%s'", s.AuthType)
	}

	if s.KerberosConfigPath == "" {
		return fmt.Errorf("kafka.sasl.gssapi.kerberosConfigPath must be set")
	}
	if s.ServiceName == "" {
		return fmt.Errorf("kafka.sasl.gssapi.serviceName must be set")
	}

	switch s.AuthType {
	case "USER_AUTH":
		if s.Username == "" || s.Password == "" || s.Realm == "" {
			return fmt.Errorf("kafka.sasl.gssapi.authType USER_AUTH requires username, password and realm to be set")
		}
	case "KEYTAB_AUTH":
		if s.KeyTabPath == "" || s.Username == "" || s.Realm == "" {
			return fmt.Errorf("kafka.sasl.gssapi.authType KEYTAB_AUTH requires keyTabPath, username and realm to be set")
		}
	}

	return nil
}
