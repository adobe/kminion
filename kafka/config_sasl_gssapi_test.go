package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSASLGSSAPIConfig_Validate(t *testing.T) {
	t.Run("valid USER_AUTH config passes", func(t *testing.T) {
		cfg := SASLGSSAPIConfig{
			AuthType:           "USER_AUTH",
			KerberosConfigPath: "/etc/krb5.conf",
			ServiceName:        "kafka",
			Username:           "user",
			Password:           "pass",
			Realm:              "EXAMPLE.COM",
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("valid KEYTAB_AUTH config passes", func(t *testing.T) {
		cfg := SASLGSSAPIConfig{
			AuthType:           "KEYTAB_AUTH",
			KerberosConfigPath: "/etc/krb5.conf",
			ServiceName:        "kafka",
			KeyTabPath:         "/etc/kafka.keytab",
			Username:           "user",
			Realm:              "EXAMPLE.COM",
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("unknown authType is rejected", func(t *testing.T) {
		cfg := SASLGSSAPIConfig{AuthType: "BOGUS", KerberosConfigPath: "/etc/krb5.conf", ServiceName: "kafka"}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "USER_AUTH or KEYTAB_AUTH")
	})

	t.Run("missing kerberosConfigPath is rejected", func(t *testing.T) {
		cfg := SASLGSSAPIConfig{AuthType: "USER_AUTH", ServiceName: "kafka", Username: "u", Password: "p", Realm: "R"}
		require.Error(t, cfg.Validate())
	})
}
