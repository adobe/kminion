package kafka

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTempKrb5Conf(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "krb5.conf")
	content := "[libdefaults]\n  default_realm = EXAMPLE.COM\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestBuildGSSAPIMechanism_UserAuth(t *testing.T) {
	cfg := SASLGSSAPIConfig{
		AuthType:           "USER_AUTH",
		KerberosConfigPath: writeTempKrb5Conf(t),
		ServiceName:        "kafka",
		Username:           "test-user",
		Password:           "test-pass",
		Realm:              "EXAMPLE.COM",
	}

	mechanism, err := buildGSSAPIMechanism(cfg)
	require.NoError(t, err, "USER_AUTH must be recognized (regression test for the 'USER_AUTH:' typo bug)")
	assert.NotNil(t, mechanism)
}

func TestBuildGSSAPIMechanism_InvalidAuthType(t *testing.T) {
	cfg := SASLGSSAPIConfig{
		AuthType:           "BOGUS_AUTH",
		KerberosConfigPath: writeTempKrb5Conf(t),
		ServiceName:        "kafka",
	}

	_, err := buildGSSAPIMechanism(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "authType must be one of USER_AUTH or KEYTAB_AUTH")
}
