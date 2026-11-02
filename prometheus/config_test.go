package prometheus

import "testing"
import "github.com/stretchr/testify/assert"

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		{"valid port", 8080, false},
		{"zero port invalid", 0, true},
		{"negative port invalid", -1, true},
		{"port above max invalid", 70000, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Port: tt.port}
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
