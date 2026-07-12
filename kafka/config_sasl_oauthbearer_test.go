package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthBearerConfig_GenericOAuth(t *testing.T) {
	config := OAuthBearerConfig{
		Type:          "",
		TokenEndpoint: "https://oauth.example.com/token",
		ClientID:      "test-client",
		ClientSecret:  "test-secret",
		Scope:         "kafka",
	}

	if config.Type != "" {
		t.Errorf("Expected empty Type for generic OAuth, got %s", config.Type)
	}

	if config.TokenEndpoint == "" {
		t.Error("TokenEndpoint should not be empty")
	}

	if config.ClientID == "" {
		t.Error("ClientID should not be empty")
	}
}

func TestOAuthBearerConfig_AdobeIMS(t *testing.T) {
	config := OAuthBearerConfig{
		Type:          OAuthBearerTypeAdobeIMS,
		TokenEndpoint: "https://ims-na1.adobelogin.com",
		ClientID:      "adobe-client-id",
		ClientSecret:  "adobe-client-secret",
		Additional: OAuthAdditionalConfig{
			ClientCode: "test-code",
		},
	}

	if config.Type != OAuthBearerTypeAdobeIMS {
		t.Errorf("Expected Type to be AdobeIMS, got %s", config.Type)
	}

	if config.ClientID == "" {
		t.Error("ClientID should not be empty")
	}

	if config.TokenEndpoint == "" {
		t.Error("TokenEndpoint should not be empty")
	}

	if config.Additional.ClientCode == "" {
		t.Error("Additional.ClientCode should not be empty")
	}
}

func TestSASLConfig_ValidateOAuthBearer_Generic(t *testing.T) {
	config := SASLConfig{
		Enabled:   true,
		Mechanism: SASLMechanismOAuthBearer,
		OAuthBearer: OAuthBearerConfig{
			Type:          "",
			TokenEndpoint: "https://oauth.example.com/token",
			ClientID:      "test-client",
			ClientSecret:  "test-secret",
			Scope:         "kafka",
		},
	}

	err := config.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid generic OAuth config, got: %v", err)
	}
}

func TestSASLConfig_ValidateOAuthBearer_Generic_MissingEndpoint(t *testing.T) {
	config := SASLConfig{
		Enabled:   true,
		Mechanism: SASLMechanismOAuthBearer,
		OAuthBearer: OAuthBearerConfig{
			Type:         "",
			ClientID:     "test-client",
			ClientSecret: "test-secret",
		},
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected error for missing token endpoint")
	}
}

func TestSASLConfig_ValidateOAuthBearer_AdobeIMS(t *testing.T) {
	config := SASLConfig{
		Enabled:   true,
		Mechanism: SASLMechanismOAuthBearer,
		OAuthBearer: OAuthBearerConfig{
			Type:          OAuthBearerTypeAdobeIMS,
			TokenEndpoint: "https://ims-na1.adobelogin.com",
			ClientID:      "adobe-client-id",
			ClientSecret:  "adobe-client-secret",
			Additional: OAuthAdditionalConfig{
				ClientCode: "test-code",
			},
		},
	}

	err := config.Validate()
	if err != nil {
		t.Errorf("Expected no error for Adobe IMS config, got: %v", err)
	}
}

func TestSASLConfig_ValidateOAuthBearer_AdobeIMS_MissingClientCode(t *testing.T) {
	config := SASLConfig{
		Enabled:   true,
		Mechanism: SASLMechanismOAuthBearer,
		OAuthBearer: OAuthBearerConfig{
			Type:          OAuthBearerTypeAdobeIMS,
			TokenEndpoint: "https://ims-na1.adobelogin.com",
			ClientID:      "adobe-client-id",
			ClientSecret:  "adobe-client-secret",
			// Missing clientCode in Additional
		},
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected error for missing Adobe IMS client code")
	}
}

func TestSASLConfig_ValidateOAuthBearer_UnknownType(t *testing.T) {
	config := SASLConfig{
		Enabled:   true,
		Mechanism: SASLMechanismOAuthBearer,
		OAuthBearer: OAuthBearerConfig{
			Type:          "UnknownProvider",
			TokenEndpoint: "https://example.com/token",
			ClientID:      "client-id",
			ClientSecret:  "client-secret",
		},
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected error for unknown OAuth type")
	}
}

func TestOAuthBearerConfig_GetToken_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "test-token-123"})
	}))
	defer server.Close()

	cfg := OAuthBearerConfig{
		TokenEndpoint: server.URL,
		ClientID:      "client",
		ClientSecret:  "secret",
	}

	token, err := cfg.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "test-token-123", token)
}

func TestOAuthBearerConfig_GetToken_RespectsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "too-late"})
	}))
	defer server.Close()

	cfg := OAuthBearerConfig{
		TokenEndpoint: server.URL,
		ClientID:      "client",
		ClientSecret:  "secret",
	}

	_, err := cfg.getTokenWithTimeout(context.Background(), 50*time.Millisecond)
	require.Error(t, err, "expected a client-side timeout error when the token endpoint is slower than the configured timeout")
}

func TestOAuthBearerConfig_Validate_RejectsNonHTTPSEndpoint(t *testing.T) {
	config := OAuthBearerConfig{
		TokenEndpoint: "http://oauth.example.com/token",
		ClientID:      "test-client",
		ClientSecret:  "test-secret",
	}
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
}
