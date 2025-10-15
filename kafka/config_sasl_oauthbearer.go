package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// OAuthBearerConfig represents the OAUTHBEARER SASL config with support for different providers
type OAuthBearerConfig struct {
	// Type of OAuth provider. Valid values: "AdobeIMS" (leave empty for generic OAuth)
	Type OAuthBearerType `koanf:"type"`

	// Generic OAuth configuration (used when Type is empty)
	TokenEndpoint string `koanf:"tokenEndpoint"`
	ClientID      string `koanf:"clientId"`
	ClientSecret  string `koanf:"clientSecret"`
	Scope         string `koanf:"scope"`

	// Additional provider-specific configuration
	Additional OAuthAdditionalConfig `koanf:"additional"`
}

// OAuthAdditionalConfig holds provider-specific OAuth configuration
type OAuthAdditionalConfig struct {
	// Adobe IMS specific fields (used when Type is "AdobeIMS")
	ClientCode string `koanf:"clientCode"`
}

// same as AcquireToken in Console https://github.com/redpanda-data/console/blob/master/backend/pkg/config/kafka_sasl_oauth.go#L56
func (c *OAuthBearerConfig) getToken(ctx context.Context) (string, error) {
	authHeaderValue := base64.StdEncoding.EncodeToString([]byte(c.ClientID + ":" + c.ClientSecret))

	queryParams := url.Values{
		"grant_type": []string{"client_credentials"},
		"scope":      []string{c.Scope},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.TokenEndpoint, strings.NewReader(queryParams.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.URL.RawQuery = queryParams.Encode()

	req.Header.Set("Authorization", "Basic "+authHeaderValue)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed with status code %d", resp.StatusCode)
	}

	var tokenResponse map[string]interface{}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&tokenResponse); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}

	accessToken, ok := tokenResponse["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("access_token not found in token response")
	}

	return accessToken, nil
}

// Validate validates the OAuthBearerConfig
func (c *OAuthBearerConfig) Validate() error {
	// Common validation for all OAuth types
	if c.TokenEndpoint == "" {
		return fmt.Errorf("OAuthBearer token endpoint is not specified")
	}
	if c.ClientID == "" || c.ClientSecret == "" {
		return fmt.Errorf("OAuthBearer client credentials are not specified")
	}

	// Type-specific validation
	switch c.Type {
	case OAuthBearerTypeAdobeIMS:
		// Adobe IMS specific validation
		if c.Additional.ClientCode == "" {
			return fmt.Errorf("OAuthBearer Adobe IMS client code is not specified")
		}
	case "":
		// Generic OAuth - no additional validation needed
	default:
		return fmt.Errorf("unknown OAuthBearer type '%s'", c.Type)
	}

	return nil
}

// OAuthBearerType represents the type of OAuth provider
type OAuthBearerType string

const (
	// OAuthBearerTypeAdobeIMS represents Adobe IMS OAuth provider
	OAuthBearerTypeAdobeIMS OAuthBearerType = "AdobeIMS"
)
