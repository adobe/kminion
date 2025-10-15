package kafka

import (
	"testing"
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
