package kafka

import (
	"context"
	"fmt"

	"github.com/adobe/ims-go/ims"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

type AdobeIMSConfig struct {
	Code         string `koanf:"imsClientCode"`
	ClientID     string `koanf:"imsClientId"`
	ClientSecret string `koanf:"imsClientSecret"`
	Endpoint     string `koanf:"imsEndpoint"`
}

type AdobeOAUTHBearer struct {
	config AdobeIMSConfig
	client *ims.Client
	token  string
}

func NewAdobeOAUTHBearer(config AdobeIMSConfig) (*AdobeOAUTHBearer, error) {
	client, err := ims.NewClient(&ims.ClientConfig{
		URL: config.Endpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("Unable to retrieve token: %v", err)
	}

	token, err := client.Token(&ims.TokenRequest{
		Code:         config.Code,
		ClientID:     config.ClientID,
		ClientSecret: config.ClientSecret,
	})
	if err != nil {
		return nil, fmt.Errorf("Unable to get token from IMS: %v", err)
	}

	return &AdobeOAUTHBearer{
		config: config,
		client: client,
		token:  token.AccessToken,
	}, nil
}

func (a *AdobeOAUTHBearer) Opt() kgo.Opt {
	return kgo.SASL(oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		return oauth.Auth{
			Token: a.token,
		}, nil
	}))
}
