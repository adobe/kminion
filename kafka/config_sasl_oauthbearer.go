package kafka

// SASLGSSAPIConfig represents the Kafka Kerberos config
type OAUTHBEARERConfig struct {
	Type     OAUTHBEARERType `koanf:"type"`
	AdobeIMS AdobeIMSConfig  `koanf:"adobeIMS"`
}

type OAUTHBEARERType string

const (
	AdobeOAUTH OAUTHBEARERType = "AdobeIMS"
)
