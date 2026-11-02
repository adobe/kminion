package prometheus

import "fmt"

type Config struct {
	Host      string `koanf:"host"`
	Port      int    `koanf:"port"`
	Namespace string `koanf:"namespace"`
}

func (c *Config) SetDefaults() {
	c.Port = 8080
	c.Namespace = "kminion"
}

func (c *Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("exporter.port must be between 1 and 65535, got %d", c.Port)
	}
	return nil
}
