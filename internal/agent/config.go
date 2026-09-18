package agent

import (
	"fmt"
	"os"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ID        string `yaml:"id"`
	CenterURL string `yaml:"center_url"`
	Token     string `yaml:"token"`
	DataDir   string `yaml:"data_dir"`
}

func LoadConfig(path string) (*Config, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
	
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer f.Close()

	var cfg Config
	err = yaml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}
	return &cfg, nil
}

func (c *Config) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer f.Close()

	encoder := yaml.NewEncoder(f)
	encoder.SetIndent(2)
	return encoder.Encode(c)
}
