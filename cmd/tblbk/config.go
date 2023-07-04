package main

import (
	"crypto/ecdsa"
	"errors"
	"net/url"

	"github.com/BurntSushi/toml"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/mitchellh/mapstructure"
)

type Config struct {
	Backuper Backuper       `toml:"backuper"`
	Sinks    map[string]any `toml:"sinks"`
}

type Backuper struct {
	Dir               string
	Frequency         int
	EnableVacuum      bool
	EnableCompression bool
}

type FVM struct {
	Enabled          bool
	Web3StorageToken string
	PrivateKey       *ecdsa.PrivateKey
	Gateway          *url.URL
	ChainID          int64
	Contract         common.Address
}

type Estuary struct {
	Enabled bool
	ApiKey  string
}

func DefaultConfig() *Config {
	return &Config{
		Backuper: Backuper{
			Dir:               "backups",
			Frequency:         240,
			EnableVacuum:      true,
			EnableCompression: true,
		},
		Sinks: map[string]any{
			"fvm":     FVM{},
			"estuary": Estuary{},
		},
	}
}

func setupConfig(path string) (*Config, error) {
	conf := &Config{}
	_, err := toml.DecodeFile(path, &conf)
	if err != nil {
		return &Config{}, err
	}

	for sink, data := range conf.Sinks {
		var s any

		switch sink {
		case "estuary":
			s = Estuary{}
		case "fvm":
			type fvm struct {
				Enabled          bool
				Web3StorageToken string
				PrivateKey       string
				Gateway          string
				ChainID          int64
				Contract         string
			}
			tmpCfg := fvm{}
			if err := mapstructure.Decode(data, &tmpCfg); err != nil {
				return &Config{}, err
			}

			pk, err := crypto.HexToECDSA(tmpCfg.PrivateKey)
			if err != nil {
				return &Config{}, err
			}

			gateway, err := url.Parse(tmpCfg.Gateway)
			if err != nil {
				return &Config{}, err
			}

			if !common.IsHexAddress(tmpCfg.Contract) {
				return &Config{}, errors.New("contract is not hex address")
			}

			s = FVM{
				Enabled:          tmpCfg.Enabled,
				Web3StorageToken: tmpCfg.Web3StorageToken,
				ChainID:          tmpCfg.ChainID,
				PrivateKey:       pk,
				Gateway:          gateway,
				Contract:         common.HexToAddress(tmpCfg.Contract),
			}
		default:
			return &Config{}, errors.New("unknown sink")
		}

		conf.Sinks[sink] = s
	}

	return conf, nil
}
