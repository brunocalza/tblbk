package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApp(t *testing.T) {
	var cfg *Config
	app, err := NewApp(cfg, "")

	require.NoError(t, err)
	require.Equal(t, app, &App{})
	app.Run()
}
