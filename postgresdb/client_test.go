//go:build integration
// +build integration

package postgresdb

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/caarlos0/env/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/philippe-berto/logger"
)

var cfg Config

func TestMain(m *testing.M) {
	os.Setenv("POSTGRES_HOST", "localhost")

	err := env.Parse(&cfg)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestConnection(t *testing.T) {

	t.Run("should be successfully connect to DB", func(t *testing.T) {
		ctx := context.Background()
		log, _ := logger.NewTestLogger()

		client, err := New(ctx, cfg, false, "", log)
		require.NoError(t, err)

		pingErr := client.Ping(ctx)
		assert.NoError(t, pingErr)
	})

	t.Run("should failed, when no database found", func(t *testing.T) {
		ctx := context.Background()
		log, _ := logger.NewTestLogger()

		cfgcopy := cfg
		cfgcopy.Host = "127.0.0.2"

		_, err := New(ctx, cfgcopy, false, "", log)
		require.Error(t, err)
		require.ErrorContains(t, err, "dial error")
	})

}
