package postgresdb

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseConfig() Config {
	return Config{
		Host:     "db.example.com",
		Name:     "cryple",
		User:     "app",
		Password: "secret",
		Port:     5432,
		Driver:   "postgres",
		SSLMode:  "disable",
	}
}

func TestGetDataBaseURL(t *testing.T) {
	t.Run("keeps sslmode disable by default", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Timeout = 5

		assert.Equal(t, "postgres://app:secret@db.example.com:5432/cryple?connect_timeout=5&sslmode=disable", cfg.GetDataBaseURL())
	})

	t.Run("carries sslmode and sslrootcert", func(t *testing.T) {
		cfg := baseConfig()
		cfg.SSLMode = "verify-full"
		cfg.SSLRootCert = "/etc/ssl/supabase/prod-ca-2021.crt"

		parsed, err := url.Parse(cfg.GetDataBaseURL())
		require.NoError(t, err)

		assert.Equal(t, "verify-full", parsed.Query().Get("sslmode"))
		assert.Equal(t, "/etc/ssl/supabase/prod-ca-2021.crt", parsed.Query().Get("sslrootcert"))
	})

	t.Run("escapes credentials with reserved characters", func(t *testing.T) {
		cfg := baseConfig()
		cfg.User = "postgres.projectref"
		cfg.Password = "p@ss/w:rd?#%"

		parsed, err := url.Parse(cfg.GetDataBaseURL())
		require.NoError(t, err)

		password, _ := parsed.User.Password()
		assert.Equal(t, "postgres.projectref", parsed.User.Username())
		assert.Equal(t, "p@ss/w:rd?#%", password)
		assert.Equal(t, "db.example.com:5432", parsed.Host)
		assert.Equal(t, "/cryple", parsed.Path)
	})

	t.Run("brackets an IPv6 host", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Host = "2001:db8::1"

		parsed, err := url.Parse(cfg.GetDataBaseURL())
		require.NoError(t, err)

		assert.Equal(t, "2001:db8::1", parsed.Hostname())
		assert.Equal(t, "5432", parsed.Port())
	})
}

func TestValidate(t *testing.T) {
	for _, mode := range SupportedSSLModes {
		cfg := baseConfig()
		cfg.SSLMode = mode

		assert.NoError(t, cfg.Validate(), mode)
	}

	t.Run("rejects an unsupported sslmode", func(t *testing.T) {
		cfg := baseConfig()
		cfg.SSLMode = "prefer"

		assert.ErrorContains(t, cfg.Validate(), "unsupported POSTGRES_SSLMODE")
	})

	t.Run("rejects a root certificate without TLS", func(t *testing.T) {
		cfg := baseConfig()
		cfg.SSLRootCert = "/etc/ssl/ca.crt"

		assert.ErrorContains(t, cfg.Validate(), "POSTGRES_SSLROOTCERT")
	})
}
