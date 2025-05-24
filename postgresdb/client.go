package postgresdb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
)

type (
	Config struct {
		Host           string `env:"POSTGRES_HOST"            envDefault:"localhost"`
		Name           string `env:"POSTGRES_DB"              envDefault:"test"`
		Password       string `env:"POSTGRES_PASSWORD"        envDefault:"postgres"`
		User           string `env:"POSTGRES_USER"            envDefault:"postgres"`
		Port           int    `env:"POSTGRES_PORT"            envDefault:"5432"`
		Driver         string `env:"POSTGRES_DRIVER"          envDefault:"postgres"`
		Timeout        int    `env:"POSTGRES_TIMEOUT"         envDefault:"5"`
		IdleConnection int    `env:"POSTGRES_IDLE_CONNECTION" envDefault:"0"`
		LifeTime       int    `env:"POSTGRES_LIFE_TIME"       envDefault:"0"`
		OpenConnection int    `env:"POSTGRES_OPEN_CONNECTION" envDefault:"0"`
		RunMigration   bool   `env:"POSTGRES_MIGRATION"       envDefault:"1"`
	}

	Client struct {
		client *sqlx.DB
	}
)

func New(ctx context.Context, cfg Config, tracerEnable bool, migrationLocation string) (*Client, error) {
	databaseURL := cfg.GetDataBaseURL()

	driverName := "pgx"
	if tracerEnable {
		var err error
		driverName, err = otelsql.Register(cfg.Driver,
			otelsql.AllowRoot(),
			otelsql.TraceQueryWithoutArgs(),
			otelsql.WithSystem(semconv.DBSystemPostgreSQL),
		)
		if err != nil {

			return nil, fmt.Errorf("database: failed to register otelsql driver: %v", err)
		}
	}

	db, err := sqlx.Open(driverName, databaseURL)
	if err != nil {

		return nil, fmt.Errorf("database: failed to open connection: %v", err)
	}

	if cfg.OpenConnection != 0 {
		db.SetMaxOpenConns(cfg.OpenConnection)
	}

	if cfg.IdleConnection != 0 {
		db.SetMaxIdleConns(cfg.IdleConnection)
	}

	if cfg.LifeTime != 0 {
		db.SetConnMaxLifetime(time.Duration(1) * time.Millisecond)
	}

	if err = db.PingContext(ctx); err != nil {

		return nil, fmt.Errorf("database: failed to ping database: %v", err)
	}

	migrations, err := runMigration(cfg, migrationLocation)
	if err != nil {
		return nil, err
	}

	if err = closeMigration(migrations); err != nil {
		return nil, err
	}

	s := &Client{
		client: db,
	}

	return s, nil
}

func (c *Client) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("database:Failed to close connection: %v", err)
	}

	return nil
}

func (c *Client) GetClient() *sqlx.DB {
	return c.client
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.PingContext(ctx)
}

func (c *Client) PrepareStatement(query string) (*sqlx.Stmt, error) {
	return c.client.Preparex(query)
}

func (cfg Config) GetDataBaseURL() string {
	baseURL := fmt.Sprintf("%s://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Driver, cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)

	if cfg.Timeout != 0 {
		baseURL += fmt.Sprintf("&connect_timeout=%d", cfg.Timeout)
	}

	return baseURL
}

func runMigration(cfg Config, migrationLocation string) (*migrate.Migrate, error) {
	if !cfg.RunMigration || migrationLocation == "" {
		return nil, nil
	}

	log.Println("Running migration")
	m, err := migrate.New(migrationLocation, cfg.GetDataBaseURL())
	if err != nil {

		return nil, fmt.Errorf("database: failed to create migration instance: %v", err)
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange && err != migrate.ErrNilVersion {

		return nil, fmt.Errorf("database: failed to run migration: %v", err)
	}

	return m, nil
}

func closeMigration(migrations *migrate.Migrate) error {
	if migrations == nil {
		return nil
	}

	sourceErr, dbErr := migrations.Close()
	if sourceErr != nil {
		return fmt.Errorf("database: failed to close migration source: %v", sourceErr)
	}
	if dbErr != nil {
		return fmt.Errorf("database: failed to close migration db: %v", dbErr)
	}

	return nil
}

func GetConstraintIdentifier(err error) string {
	if pqErr, ok := err.(*pgconn.PgError); ok {
		return pqErr.ConstraintName
	}

	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Constraint
	}

	return ""
}
