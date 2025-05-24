package postgresdb

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	l "github.com/philippe-berto/logger"
	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
)

type (
	Config struct {
		Host           string `env:"POSTGRES_HOST,required"`
		Name           string `env:"POSTGRES_DB"        envDefault:"test"`
		Password       string `env:"POSTGRES_PASSWORD"  envDefault:"postgres"`
		User           string `env:"POSTGRES_USER"      envDefault:"postgres"`
		Port           int    `env:"POSTGRES_PORT"      envDefault:"5432"`
		Driver         string `env:"POSTGRES_DRIVER"    envDefault:"postgres"`
		Timeout        int    `env:"POSTGRES_TIMEOUT"         envDefault:"5"`
		IdleConnection int    `env:"POSTGRES_IDLE_CONNECTION" envDefault:"0"`
		LifeTime       int    `env:"POSTGRES_LIFE_TIME"       envDefault:"0"`
		OpenConnection int    `env:"POSTGRES_OPEN_CONNECTION" envDefault:"0"`
		RunMigration   bool   `env:"POSTGRES_MIGRATION"       envDefault:"1"`
	}

	Client struct {
		client *sqlx.DB
		log    *l.Logger
	}
)

func New(ctx context.Context, cfg Config, tracerEnable bool, migrationLocation string, log *l.Logger) (*Client, error) {
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
			log.WithFields(l.Fields{"error": err.Error()}).
				Error("Database: otelsql driver")

			return nil, err
		}
	}

	db, err := sqlx.Open(driverName, databaseURL)
	if err != nil {
		log.WithFields(l.Fields{"error": err.Error()}).
			Error("Database: fail to open connection")

		return nil, err
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
		log.WithFields(l.Fields{"error": err.Error()}).
			Error("Database: Could not ping the database")

		return nil, err
	}

	migrations, err := runMigration(cfg, migrationLocation, log)
	if err != nil {
		return nil, err
	}

	if err = closeMigration(migrations, log); err != nil {
		return nil, err
	}

	s := &Client{
		client: db,
		log:    log,
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

func runMigration(cfg Config, migrationLocation string, log *l.Logger) (*migrate.Migrate, error) {
	if !cfg.RunMigration || migrationLocation == "" {
		return nil, nil
	}

	log.Info("Running migration")
	m, err := migrate.New(migrationLocation, cfg.GetDataBaseURL())
	if err != nil {
		log.WithFields(l.Fields{"error": err.Error()}).
			Error("Database: Error setting up migration connection")

		return nil, err
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange && err != migrate.ErrNilVersion {
		log.WithFields(l.Fields{"error": err.Error()}).
			Error("Database: Error running migration")

		return nil, err
	}

	return m, nil
}

func closeMigration(migrations *migrate.Migrate, log *l.Logger) error {
	if migrations == nil {
		return nil
	}

	sourceErr, dbErr := migrations.Close()
	if sourceErr != nil {
		log.WithFields(l.Fields{"error": sourceErr.Error()}).
			Error("Database: Error close migration source")

		return fmt.Errorf("database: failed to close migration source")
	}

	if dbErr != nil {
		log.WithFields(l.Fields{"error": dbErr.Error()}).
			Error("Database: Error close migration database")

		return fmt.Errorf("database: failed to close migration db")
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
