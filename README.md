**to test, run**:

```
docker-compose up -d
go test -tags=integration -count=1 ./...
```

### `postgresdb/client.go`

This file implements a PostgreSQL client with connection management, configuration, and migration support.

**Key components:**

- **`Config` struct:** Holds all configuration options for connecting to a PostgreSQL database, including host, port, credentials, connection pool settings, and migration options.
- **`Client` struct:** Wraps a `sqlx.DB` instance and provides methods for interacting with the database.
- **`New` function:** Initializes a new PostgreSQL client, sets up OpenTelemetry tracing if enabled, configures connection pooling, verifies connectivity, and runs database migrations if configured.
- **Connection management:** Methods for opening, closing, and pinging the database connection.
- **Migration support:** Integrates with `golang-migrate` to run database migrations automatically on startup.
- **Utility methods:** Includes helpers for preparing SQL statements and extracting constraint identifiers from errors.

### `transaction/transaction.go`

This file implements a transaction management package for PostgreSQL using the `postgresdb.Client` as the underlying database client. It provides a structured way to execute operations within a database transaction, with optional OpenTelemetry tracing support.

**Key components:**

- **`TX` struct:** Manages transaction execution and tracing configuration.
- **`Transaction` and `Handler` interfaces:** Define abstractions for transaction execution and handling.
- **`TxFunc` type:** Allows using simple function types as transaction handlers.
- **`New` function:** Instantiates a new transaction manager.
- **`ExecTx` method:** Executes a handler within a transaction, handling commit, rollback, and tracing.
- **Tracing integration:** If enabled, wraps transaction execution in an OpenTelemetry span for observability.

This design provides a robust and extensible foundation for managing PostgreSQL connections and schema migrations in Go applications. Also, enables safe, reusable, and traceable transaction logic for PostgreSQL operations.
