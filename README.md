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

**Connection string and TLS:**

`GetDataBaseURL` builds the URL with `net/url`, so a user or password holding `@`, `/`, `:`, `?`, `#` or `%` is escaped instead of breaking the URL, and an IPv6 host is bracketed. The same URL feeds both the pgx pool and `golang-migrate`, so TLS applies to migrations too.

| Variable               | Default   | Meaning                                                                                                               |
| ---------------------- | --------- | --------------------------------------------------------------------------------------------------------------------- |
| `POSTGRES_SSLMODE`     | `disable` | `disable`, `require`, `verify-ca` or `verify-full`. Anything else makes `New` fail before connecting.                 |
| `POSTGRES_SSLROOTCERT` | empty     | Path to the CA certificate the server's certificate must chain to. Refused with `disable`, since it would be ignored. |

- `disable` keeps the behaviour of v0.1.x and is right only on a trusted network: local Docker, or a private network that is already encrypted.
- `require` encrypts but accepts any certificate, so it does not stop an attacker who can intercept the connection from impersonating the server.
- `verify-full` encrypts, checks the certificate against `POSTGRES_SSLROOTCERT` and checks that it names `POSTGRES_HOST`. Use it for any database reached over the internet.
- `allow` and `prefer` are not accepted: `golang-migrate` connects with `lib/pq`, which does not support them, and both silently fall back to plaintext.

For a hosted database such as Supabase, download the provider's CA certificate, ship it with the application, and set `POSTGRES_SSLMODE=verify-full` with `POSTGRES_SSLROOTCERT` pointing at it.

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
