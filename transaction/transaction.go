package transaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/philippe-berto/database/postgresdb"

	"github.com/jmoiron/sqlx"
	"github.com/philippe-berto/tracer"
	otelTracer "go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/philippe-berto/database"

type (
	TX struct {
		client       *postgresdb.Client
		tracerEnable bool
	}

	TxFunc func(ctx context.Context, tx *sqlx.Tx) (interface{}, error)

	Transaction interface {
		ExecTx(ctx context.Context, h Handler) (interface{}, error)
	}

	Handler interface {
		Handle(ctx context.Context, tx *sqlx.Tx) (interface{}, error)
	}
)

func New(s *postgresdb.Client, tracerEnable bool) *TX {
	return &TX{
		tracerEnable: tracerEnable,
		client:       s,
	}
}

func (t *TX) ExecTx(ctx context.Context, h Handler) (interface{}, error) {
	spanCtx := ctx
	if t.tracerEnable {
		var span otelTracer.Span
		spanCtx, span = tracer.BaseTracer(ctx, tracerName, "", "Database: Running transaction")
		defer span.End()
	}

	tx, err := t.client.GetClient().BeginTxx(spanCtx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	res, err := h.Handle(spanCtx, tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}

		return nil, err
	}

	return res, tx.Commit()
}

func (tf TxFunc) Handle(ctx context.Context, tx *sqlx.Tx) (interface{}, error) {
	return tf(ctx, tx)
}
