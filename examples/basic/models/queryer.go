package models

import (
	"context"
	"database/sql"
)

// Execer is the common interface to execute a query without returning any rows.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Queryer is the common interface to execute a query returning row(s).
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}
