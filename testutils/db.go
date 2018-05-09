package testutils

import (
	"context"
	"database/sql"
	"testing"
)

var (
	dbExecExit = struct{}{}
)

// CatchDBExecPanic catch DBExec's panic. Usually it is called in a defer.
func CatchDBExecPanic() {
	r := recover()
	if r == nil || r == dbExecExit {
		return
	}
	panic(r)
}

// DBExec execute the query and panic if something error happened. Should use CatchDBExecPanic to catch the panic.
func DBExec(t *testing.T, conn *sql.Conn, query string, args ...interface{}) {
	_, err := conn.ExecContext(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
		panic(dbExecExit)
	}
}
