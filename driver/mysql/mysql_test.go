package mysql

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/huangjunwen/sqlw/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	conn *sql.DB
)

func TestMain(m *testing.M) {

	var rc int

	testutils.Chain(
		func(ctx context.Context) error {
			conn = testutils.MysqlConn(ctx)
			rc = m.Run()
			return nil
		},
		testutils.WithMysqlConn,
	)(context.Background())

	os.Exit(rc)

}

func TestMysqlDrv(t *testing.T) {

	assert := assert.New(t)

	defer testutils.CatchDBExecPanic()
	exec := func(query string, args ...interface{}) {
		testutils.DBExec(t, conn, query, args...)
	}

	// --- test extractDBName ---
	exec("CREATE DATABASE xxxxxxxxx")
	exec("USE xxxxxxxxx")
	exec("DROP DATABASE xxxxxxxxx")

	dbName, err := extractDBName(conn)
	assert.Equal("", dbName, "Expect db name is empty when there is no database selected")
	assert.NoError(err)

	exec("CREATE DATABASE testing")
	defer func() {
		conn.Exec("DROP DATABASE testing")
	}()
	exec("USE testing")

	dbName, err = extractDBName(conn)
	assert.Equal("testing", dbName, "Expect db name is not empty when there is a database selected")
	assert.NoError(err)

}
