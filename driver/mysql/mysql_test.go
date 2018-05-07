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
	drv  mysqlDrv
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

func TestNoDBSelected(t *testing.T) {

	assert := assert.New(t)

	defer testutils.CatchDBExecPanic()
	exec := func(query string, args ...interface{}) {
		testutils.DBExec(t, conn, query, args...)
	}

	exec("CREATE DATABASE notexist")
	exec("USE notexist")
	exec("DROP DATABASE notexist")

	// --- extractDBName ---
	{
		dbName, err := extractDBName(conn)
		assert.Equal("", dbName)
		assert.NoError(err)
	}

	// --- ExtractTableNames --
	{
		tableNames, err := drv.ExtractTableNames(conn)
		assert.Len(tableNames, 0)
		assert.Error(err)
	}

}

func TestExtractTableInfo(t *testing.T) {

	assert := assert.New(t)

	defer testutils.CatchDBExecPanic()
	exec := func(query string, args ...interface{}) {
		testutils.DBExec(t, conn, query, args...)
	}

	exec("CREATE DATABASE testing")
	defer func() {
		conn.Exec("DROP DATABASE testing")
	}()
	exec("USE testing")

	// NOTE: use keyworkds as identifiers to test quoting
	exec("" +
		"CREATE TABLE `from` (" +
		" `join` VARCHAR(128), " +
		" KEY `index` (`join`)" +
		")")

	exec("" +
		"CREATE TABLE `where` (" +
		" `and` DATETIME DEFAULT NOW(), " +
		"	`or` FLOAT, " +
		" UNIQUE KEY `index` (`or`, `and`)" +
		")")

	exec("" +
		"CREATE TABLE `group` (" +
		"	`by` INT AUTO_INCREMENT, " +
		"	PRIMARY KEY (`by`)" +
		")")

	exec("" +
		"CREATE TABLE `order` (" +
		"	`by` INT, " +
		" FOREIGN KEY (`by`) REFERENCES `group` (`by`)" +
		")")

	// --- extractDBName ---
	{
		dbName, err := extractDBName(conn)
		assert.Equal("testing", dbName)
		assert.NoError(err)
	}

	// --- ExtractTableNames ---
	{
		tableNames, err := drv.ExtractTableNames(conn)
		assert.NoError(err)
		assert.Len(tableNames, 4)
	}

	// --- ExtractColumns ---
	{
		columnNames, columnTypes, err := drv.ExtractColumns(conn, "where")
		assert.NoError(err)
		assert.Len(columnNames, 2)
		assert.Len(columnTypes, 2)
		assert.Equal(columnNames[0], "and")
		assert.Equal(columnNames[1], "or")
	}

	// -- ExtractAutoIncColumn ---
	{
		{
			columnName, err := drv.ExtractAutoIncColumn(conn, "group")
			assert.NoError(err)
			assert.Equal(columnName, "by")
		}

		{
			columnName, err := drv.ExtractAutoIncColumn(conn, "where")
			assert.NoError(err)
			assert.Equal(columnName, "")
		}
	}

	// -- ExtractIndexNames ---
	{
		{
			indexNames, err := drv.ExtractIndexNames(conn, "from")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal(indexNames[0], "index")
		}

		{
			indexNames, err := drv.ExtractIndexNames(conn, "where")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal(indexNames[0], "index")
		}

		{
			indexNames, err := drv.ExtractIndexNames(conn, "group")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal(indexNames[0], "PRIMARY")
		}

		{
			// NOTE: implicit key for foreign key
			indexNames, err := drv.ExtractIndexNames(conn, "order")
			assert.NoError(err)
			assert.Len(indexNames, 1)
		}

	}

	// -- ExtractIndex ---
	{
		{
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "from", "index")
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal(columnNames[0], "join")
			assert.False(isPrimary)
			assert.False(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "where", "index")
			assert.NoError(err)
			assert.Len(columnNames, 2)
			assert.Equal(columnNames[0], "or")
			assert.Equal(columnNames[1], "and")
			assert.False(isPrimary)
			assert.True(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "group", "PRIMARY")
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal(columnNames[0], "by")
			assert.True(isPrimary)
			assert.True(isUnique)
		}

		{
			// NOTE: Already tested before, so skip it
			indexNames, _ := drv.ExtractIndexNames(conn, "order")
			indexName := indexNames[0]

			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "order", indexName)
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal(columnNames[0], "by")
			assert.False(isPrimary)
			assert.False(isUnique)
		}
	}

	// -- ExtractFKNames ---
	{
		{
			fkNames, err := drv.ExtractFKNames(conn, "from")
			assert.NoError(err)
			assert.Len(fkNames, 0)
		}

		{
			fkNames, err := drv.ExtractFKNames(conn, "order")
			assert.NoError(err)
			assert.Len(fkNames, 1)
		}

	}

	// --- ExtractFK ---
	{
		{
			fkNames, _ := drv.ExtractFKNames(conn, "order")
			fkName := fkNames[0]
			columnNames, refTableName, refColumnNames, err := drv.ExtractFK(conn, "order", fkName)
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal(columnNames[0], "by")
			assert.Equal(refTableName, "group")
			assert.Len(refColumnNames, 1)
			assert.Equal(refColumnNames[0], "by")
		}
	}

}
