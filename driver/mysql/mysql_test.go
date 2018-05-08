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
		assert.Error(err)
		assert.Equal("", dbName)
	}

	// --- ExtractTableNames --
	{
		tableNames, err := drv.ExtractTableNames(conn)
		assert.Error(err)
		assert.Len(tableNames, 0)
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
		columns, err := drv.ExtractColumns(conn, "where")
		assert.NoError(err)
		assert.Len(columns, 2)
		assert.Equal("and", columns[0].ColumnName)
		assert.Equal("or", columns[1].ColumnName)
	}

	// -- ExtractAutoIncColumn ---
	{
		{
			columnName, err := drv.ExtractAutoIncColumn(conn, "group")
			assert.NoError(err)
			assert.Equal("by", columnName)
		}

		{
			columnName, err := drv.ExtractAutoIncColumn(conn, "where")
			assert.NoError(err)
			assert.Equal("", columnName)
		}
	}

	// -- ExtractIndexNames ---
	{
		{
			indexNames, err := drv.ExtractIndexNames(conn, "from")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("index", indexNames[0])
		}

		{
			indexNames, err := drv.ExtractIndexNames(conn, "where")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("index", indexNames[0])
		}

		{
			indexNames, err := drv.ExtractIndexNames(conn, "group")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("PRIMARY", indexNames[0])
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
			assert.Equal("join", columnNames[0])
			assert.False(isPrimary)
			assert.False(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "where", "index")
			assert.NoError(err)
			assert.Len(columnNames, 2)
			assert.Equal("or", columnNames[0])
			assert.Equal("and", columnNames[1])
			assert.False(isPrimary)
			assert.True(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "group", "PRIMARY")
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
			assert.True(isPrimary)
			assert.True(isUnique)
		}

		{
			indexNames, _ := drv.ExtractIndexNames(conn, "order")
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, "order", indexNames[0])
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
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
			columnNames, refTableName, refColumnNames, err := drv.ExtractFK(conn, "order", fkNames[0])
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
			assert.Equal("group", refTableName)
			assert.Len(refColumnNames, 1)
			assert.Equal("by", refColumnNames[0])
		}
	}

}

/*

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

	exec("" +
		"CREATE TABLE `c` (" +
		" `f32` FLOAT, " +
		" `f64` DOUBLE, " +
		" `b` BOOL, " +
		" `i8` TINYINT, " +
		" `ui8` TINYINT UNSIGNED, " +
		" `i16` SMALLINT, " +
		" `ui16` SMALLINT UNSIGNED, " +
		" `i24` MEDIUMINT, " +
		" `ui24` MEDIUMINT UNSIGNED, " +
		" `i32` INT, " +
		" `ui32` INT UNSIGNED, " +
		" `i64` BIGINT, " +
		" `ui64` BIGINT UNSIGNED, " +
		" KEY `index` (`join`)" +
		")")

}

*/
