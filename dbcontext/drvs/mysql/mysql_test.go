package mysql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/huangjunwen/sqlw/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	conn *sql.Conn
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

func TestExtractQueryResultColumns(t *testing.T) {

	assert := assert.New(t)

	defer testutils.CatchDBExecPanic()
	exec := func(query string, args ...interface{}) {
		testutils.DBExec(t, conn, query, args...)
	}

	exec("CREATE DATABASE testing1")
	defer func() {
		conn.ExecContext(context.Background(), "DROP DATABASE testing1")
	}()
	exec("USE testing1")

	exec("" +
		"CREATE TABLE `types` (" +
		" `float_z_float32` FLOAT NOT NULL, " +
		" `float_n_float32` FLOAT, " +
		" `double_z_float64` DOUBLE NOT NULL, " +
		" `double_n_float64` DOUBLE, " +
		" `bool_z_bool` BOOL NOT NULL, " +
		" `bool_n_bool` BOOL, " +
		" `tiny_z_int8` TINYINT NOT NULL, " +
		" `tiny_n_int8` TINYINT, " +
		" `utiny_z_uint8` TINYINT UNSIGNED NOT NULL, " +
		" `utiny_n_uint8` TINYINT UNSIGNED, " +
		" `small_z_int16` SMALLINT NOT NULL, " +
		" `small_n_int16` SMALLINT, " +
		" `usmall_z_uint16` SMALLINT UNSIGNED NOT NULL, " +
		" `usmall_n_uint16` SMALLINT UNSIGNED, " +
		" `medium_z_int32` MEDIUMINT NOT NULL, " +
		" `medium_n_int32` MEDIUMINT, " +
		" `umedium_z_uint32` MEDIUMINT UNSIGNED NOT NULL, " +
		" `umedium_n_uint32` MEDIUMINT UNSIGNED, " +
		" `int_z_int32` INT NOT NULL, " +
		" `int_n_int32` INT, " +
		" `uint_z_uint32` INT UNSIGNED NOT NULL, " +
		" `uint_n_uint32` INT UNSIGNED, " +
		" `big_z_int64` BIGINT NOT NULL, " +
		" `big_n_int64` BIGINT, " +
		" `ubig_z_uint64` BIGINT UNSIGNED NOT NULL, " +
		" `ubig_n_uint64` BIGINT UNSIGNED, " +
		" `datetime_z_time` DATETIME NOT NULL, " +
		" `datetime_n_time` DATETIME, " +
		" `date_z_time` DATE NOT NULL, " +
		" `date_n_time` DATE, " +
		" `timestamp_z_time` TIMESTAMP NOT NULL DEFAULT NOW(), " +
		//" `timestamp_n_time` TIMESTAMP, " + // TODO
		" `bit_z_bit` BIT(10) NOT NULL, " +
		" `bit_n_bit` BIT(10), " +
		" `json_z_json` JSON NOT NULL, " +
		" `json_n_json` JSON, " +
		" `char_z_string` CHAR(32) NOT NULL, " +
		" `char_n_string` CHAR(32), " +
		" `vchar_z_string` VARCHAR(32) NOT NULL, " +
		" `vchar_n_string` VARCHAR(32), " +
		" `text_z_string` TEXT NOT NULL, " +
		" `text_n_string` TEXT, " +
		" `blob_z_string` BLOB NOT NULL, " +
		" `blob_n_string` BLOB " +
		")")

	columns, err := drv.ExtractQueryResultColumns(conn, "SELECT * FROM `types`")
	assert.NoError(err)
	for _, column := range columns {
		parts := strings.Split(column.ColumnName, "_")
		assert.Len(parts, 3)
		nullable := parts[1] == "n"
		assert.Equal(nullable, column.Nullable, "Nullable error for column %+q", column.ColumnName)
		assert.Equal(parts[2], column.DataType, "DataType error for column %+q", column.ColumnName)
	}

}

func TestExtractTableInfo(t *testing.T) {

	assert := assert.New(t)

	defer testutils.CatchDBExecPanic()
	exec := func(query string, args ...interface{}) {
		testutils.DBExec(t, conn, query, args...)
	}

	exec("CREATE DATABASE testing2")
	defer func() {
		conn.ExecContext(context.Background(), "DROP DATABASE testing2")
	}()
	exec("USE testing2")

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
		assert.NoError(err)
		assert.Equal("testing2", dbName)
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
