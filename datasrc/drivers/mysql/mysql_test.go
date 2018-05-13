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
	conn   *sql.Conn
	driver mysqlDriver
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

	// --- loadDBName ---
	{
		dbName, err := loadDBName(conn)
		assert.Error(err)
		assert.Equal("", dbName)
	}

	// --- LoadTableNames --
	{
		tableNames, err := driver.LoadTableNames(conn)
		assert.Error(err)
		assert.Len(tableNames, 0)
	}

}

func TestLoadQueryResultColumns(t *testing.T) {

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

	columns, err := driver.LoadQueryResultColumns(conn, "SELECT * FROM `types`")
	assert.NoError(err)
	for _, column := range columns {
		parts := strings.Split(column.Name, "_")
		assert.Len(parts, 3)
		assert.Equal(parts[2], column.DataType, "DataType error for column %+q", column.Name)
	}

}

func TestLoadTableInfo(t *testing.T) {

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

	// --- loadDBName ---
	{
		dbName, err := loadDBName(conn)
		assert.NoError(err)
		assert.Equal("testing2", dbName)
	}

	// --- LoadTableNames ---
	{
		tableNames, err := driver.LoadTableNames(conn)
		assert.NoError(err)
		assert.Len(tableNames, 4)
	}

	// --- LoadColumns ---
	{
		columns, err := driver.LoadColumns(conn, "where")
		assert.NoError(err)
		assert.Len(columns, 2)
		assert.Equal("and", columns[0].Name)
		assert.Equal("or", columns[1].Name)
	}

	// -- LoadAutoIncColumn ---
	{
		{
			columnName, err := driver.LoadAutoIncColumn(conn, "group")
			assert.NoError(err)
			assert.Equal("by", columnName)
		}

		{
			columnName, err := driver.LoadAutoIncColumn(conn, "where")
			assert.NoError(err)
			assert.Equal("", columnName)
		}
	}

	// -- LoadIndexNames ---
	{
		{
			indexNames, err := driver.LoadIndexNames(conn, "from")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("index", indexNames[0])
		}

		{
			indexNames, err := driver.LoadIndexNames(conn, "where")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("index", indexNames[0])
		}

		{
			indexNames, err := driver.LoadIndexNames(conn, "group")
			assert.NoError(err)
			assert.Len(indexNames, 1)
			assert.Equal("PRIMARY", indexNames[0])
		}

		{
			// NOTE: implicit key for foreign key
			indexNames, err := driver.LoadIndexNames(conn, "order")
			assert.NoError(err)
			assert.Len(indexNames, 1)
		}

	}

	// -- LoadIndex ---
	{
		{
			columnNames, isPrimary, isUnique, err := driver.LoadIndex(conn, "from", "index")
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("join", columnNames[0])
			assert.False(isPrimary)
			assert.False(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := driver.LoadIndex(conn, "where", "index")
			assert.NoError(err)
			assert.Len(columnNames, 2)
			assert.Equal("or", columnNames[0])
			assert.Equal("and", columnNames[1])
			assert.False(isPrimary)
			assert.True(isUnique)
		}

		{
			columnNames, isPrimary, isUnique, err := driver.LoadIndex(conn, "group", "PRIMARY")
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
			assert.True(isPrimary)
			assert.True(isUnique)
		}

		{
			indexNames, _ := driver.LoadIndexNames(conn, "order")
			columnNames, isPrimary, isUnique, err := driver.LoadIndex(conn, "order", indexNames[0])
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
			assert.False(isPrimary)
			assert.False(isUnique)
		}
	}

	// -- LoadFKNames ---
	{
		{
			fkNames, err := driver.LoadFKNames(conn, "from")
			assert.NoError(err)
			assert.Len(fkNames, 0)
		}

		{
			fkNames, err := driver.LoadFKNames(conn, "order")
			assert.NoError(err)
			assert.Len(fkNames, 1)
		}

	}

	// --- LoadFK ---
	{
		{
			fkNames, _ := driver.LoadFKNames(conn, "order")
			columnNames, refTableName, refColumnNames, err := driver.LoadFK(conn, "order", fkNames[0])
			assert.NoError(err)
			assert.Len(columnNames, 1)
			assert.Equal("by", columnNames[0])
			assert.Equal("group", refTableName)
			assert.Len(refColumnNames, 1)
			assert.Equal("by", refColumnNames[0])
		}
	}

}
