package datasrc

import (
	"database/sql"
)

// Driver is the low level interface of loader talking to different databases.
//
// NOTE: Use sql.Conn instead of sql.DB to make sure only one database connection is using.
type Driver interface {
	// LoadQueryResultColumns returns result columns of a query.
	LoadQueryResultColumns(conn *sql.Conn, query string) (columns []*Column, err error)

	// LoadTableNames returns all table names in current database.
	LoadTableNames(conn *sql.Conn) (tableNames []string, err error)

	// LoadTableColumns returns columns of a given table.
	LoadTableColumns(conn *sql.Conn, tableName string) (tableColumns []*TableColumn, err error)

	// LoadIndexNames returns all index name for a given table.
	LoadIndexNames(conn *sql.Conn, tableName string) (indexNames []string, err error)

	// LoadIndex returns information of a given index.
	LoadIndex(conn *sql.Conn, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// LoadFKNames returns all foreign key constraint names for a given table.
	LoadFKNames(conn *sql.Conn, tableName string) (fkNames []string, err error)

	// LoadFK returns information of a given foreign key constraint.
	LoadFK(conn *sql.Conn, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// DataTypes returns full list of driver-specific type identifiers used in Column.DataType.
	DataTypes() []string

	// Quote returns the quoted identifier.
	Quote(identifier string) string
}

// DriverWithAutoInc is Driver having single auto increment column support (e.g. MySQL)
type DriverWithAutoInc interface {
	Driver

	// LoadAutoIncColumn returns the 'auto increament' column's name for a given table or "" if not found.
	LoadAutoIncColumn(conn *sql.Conn, tableName string) (columnName string, err error)
}

var (
	drivers = map[string]Driver{}
)

// RegistDriver regist a driver.
func RegistDriver(driverName string, driver Driver) {
	drivers[driverName] = driver
}

// GetDriver get a Driver by its name.
func GetDriver(driverName string) Driver {
	return drivers[driverName]
}
