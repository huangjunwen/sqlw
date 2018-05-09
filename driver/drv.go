package driver

import (
	"database/sql"
)

// Column represents query result column.
type Column struct {
	// ColumnName is the name of the column.
	ColumnName string

	// DataType is a driver-specific type identifier (ignore nullable), such as:
	// - uint24
	// - json
	// - time
	// It is used in scan type mapping.
	DataType string

	// Nullable is true if the column can have NULL value.
	Nullable bool
}

// Drv is the common interface to talk to different database systems.
type Drv interface {
	// ExtractQuery returns result columns of a query.
	ExtractQuery(conn *sql.Conn, query string) (columns []Column, err error)

	// ExtractTableNames extracts all table names in current database (schema).
	ExtractTableNames(conn *sql.Conn) (tableNames []string, err error)

	// ExtractColumns extracts columns of a given table.
	ExtractColumns(conn *sql.Conn, tableName string) (columns []Column, err error)

	// ExtractIndexNames extracts all index (key) name for a given table.
	ExtractIndexNames(conn *sql.Conn, tableName string) (indexNames []string, err error)

	// ExtractIndex extracts information of a given index.
	ExtractIndex(conn *sql.Conn, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// ExtractFKNames extracts all foreign key constraint names for a given table.
	ExtractFKNames(conn *sql.Conn, tableName string) (fkNames []string, err error)

	// ExtractFK extracts information of a given foreign key constraint.
	ExtractFK(conn *sql.Conn, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// DataTypes list full list of driver-specific type identifiers.
	DataTypes() []string

	// Quote returns the quoted identifier.
	Quote(identifier string) string
}

// DrvWithAutoInc is Drv having single auto increment column support (e.g. MySQL)
type DrvWithAutoInc interface {
	Drv

	// ExtractAutoIncColumn() extract the 'auto increament' column's name for a given table or "" if not found.
	ExtractAutoIncColumn(conn *sql.Conn, tableName string) (columnName string, err error)
}

var (
	drvs = map[string]Drv{}
)

// RegistDrv regist a driver.
func RegistDrv(driverName string, drv Drv) {
	drvs[driverName] = drv
}

// GetDrv get a Driver by its name.
func GetDrv(driverName string) Drv {
	return drvs[driverName]
}
