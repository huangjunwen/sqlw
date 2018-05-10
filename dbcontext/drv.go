package dbcontext

import (
	"database/sql"
)

// Drv is the common interface to talk to different database systems.
//
// NOTE: Use sql.Conn instead of sql.DB to make sure only one database connection is using.
type Drv interface {
	// ExtractQueryResultColumns returns result columns of a query.
	ExtractQueryResultColumns(conn *sql.Conn, query string) (columns []Col, err error)

	// ExtractTableNames returns all table names in current database.
	ExtractTableNames(conn *sql.Conn) (tableNames []string, err error)

	// ExtractColumns returns columns of a given table.
	ExtractColumns(conn *sql.Conn, tableName string) (columns []Col, err error)

	// ExtractIndexNames returns all index name for a given table.
	ExtractIndexNames(conn *sql.Conn, tableName string) (indexNames []string, err error)

	// ExtractIndex returns information of a given index.
	ExtractIndex(conn *sql.Conn, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// ExtractFKNames returns all foreign key constraint names for a given table.
	ExtractFKNames(conn *sql.Conn, tableName string) (fkNames []string, err error)

	// ExtractFK returns information of a given foreign key constraint.
	ExtractFK(conn *sql.Conn, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// DataTypes returns full list of driver-specific type identifiers used in Col.DataType.
	DataTypes() []string

	// Quote returns the quoted identifier.
	Quote(identifier string) string
}

// DrvWithAutoInc is Drv having single auto increment column support (e.g. MySQL)
type DrvWithAutoInc interface {
	Drv

	// ExtractAutoIncColumn returns the 'auto increament' column's name for a given table or "" if not found.
	ExtractAutoIncColumn(conn *sql.Conn, tableName string) (columnName string, err error)
}

var (
	drvs = map[string]Drv{}
)

// RegistDrv regist a driver.
func RegistDrv(name string, drv Drv) {
	drvs[name] = drv
}

// GetDrv get a Driver by its name.
func GetDrv(name string) Drv {
	return drvs[name]
}
