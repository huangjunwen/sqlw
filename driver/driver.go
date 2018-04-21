package driver

import (
	"database/sql"
)

// Driver is the common interface to talk to different database systems.
type Driver interface {
	// ExtractTableNames() extract all table names in current database (schema).
	ExtractTableNames(conn *sql.DB) (tableNames []string, err error)

	// ExtractColumns() extract all columns' name and type in a table.
	ExtractColumns(conn *sql.DB, tableName string) (columnNames []string, columnTypes []*sql.ColumnType, err error)

	// ExtractIndexNames() extract all index (key) name for a given table.
	ExtractIndexNames(conn *sql.DB, tableName string) (indexNames []string, err error)

	// ExtractIndex() extract information of a given index.
	ExtractIndex(conn *sql.DB, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// ExtractFKNames() extract all foreign key constraint names for a given table.
	ExtractFKNames(conn *sql.DB, tableName string) (fkNames []string, err error)

	// ExtractFK() extract information of a given foreign key constraint.
	ExtractFK(conn *sql.DB, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// PrimitiveScanType() returns go primitive scanning type (ignore nullable) of a column type. Must be one of:
	// - int/uint/int8/uint8/int16/uint16/int32/uint32/int64/uint64
	// - float32/float64
	// - bool
	// - []byte/string
	// - time.Time
	PrimitiveScanType(typ *sql.ColumnType) (string, error)
}

// DriverWithAutoInc is Driver that support single auto increment column sematic (e.g. MySQL)
type DriverWithAutoInc interface {
	Driver

	// ExtractAutoIncColumn() extract the 'auto increament' column's name for a given table or "" if not found.
	ExtractAutoIncColumn(conn *sql.DB, tableName string) (columnName string, err error)
}

var (
	drivers = map[string]Driver{}
)

// RegistDriver() regist a driver.
func RegistDriver(driverName string, driver Driver) {
	drivers[driverName] = driver
}

// GetDriver() get a Driver by its name.
func GetDriver(driverName string) Driver {
	return drivers[driverName]
}
