package driver

import (
	"database/sql"
)

// Driver is the common interface to talk to different database systems.
type Driver interface {
	// ExtractTableNames() extract all table names in current database (schema).
	ExtractTableNames(db *sql.DB) (tableNames []string, err error)

	// ExtractColumns() extract all columns' name and type in a table.
	ExtractColumns(db *sql.DB, tableName string) (columnNames []string, columnTypes []*sql.ColumnType, err error)

	// ExtractIndexNames() extract all index (key) name for a given table.
	ExtractIndexNames(db *sql.DB, tableName string) (indexNames []string, err error)

	// ExtractIndex() extract information of a given index.
	ExtractIndex(db *sql.DB, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// ExtractFKNames() extract all foreign key constraint names for a given table.
	ExtractFKNames(db *sql.DB, tableName string) (fkNames []string, err error)

	// ExtractFK() extract information of a given foreign key constraint.
	ExtractFK(db *sql.DB, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// ScanType() returns a Go type literal suitable for scanning into using Rows.Scan.
	//
	// NOTE: Should return types in https://github.com/guregu/null no matter nullable or not.
	ScanType(typ *sql.ColumnType) (string, error)
}

// DriverWithAutoInc is Driver that support single auto increment column sematic (e.g. MySQL)
type DriverWithAutoInc interface {
	Driver

	// ExtractAutoIncColumn() extract the 'auto increament' column's name for a given table or "" if not found.
	ExtractAutoIncColumn(db *sql.DB, tableName string) (columnName string, err error)
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
