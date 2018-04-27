package driver

import (
	"database/sql"
)

// Drv is the common interface to talk to different database systems.
type Drv interface {
	// ExtractTableNames extracts all table names in current database (schema).
	ExtractTableNames(conn *sql.DB) (tableNames []string, err error)

	// ExtractColumns extracts all columns' name and type in a table.
	ExtractColumns(conn *sql.DB, tableName string) (columnNames []string, columnTypes []*sql.ColumnType, err error)

	// ExtractIndexNames extracts all index (key) name for a given table.
	ExtractIndexNames(conn *sql.DB, tableName string) (indexNames []string, err error)

	// ExtractIndex extracts information of a given index.
	ExtractIndex(conn *sql.DB, tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error)

	// ExtractFKNames extracts all foreign key constraint names for a given table.
	ExtractFKNames(conn *sql.DB, tableName string) (fkNames []string, err error)

	// ExtractFK extracts information of a given foreign key constraint.
	ExtractFK(conn *sql.DB, tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error)

	// PrimitiveScanType returns go primitive scanning type (ignore nullable) of a column type. Must be one of:
	// - int/uint/int8/uint8/int16/uint16/int32/uint32/int64/uint64
	// - float32/float64
	// - bool
	// - []byte/string
	// - time.Time
	PrimitiveScanType(typ *sql.ColumnType) (string, error)
}

// DrvWithAutoInc is Drv having single auto increment column support (e.g. MySQL)
type DrvWithAutoInc interface {
	Drv

	// ExtractAutoIncColumn() extract the 'auto increament' column's name for a given table or "" if not found.
	ExtractAutoIncColumn(conn *sql.DB, tableName string) (columnName string, err error)
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
