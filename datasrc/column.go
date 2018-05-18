package datasrc

import (
	"database/sql"
	"reflect"
)

// Column represents a query result column.
type Column struct {
	// --- The following fields are extract from sql.ColumnType ---

	// Name is name of the result column.
	Name string

	// ScanType is the go type suitable for scanning into.
	ScanType reflect.Type

	// DatabaseTypeName is the database system name of the column type. (e.g. "VARCHAR", "INT")
	DatabaseTypeName string

	// Nullable is valid only when HasNullable is true.
	HasNullable bool

	// Nullable is true if this column can have NULL value.
	Nullable bool

	// Length is valid only HasLength is true
	HasLength bool

	// Length returns the column type length for variable length column types.
	Length int64

	// Precision and Scale is valid only when HasPrecisionScale is true.
	HasPrecisionScale bool

	// Column precision.
	Precision int64

	// Column scale.
	Scale int64

	// --- Use DataType instead of ScanType and DatabaseTypeName ---

	// DataType is a 'translated' driver-specific type identifier (ignore nullable), such as:
	// - uint24
	// - json
	// - time
	// It is used in scan type mapping only, thus can be any valid identifier, no need to be a real type name.
	DataType string
}

// NewColumn extract information from sql.Column and returns Column.
func NewColumn(col *sql.ColumnType, dataType string) *Column {
	ret := &Column{
		Name:             col.Name(),
		ScanType:         col.ScanType(),
		DatabaseTypeName: col.DatabaseTypeName(),
		DataType:         dataType,
	}
	ret.Nullable, ret.HasNullable = col.Nullable()
	ret.Length, ret.HasLength = col.Length()
	ret.Precision, ret.Scale, ret.HasPrecisionScale = col.DecimalSize()
	return ret
}
