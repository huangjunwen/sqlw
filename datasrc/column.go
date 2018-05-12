package datasrc

import (
	"database/sql"
)

// Column represents a query result column.
type Column struct {
	// ColumnType is the raw column type.
	*sql.ColumnType

	// DataType is a 'translated' driver-specific type identifier (ignore nullable), such as:
	// - uint24
	// - json
	// - time
	// It is used in scan type mapping only, thus can be any valid identifier, no need to be a real type name.
	DataType string
}
