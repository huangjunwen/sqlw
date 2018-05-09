package dbcontext

import (
	"database/sql"
)

// Col represents query result column.
type Col struct {
	// ColumnType is the raw column type.
	*sql.ColumnType

	// DataType is a 'translated' driver-specific type identifier (ignore nullable), such as:
	// - uint24
	// - json
	// - time
	// It is used in scan type mapping only, thus can be any valid identifier, no need to be a real type name.
	DataType string
}
