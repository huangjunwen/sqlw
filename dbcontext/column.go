package dbcontext

// Column represents query result column.
type Column struct {
	// ColumnName is the name of the column.
	ColumnName string

	// DataType is a driver-specific type identifier (ignore nullable), such as:
	// - uint24
	// - json
	// - time
	// It is used in scan type mapping only, thus can be any valid identifier, no need to be a real type name.
	DataType string

	// Nullable is true if the column can have NULL value.
	Nullable bool
}
