package sqlwrapper

import (
	"database/sql"
	"fmt"
)

// TableInfo contains information of a given database table.
type TableInfo interface {
	// Same as TableName().
	fmt.Stringer

	// TableName() returns the name of the table.
	TableName() string

	// NumColumn() returns the number of columns in the table.
	NumColumn() int

	// Column() returns the i'th column, it panic if i is not in range [0, NumColumn()) .
	Column(i int) ColumnInfo

	// ColumnByName() returns the named column or nil (and false) if not found.
	ColumnByName(columnName string) (columnInfo ColumnInfo, found bool)

	// NumIndex() returns the number of indices in the table.
	NumIndex() int

	// Index() returns the i'th index, it panic if i is not in range [0, NumIndex()) .
	Index(i int) IndexInfo

	// IndexByName() returns the named index or nil (and false) if not found.
	IndexByName(indexName string) (indexInfo IndexInfo, found bool)

	// NumFK() returns the number of foreign key constraints in the table.
	NumFK() int

	// FK() returns the i'th foreign key, it panic if i is not in range [0, NumFK()) .
	FK(i int) FKInfo

	// FKByName() returns the named foreign key or nil (and false) if not found.
	FKByName(fkName string) (fkInfo FKInfo, found bool)

	// Primary() returns the primary index of the table.
	Primary() IndexInfo

	// AutoIncColumn() returns the integer auto increase column of the table or nil if not found.
	AutoIncColumn() (columnInfo ColumnInfo, found bool)
}

// ColumnInfo contains information of a column.
type ColumnInfo interface {
	// Same as ColumnName().
	fmt.Stringer

	// ColumnName() returns the name of the column.
	ColumnName() string

	// Table() returns the table this column belongs to.
	Table() TableInfo

	// ColumnType() returns the type of the column.
	ColumnType() *sql.ColumnType

	// Pos() returns the column's position in table.
	Pos() int
}

// IndexInfo contains information of an index.
type IndexInfo interface {
	// Same as IndexName()
	fmt.Stringer

	// IndexName() returns the name of the index (key).
	IndexName() string

	// Table() returns the table this index belongs to.
	Table() TableInfo

	// Columns() returns the columns of the index.
	Columns() []ColumnInfo

	// IsPrimary() returns true if this is the primary key.
	IsPrimary() bool

	// IsUnique() returns true if this is a unique key.
	IsUnique() bool
}

// FKInfo contains information of a foreign key constraint.
//
// NOTE: the RefColumns must be a primary (or unique) key.
type FKInfo interface {
	// Same as FKName().
	fmt.Stringer

	// FKName() returns the name of this foreign key constraint.
	FKName() string

	// Table() returns the table this foreign key belongs to.
	Table() TableInfo

	// Columns() returns the columns of the foreign key.
	Columns() []ColumnInfo

	// RefTable() returns the referenced table.
	RefTable() TableInfo

	// RefColumns() returns the referenced columns of the foreign key.
	RefColumns() []ColumnInfo
}

// StmtInfo contains information of an SQL statement.
type StmtInfo interface {
	// Same as StmtName().
	fmt.Stringer

	// StmtName() returns the name of the statment, it is used as the wrapper function name.
	StmtName() string

	// Args() returns a list of argument of the wrapper function.
	Args() []ArgInfo

	// Text() returns the statment text used for template renderring.
	Text() string
}

// ArgInfo contains information of a function argument.
type ArgInfo interface {
	// Same as ArgName().
	fmt.Stringer

	// ArgName() returns the name of the argument.
	ArgName() string

	// TypeName() returns the type in literal (e.g. "*sql.NullString")
	TypeName() string
}

// SelectStmtInfo contains information of a 'SELECT' sql.
type SelectStmtInfo interface {
	StmtInfo

	// NumResultColumn() returns the number of result columns.
	NumResultColumn() int

	// ResultColumnName() returns the i'th result column name, it panic if i not in range [0, NumResultColumn()) .
	ResultColumnName(i int) string

	// ResultColumnType() returns the i'th result column type, it panic if i not in range [0, NumResultColumn()) .
	ResultColumnType(i int) *sql.ColumnType

	// Wildcard() returns wildcard information of the i'th result column if it is known that it belongs to a wildcard expansion (e.g. "table.*") .
	// It panic if i not in range [0, NumResultColumn()) .
	Wildcard(i int) (wildcardInfo WildcardInfo, columnInfo ColumnInfo, yes bool)
}

// WildcardInfo contains information of a wildcard expansion.
type WildcardInfo interface {
	// Alias() or Table().TableName()
	fmt.Stringer

	// Table() returns the table of this wildcard expansion.
	Table() TableInfo

	// Alias() returns the alias of this wildcard expansion or "" if not used.
	Alias() string
}
