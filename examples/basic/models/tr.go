package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// TableRow represents a row (record) of a table.
type TableRow interface {
	// TableMeta returns meta information of the table.
	TableMeta() *TableMeta

	// ColumnValuers append all columns as valuers to dest.
	ColumnValuers(dest *[]interface{})

	// ColumnScanners append all columns as scanners to dest.
	ColumnScanners(dest *[]interface{})
}

// TableRowWithPrimary represents a row (record) of a table with primary key.
type TableRowWithPrimary interface {
	TableRow

	// PrimaryValue returns the primary key value of the row.
	PrimaryValue() TableRowPrimaryValue
}

// TableRowPrimaryValue represents the primary key value of a table row.
type TableRowPrimaryValue interface {
	// IsNull returns true if the primary key value is not valid (NULL).
	IsNull() bool

	// PrimaryValuers primary columns as valuers to dest.
	PrimaryValuers(dest *[]interface{})
}

// TableMeta contains meta information of a table.
type TableMeta struct {
	tableName          string
	columnNames        []string       // column pos -> column name
	columnNamesMap     map[string]int // column name -> column pos
	primaryColumnNames []string       // len(primaryColumnNames) == 0 if not exists
	autoIncColumnName  string         // autoIncColumnName == "" if not exists
}

// TableMetaOption is option in creating TableMeta.
type TableMetaOption func(*TableMeta)

// OptPrimaryColumnNames sets primary column names of the table.
func OptPrimaryColumnNames(columnNames ...string) TableMetaOption {
	return func(meta *TableMeta) {
		for _, columnName := range columnNames {
			_, ok := meta.columnNamesMap[columnName]
			if !ok {
				panic(fmt.Errorf("Can't find column name %+q in table %+q", columnName, meta.tableName))
			}
		}
		meta.primaryColumnNames = columnNames
	}
}

// OptAutoIncColumnName sets the 'auto_increment' column name of the table.
func OptAutoIncColumnName(columnName string) TableMetaOption {
	return func(meta *TableMeta) {
		_, ok := meta.columnNamesMap[columnName]
		if !ok {
			panic(fmt.Errorf("Can't find column name %+q in table %+q", columnName, meta.tableName))
		}
		meta.autoIncColumnName = columnName
	}
}

// NewTableMeta creates a new TableMeta.
//
// NOTE: This function must be called at package-level variable initialization or in init() function.
func NewTableMeta(tableName string, columnNames []string, opts ...TableMetaOption) *TableMeta {
	meta := &TableMeta{
		tableName:      tableName,
		columnNames:    columnNames,
		columnNamesMap: make(map[string]int),
	}
	for i, columnName := range columnNames {
		meta.columnNamesMap[columnName] = i
	}
	for _, opt := range opts {
		opt(meta)
	}

	// Some global initialization.
	if len(meta.primaryColumnNames) != 0 {
		buildDelete(meta)
		buildReload(meta)
	}
	return meta
}

// TableName returns the name of the table.
func (meta *TableMeta) TableName() string {
	return meta.tableName
}

// NumColumn returns the number of columns of the table.
func (meta *TableMeta) NumColumn() int {
	return len(meta.columnNames)
}

// ColumnName returns the i-th column name.
//
// It panics if i is not in range [0, NumColumn()).
func (meta *TableMeta) ColumnName(i int) string {
	return meta.columnNames[i]
}

// NumPrimaryColumn returns the number of columns of primary key or 0 if no primary key exists.
func (meta *TableMeta) NumPrimaryColumn() int {
	return len(meta.primaryColumnNames)
}

// PrimaryColumnName returns the i-th primary key column name.
//
// It panics if i is not in range [0, NumPrimaryColumn()).
func (meta *TableMeta) PrimaryColumnName(i int) string {
	return meta.primaryColumnNames[i]
}

// IsPrimaryColumn returns true if named column is part of the primary key.
func (meta *TableMeta) IsPrimaryColumn(columnName string) bool {
	// NOTE: use loop is fast enough since primary key usually contains only a few columns.
	for _, colName := range meta.primaryColumnNames {
		if colName == columnName {
			return true
		}
	}
	return false
}

// AutoIncColumnName returns the 'auto_increment' column name or "" if not exists.
func (meta *TableMeta) AutoIncColumnName() string {
	return meta.autoIncColumnName
}

func isNull(value interface{}) bool {
	if value == nil {
		return true
	}
	if val, ok := value.(driver.Valuer); ok {
		v, err := val.Value()
		if err != nil {
			panic(err)
		}
		return v == nil
	}
	return false
}

func buildInsert(tr TableRow) (string, []interface{}, error) {

	meta := tr.TableMeta()
	cols := []byte{} // "`col1`, `col2`, ..."
	phs := []byte{}  // "?, ?, ..."
	args := []interface{}{}

	vals := []interface{}{}
	tr.ColumnValuers(&vals)
	n := meta.NumColumn()

	for i := 0; i < n; i++ {
		columnName := meta.ColumnName(i)
		val := vals[i]

		// Skip null column.
		if isNull(val) {
			continue
		}

		if len(args) != 0 {
			cols = append(cols, ", "...)
			phs = append(phs, ", "...)
		}
		cols = append(cols, '`')
		cols = append(cols, columnName...)
		cols = append(cols, '`')
		phs = append(phs, '?')
		args = append(args, val)
	}

	// NOTE: "INSERT INTO tab () VALUES ()" is valid
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", meta.TableName(), cols, phs), args, nil
}

func insertTableRow(ctx context.Context, e Execer, tr TableRow) (sql.Result, error) {

	query, args, err := buildInsert(tr)
	if err != nil {
		return nil, err
	}

	return e.ExecContext(ctx, query, args...)

}

func buildUpdate(tr, newTr TableRowWithPrimary) (string, []interface{}, error) {

	meta := tr.TableMeta()
	if meta != newTr.TableMeta() {
		panic(fmt.Errorf("Update: update with different table's row"))
	}

	asgmts := []byte{} // "`col1`=?, `col2`=?, ..."
	prims := []byte{}  // "`id1`=? AND `id2`=? ..."
	asgmtArgs := []interface{}{}
	primArgs := []interface{}{}

	vals := []interface{}{}
	tr.ColumnValuers(&vals)
	newVals := []interface{}{}
	newTr.ColumnValuers(&newVals)
	n := meta.NumColumn()

	for i := 0; i < n; i++ {

		columnName := meta.ColumnName(i)
		val := vals[i]
		newVal := newVals[i]

		// --- Normal column ---
		if !meta.IsPrimaryColumn(columnName) {

			if val == newVal {
				// No change.
				continue
			}

			if len(asgmtArgs) != 0 {
				asgmts = append(asgmts, ", "...)
			}
			asgmts = append(asgmts, '`')
			asgmts = append(asgmts, columnName...)
			asgmts = append(asgmts, "`=?"...)
			asgmtArgs = append(asgmtArgs, newVal)

			continue

		}

		// --- Primary column ---

		// Two rows should have same not-null primary values.
		if val != newVal {
			return "", nil, fmt.Errorf("Update: rows have different primary value(s)")
		}
		if isNull(val) {
			return "", nil, fmt.Errorf("Update: rows have null primary value(s)")
		}

		// Add where clause.
		if len(primArgs) != 0 {
			prims = append(prims, " AND "...)
		}
		prims = append(prims, '`')
		prims = append(prims, columnName...)
		prims = append(prims, "`=?"...)
		primArgs = append(primArgs, val)

		// Add "id=id, ..." in assigment list as placeholder. Since
		// "UPDATE tab SET WHERE id=1" is not valid, but
		// "UPDATE tab SET id=id WHERE id=1" is valid.
		if len(asgmtArgs) != 0 {
			asgmts = append(asgmts, ", "...)
		}
		asgmts = append(asgmts, '`')
		asgmts = append(asgmts, columnName...)
		asgmts = append(asgmts, "`=`"...)
		asgmts = append(asgmts, columnName...)
		asgmts = append(asgmts, '`')

	}

	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", meta.TableName(), asgmts, prims), append(asgmtArgs, primArgs), nil

}

func updateTableRow(ctx context.Context, e Execer, tr, newTr TableRowWithPrimary) (updated bool, err error) {

	query, args, err := buildUpdate(tr, newTr)
	if err != nil {
		return false, err
	}

	r, err := e.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}

	affected, err := r.RowsAffected()
	if err != nil {
		return false, err
	}

	if affected <= 0 {
		return false, nil
	}

	return true, nil

}

var (
	deleteTableRowQuerys = map[*TableMeta]string{}
)

func buildDelete(meta *TableMeta) {

	n := meta.NumPrimaryColumn()
	if n <= 0 {
		panic(fmt.Errorf("buildDelete for table without primary key"))
	}

	prims := []byte{} // "`id1`=? AND `id2=?` ..."

	for i := 0; i < n; i++ {

		columnName := meta.PrimaryColumnName(i)
		if len(prims) != 0 {
			prims = append(prims, " AND "...)
		}
		prims = append(prims, '`')
		prims = append(prims, columnName...)
		prims = append(prims, "`=?"...)

	}

	deleteTableRowQuerys[meta] = fmt.Sprintf("DELETE FROM `%s` WHERE %s", meta.TableName(), prims)

}

func deleteTableRow(ctx context.Context, e Execer, tr TableRowWithPrimary) (deleted bool, err error) {

	p := tr.PrimaryValue()
	if p.IsNull() {
		return false, fmt.Errorf("Delete: row has null primary value(s)")
	}

	args := []interface{}{}
	p.PrimaryValuers(&args)

	r, err := e.ExecContext(ctx, deleteTableRowQuerys[tr.TableMeta()], args...)
	if err != nil {
		return false, err
	}

	affected, err := r.RowsAffected()
	if err != nil {
		return false, err
	}

	if affected <= 0 {
		return false, nil
	}

	return true, nil

}

var (
	reloadTableRowQuerys = map[*TableMeta]string{}
)

func buildReload(meta *TableMeta) {

	n := meta.NumPrimaryColumn()
	if n <= 0 {
		panic(fmt.Errorf("buildReload for table without primary key"))
	}

	prims := []byte{} // "`id1`=? AND `id2=?` ..."

	for i := 0; i < n; i++ {

		columnName := meta.PrimaryColumnName(i)
		if len(prims) != 0 {
			prims = append(prims, " AND "...)
		}
		prims = append(prims, '`')
		prims = append(prims, columnName...)
		prims = append(prims, "`=?"...)

	}

	selects := []byte{} // "`col1`, `col2`, ..."

	m := meta.NumColumn()

	for i := 0; i < m; i++ {

		columnName := meta.ColumnName(i)
		if len(selects) != 0 {
			selects = append(selects, ", "...)
		}
		selects = append(selects, '`')
		selects = append(selects, columnName...)
		selects = append(selects, '`')

	}

	reloadTableRowQuerys[meta] = fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", selects, meta.TableName(), prims)

}

func reloadTableRow(ctx context.Context, q Queryer, tr TableRowWithPrimary) (reloaded bool, err error) {

	p := tr.PrimaryValue()
	if p.IsNull() {
		return false, fmt.Errorf("Reload: row has null primary value(s)")
	}

	args := []interface{}{}
	p.PrimaryValuers(&args)

	row := q.QueryRowContext(ctx, reloadTableRowQuerys[tr.TableMeta()], args...)

	dest := []interface{}{}
	tr.ColumnScanners(&dest)

	if err := row.Scan(dest...); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return true, nil

}
