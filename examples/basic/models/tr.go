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
	// Basic information.
	tableName          string
	columnNames        []string       // column pos -> column name
	columnNamesMap     map[string]int // column name -> column pos
	primaryColumnNames []string       // len(primaryColumnNames) == 0 if not exists
	autoIncColumnName  string         // autoIncColumnName == "" if not exists

	// Precalculate information.
	pcColumnList  string // "`col1`, `col2`, ..."
	pcPrimaryCond string // "`id1`=? AND `id2`=? ..."
	pcSelectQuery string // "SELECT `col11`, ... FROM tab WHERE `id1`=? AND ..."
	pcDeleteQuery string // "DELETE FROM tab WHERE `id1`=? AND ..."
	// NOTE: INSERT and UPDATE querys are dynamic generated
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
func NewTableMeta(tableName string, columnNames []string, opts ...TableMetaOption) *TableMeta {
	meta := &TableMeta{
		tableName:      tableName,
		columnNames:    columnNames,
		columnNamesMap: make(map[string]int),
	}
	for i, columnName := range meta.columnNames {
		meta.columnNamesMap[columnName] = i
	}
	for _, opt := range opts {
		opt(meta)
	}
	meta.precalculate()
	return meta
}

func (meta *TableMeta) precalculate() {
	// pcColumnList
	columnList := []byte{}
	for _, columnName := range meta.columnNames {
		columnList = append(columnList, ", `"...)
		columnList = append(columnList, columnName...)
		columnList = append(columnList, '`')
	}
	meta.pcColumnList = string(columnList[2:]) // Strip the initial ", "

	if len(meta.primaryColumnNames) == 0 {
		return
	}

	// primaryCond
	primaryCond := []byte{}
	for _, columnName := range meta.primaryColumnNames {
		primaryCond = append(primaryCond, " AND `"...)
		primaryCond = append(primaryCond, columnName...)
		primaryCond = append(primaryCond, "`=?"...)
	}
	meta.pcPrimaryCond = string(primaryCond[5:]) // Strip the initial " AND "

	// pcSelectQuery
	meta.pcSelectQuery = fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", meta.pcColumnList, meta.tableName, meta.pcPrimaryCond)

	// pcDeleteQuery
	meta.pcDeleteQuery = fmt.Sprintf("DELETE FROM `%s` WHERE %s", meta.tableName, meta.pcPrimaryCond)

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
	cols := []byte{} // ", `col1`, `col2`, ..."
	phs := []byte{}  // ", ?, ?, ..."
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

		cols = append(cols, ", `"...)
		cols = append(cols, columnName...)
		cols = append(cols, '`')
		phs = append(phs, ", ?"...)
		args = append(args, val)
	}

	if len(cols) > 0 {
		cols = cols[2:] // Strip initial ", "
	}
	if len(phs) > 0 {
		phs = phs[2:] // Strip initial ", "
	}

	// NOTE: "INSERT INTO tab () VALUES ()" is valid
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", meta.TableName(), cols, phs), args, nil
}

// insertTableRow insert a table row with not-null columns (e.g. null columns are ignored).
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

	primVal := tr.PrimaryValue()
	if primVal.IsNull() {
		return "", nil, fmt.Errorf("Update: row(s) have null primary value(s)")
	}
	if primVal != newTr.PrimaryValue() {
		return "", nil, fmt.Errorf("Update: row(s) have different primary value(s)")
	}

	asgmts := []byte{} // ", `col1`=?, `col2`=?, ..."
	asgmtArgs := []interface{}{}

	vals := []interface{}{}
	tr.ColumnValuers(&vals)
	newVals := []interface{}{}
	newTr.ColumnValuers(&newVals)
	n := meta.NumColumn()

	for i := 0; i < n; i++ {

		columnName := meta.ColumnName(i)
		val := vals[i]
		newVal := newVals[i]

		if meta.IsPrimaryColumn(columnName) {
			// Skip primary column(s).
			continue
		}

		if val == newVal {
			// Skip column(s) that have same values.
			continue
		}

		asgmts = append(asgmts, ", `"...)
		asgmts = append(asgmts, columnName...)
		asgmts = append(asgmts, "`=?"...)
		asgmtArgs = append(asgmtArgs, newVal)

	}

	if len(asgmtArgs) == 0 {
		return "", nil, nil
	}

	primArgs := []interface{}{}
	primVal.PrimaryValuers(&primArgs)
	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", meta.TableName(), asgmts[2:], meta.pcPrimaryCond), append(asgmtArgs, primArgs...), nil

}

// updateTableRow updates a table row to new values. Only columns with different values are updated.
func updateTableRow(ctx context.Context, e Execer, tr, newTr TableRowWithPrimary) error {

	query, args, err := buildUpdate(tr, newTr)
	if err != nil {
		return err
	}

	// No changes.
	if query == "" {
		return nil
	}

	// NOTE: Affected rows can have different meanings in MySQL (https://dev.mysql.com/doc/refman/5.7/en/mysql-affected-rows.html):
	//
	//   For UPDATE statements, the affected-rows value by default is the number of
	//   rows actually changed. If you specify the CLIENT_FOUND_ROWS flag to mysql_real_connect()
	//   when connecting to mysqld, the affected-rows value is the number of rows "found";
	//   that is, matched by the WHERE clause.
	//
	// So just discard this piece of information.
	_, err = e.ExecContext(ctx, query, args...)
	return err

}

// deleteTableRow deletes a table row.
func deleteTableRow(ctx context.Context, e Execer, tr TableRowWithPrimary) error {

	primVal := tr.PrimaryValue()
	if primVal.IsNull() {
		return fmt.Errorf("Delete: row has null primary value(s)")
	}

	args := []interface{}{}
	primVal.PrimaryValuers(&args)

	_, err := e.ExecContext(ctx, tr.TableMeta().pcDeleteQuery, args...)
	return err

}

// selectTableRow selects a table row with/without "FOR UPDATE".
//
// NOTE: ErrNoRows can be returned.
func selectTableRow(ctx context.Context, q Queryer, tr TableRowWithPrimary, lock bool) error {

	primVal := tr.PrimaryValue()
	if primVal.IsNull() {
		return fmt.Errorf("Reload: row has null primary value(s)")
	}

	args := []interface{}{}
	primVal.PrimaryValuers(&args)

	query := tr.TableMeta().pcSelectQuery
	if lock {
		query += " FOR UPDATE"
	}
	row := q.QueryRowContext(ctx, query, args...)

	dest := []interface{}{}
	tr.ColumnScanners(&dest)

	return row.Scan(dest...)

}
