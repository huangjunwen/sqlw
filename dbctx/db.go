package dbctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

type DBInfo struct {
	tables     []*TableInfo
	tableNames map[string]int
}

type TableInfo struct {
	db            *DBInfo
	tableName     string
	columns       []*ColumnInfo
	columnNames   map[string]int
	indices       []*IndexInfo
	indexNames    map[string]int
	fks           []*FKInfo
	fkNames       map[string]int
	primary       *IndexInfo  // nil if not exists
	autoIncColumn *ColumnInfo // nil if not exists
}

type ColumnInfo struct {
	table      *TableInfo
	columnName string
	columnType *sql.ColumnType
	pos        int // position in table
}

type IndexInfo struct {
	table     *TableInfo
	indexName string
	columns   []*ColumnInfo
	isPrimary bool
	isUnique  bool
}

type FKInfo struct {
	fkName         string
	table          *TableInfo
	columns        []*ColumnInfo
	refTableName   string
	refColumnNames []string
}

func newDBInfo(conn *sql.DB, drv driver.Driver) (*DBInfo, error) {

	_, supportAutoInc := drv.(driver.DriverWithAutoInc)

	db := &DBInfo{
		tableNames: make(map[string]int),
	}

	tableNames, err := drv.ExtractTableNames(conn)
	if err != nil {
		return nil, err
	}

	for _, tableName := range tableNames {

		table := &TableInfo{
			db:          db,
			tableName:   tableName,
			columnNames: make(map[string]int),
			indexNames:  make(map[string]int),
			fkNames:     make(map[string]int),
		}

		// fill columns info

		columnNames, columnTypes, err := drv.ExtractColumns(conn, tableName)
		if err != nil {
			return nil, err
		}

		for i, columnName := range columnNames {
			column := &ColumnInfo{
				table:      table,
				columnName: columnName,
				columnType: columnTypes[i],
				pos:        i,
			}
			table.columns = append(table.columns, column)
			table.columnNames[columnName] = len(table.columns) - 1
		}

		if supportAutoInc {
			autoIncColumnName, err := drv.(driver.DriverWithAutoInc).ExtractAutoIncColumn(conn, tableName)
			if err != nil {
				return nil, err
			}
			if autoIncColumnName != "" {
				table.autoIncColumn = table.columns[table.columnNames[autoIncColumnName]]
			}
		}

		// fill indices info

		indexNames, err := drv.ExtractIndexNames(conn, tableName)
		if err != nil {
			return nil, err
		}

		for _, indexName := range indexNames {
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(conn, tableName, indexName)
			if err != nil {
				return nil, err
			}

			index := &IndexInfo{
				table:     table,
				indexName: indexName,
				isPrimary: isPrimary,
				isUnique:  isUnique,
			}

			for _, columnName := range columnNames {
				index.columns = append(index.columns, table.columns[table.columnNames[columnName]])
			}

			table.indices = append(table.indices, index)
			table.indexNames[indexName] = len(table.indices) - 1

			// This is primary index
			if isPrimary {
				table.primary = index
			}
		}

		// fill fk info

		fkNames, err := drv.ExtractFKNames(conn, tableName)
		if err != nil {
			return nil, err
		}

		for _, fkName := range fkNames {
			columnNames, refTableName, refColumnNames, err := drv.ExtractFK(conn, tableName, fkName)
			if err != nil {
				return nil, err
			}

			fk := &FKInfo{
				fkName:         fkName,
				table:          table,
				refTableName:   refTableName,
				refColumnNames: refColumnNames,
			}

			for _, columnName := range columnNames {
				fk.columns = append(fk.columns, table.columns[table.columnNames[columnName]])
			}

			table.fks = append(table.fks, fk)
			table.fkNames[fkName] = len(table.fks) - 1

		}

		db.tables = append(db.tables, table)
		db.tableNames[tableName] = len(db.tables) - 1

	}

	return db, nil

}

func (info *DBInfo) Valid() bool {
	return info != nil
}

func (info *DBInfo) NumTable() int {
	return len(info.tables)
}

func (info *DBInfo) Table(i int) *TableInfo {
	return info.tables[i]
}

func (info *DBInfo) TableByName(tableName string) (tableInfo *TableInfo, found bool) {
	i, found := info.tableNames[tableName]
	if !found {
		return nil, false
	}
	return info.tables[i], true
}

func (info *DBInfo) TableByNameM(tableName string) *TableInfo {
	table, found := info.TableByName(tableName)
	if !found {
		panic(fmt.Errorf("Table %+q not found", tableName))
	}
	return table
}

func (info *TableInfo) Valid() bool {
	return info != nil
}

func (info *TableInfo) String() string {
	if info == nil {
		return "<nil table>"
	}
	return info.tableName
}

func (info *TableInfo) TableName() string {
	return info.tableName
}

func (info *TableInfo) NumColumn() int {
	return len(info.columns)
}

func (info *TableInfo) Column(i int) *ColumnInfo {
	return info.columns[i]
}

func (info *TableInfo) ColumnByName(columnName string) (columnInfo *ColumnInfo, found bool) {
	i, found := info.columnNames[columnName]
	if !found {
		return nil, false
	}
	return info.columns[i], true
}

func (info *TableInfo) ColumnByNameM(columnName string) *ColumnInfo {
	column, found := info.ColumnByName(columnName)
	if !found {
		panic(fmt.Errorf("Column %+q not found in table %+q", columnName, info.tableName))
	}
	return column
}

func (info *TableInfo) NumIndex() int {
	return len(info.indices)
}

func (info *TableInfo) Index(i int) *IndexInfo {
	return info.indices[i]
}

func (info *TableInfo) IndexByName(indexName string) (indexInfo *IndexInfo, found bool) {
	i, found := info.indexNames[indexName]
	if !found {
		return nil, false
	}
	return info.indices[i], true
}

func (info *TableInfo) IndexByNameM(indexName string) *IndexInfo {
	index, found := info.IndexByName(indexName)
	if !found {
		panic(fmt.Errorf("Index %+q not found in table %+q", indexName, info.tableName))
	}
	return index
}

func (info *TableInfo) NumFK() int {
	return len(info.fks)
}

func (info *TableInfo) FK(i int) *FKInfo {
	return info.fks[i]
}

func (info *TableInfo) FKByName(fkName string) (fkInfo *FKInfo, found bool) {
	i, found := info.fkNames[fkName]
	if !found {
		return nil, false
	}
	return info.fks[i], true
}

func (info *TableInfo) FKByNameM(fkName string) *FKInfo {
	fk, found := info.FKByName(fkName)
	if !found {
		panic(fmt.Errorf("FK %+q not found in table %+q", fkName, info.tableName))
	}
	return fk
}

func (info *TableInfo) Primary() *IndexInfo {
	return info.primary
}

// AutoIncColumn() returns the single 'auto increment' column of the table.
//
// NOTE: If the database does not support such sematic, it always returns nil.
func (info *TableInfo) AutoIncColumn() *ColumnInfo {
	return info.autoIncColumn
}

func (info *ColumnInfo) Valid() bool {
	return info != nil
}

func (info *ColumnInfo) String() string {
	if info == nil {
		return "<nil column>"
	}
	return info.columnName
}

func (info *ColumnInfo) ColumnName() string {
	return info.columnName
}

func (info *ColumnInfo) Table() *TableInfo {
	return info.table
}

func (info *ColumnInfo) ColumnType() *sql.ColumnType {
	return info.columnType
}

func (info *ColumnInfo) Pos() int {
	return info.pos
}

func (info *IndexInfo) Valid() bool {
	return info != nil
}

func (info *IndexInfo) String() string {
	if info == nil {
		return "<nil index>"
	}
	return info.indexName
}

func (info *IndexInfo) IndexName() string {
	return info.indexName
}

func (info *IndexInfo) Table() *TableInfo {
	return info.table
}

func (info *IndexInfo) Columns() []*ColumnInfo {
	return info.columns
}

func (info *IndexInfo) IsPrimary() bool {
	return info.isPrimary
}

func (info *IndexInfo) IsUnique() bool {
	return info.isUnique
}

func (info *FKInfo) Valid() bool {
	return info != nil
}

func (info *FKInfo) String() string {
	if info == nil {
		return "<nil fk>"
	}
	return info.fkName
}

func (info *FKInfo) FKName() string {
	return info.fkName
}

func (info *FKInfo) Table() *TableInfo {
	return info.table
}

func (info *FKInfo) Columns() []*ColumnInfo {
	return info.columns
}

func (info *FKInfo) RefTable() *TableInfo {
	refTable, found := info.table.db.TableByName(info.refTableName)
	if !found {
		panic(fmt.Errorf("Can't find ref table %+q", info.refTableName))
	}
	return refTable
}

func (info *FKInfo) RefColumns() []*ColumnInfo {
	refTable := info.RefTable()
	refColumns := []*ColumnInfo{}
	for _, refColumnName := range info.refColumnNames {
		refColumn, found := refTable.ColumnByName(refColumnName)
		if !found {
			panic(fmt.Errorf("Can't find column %+q in ref table %+q", refColumnName, info.refTableName))
		}
		refColumns = append(refColumns, refColumn)
	}
	return refColumns
}
