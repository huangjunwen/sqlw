package wrapper

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlwrapper/driver"
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
	primary       int // <0 if not exists
	autoIncColumn int // <0 if not exists TODO
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

func ExtractDBInfo(drv driver.Driver, db *sql.DB, filter func(string) bool) (*DBInfo, error) {
	if filter == nil {
		// no filter
		filter = func(string) bool { return true }
	}

	dbInfo := &DBInfo{
		tableNames: make(map[string]int),
	}

	tableNames, err := drv.ExtractTableNames(db)
	if err != nil {
		return nil, err
	}

	for _, tableName := range tableNames {
		if !filter(tableName) {
			continue
		}

		tableInfo := &TableInfo{
			db:            dbInfo,
			tableName:     tableName,
			columnNames:   make(map[string]int),
			indexNames:    make(map[string]int),
			fkNames:       make(map[string]int),
			primary:       -1,
			autoIncColumn: -1,
		}

		// fill columns info

		columnNames, columnTypes, err := drv.ExtractColumns(db, tableName)
		if err != nil {
			return nil, err
		}

		for i, columnName := range columnNames {
			columnInfo := &ColumnInfo{
				table:      tableInfo,
				columnName: columnName,
				columnType: columnTypes[i],
				pos:        i,
			}
			tableInfo.columns = append(tableInfo.columns, columnInfo)
			tableInfo.columnNames[columnName] = len(tableInfo.columns) - 1
		}

		// fill indices info

		indexNames, err := drv.ExtractIndexNames(db, tableName)
		if err != nil {
			return nil, err
		}

		for _, indexName := range indexNames {
			columnNames, isPrimary, isUnique, err := drv.ExtractIndex(db, tableName, indexName)
			if err != nil {
				return nil, err
			}

			indexInfo := &IndexInfo{
				table:     tableInfo,
				indexName: indexName,
				isPrimary: isPrimary,
				isUnique:  isUnique,
			}

			for _, columnName := range columnNames {
				indexInfo.columns = append(indexInfo.columns, tableInfo.columns[tableInfo.columnNames[columnName]])
			}

			tableInfo.indices = append(tableInfo.indices, indexInfo)
			tableInfo.indexNames[indexName] = len(tableInfo.indices) - 1

			// This is primary index
			if isPrimary {
				tableInfo.primary = len(tableInfo.indices) - 1
			}
		}

		// fill fk info

		fkNames, err := drv.ExtractFKNames(db, tableName)
		if err != nil {
			return nil, err
		}

		for _, fkName := range fkNames {
			columnNames, refTableName, refColumnNames, err := drv.ExtractFK(db, tableName, fkName)
			if err != nil {
				return nil, err
			}

			fkInfo := &FKInfo{
				fkName:         fkName,
				table:          tableInfo,
				refTableName:   refTableName,
				refColumnNames: refColumnNames,
			}

			for _, columnName := range columnNames {
				fkInfo.columns = append(fkInfo.columns, tableInfo.columns[tableInfo.columnNames[columnName]])
			}

			tableInfo.fks = append(tableInfo.fks, fkInfo)
			tableInfo.fkNames[fkName] = len(tableInfo.fks) - 1

		}

		dbInfo.tables = append(dbInfo.tables, tableInfo)
		dbInfo.tableNames[tableName] = len(dbInfo.tables) - 1

	}

	return dbInfo, nil

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

func (info *TableInfo) String() string {
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

func (info *TableInfo) Primary() *IndexInfo {
	if info.primary < 0 {
		return nil
	}
	return info.indices[info.primary]
}

func (info *TableInfo) AutoIncColumn() *ColumnInfo {
	panic("Not Implemented")
}

func (info *ColumnInfo) String() string {
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

func (info *IndexInfo) String() string {
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

func (info *FKInfo) String() string {
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
		panic(fmt.Errorf("Can't find ref table %+q, maybe it's filter out?", info.refTableName))
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
