package mysql

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/go-sql-driver/mysql"

	"github.com/huangjunwen/sqlw/driver"
)

type mysqlDriver struct{}

var (
	_ driver.Drv            = mysqlDriver{}
	_ driver.DrvWithAutoInc = mysqlDriver{}
)

var (
	// Copy from github.com/go-sql-driver/mysql/fields.go
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime  = reflect.TypeOf(mysql.NullTime{})
	scanTypeUint8     = reflect.TypeOf(uint8(0))
	scanTypeUint16    = reflect.TypeOf(uint16(0))
	scanTypeUint32    = reflect.TypeOf(uint32(0))
	scanTypeUint64    = reflect.TypeOf(uint64(0))
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
)

func (driver mysqlDriver) ExtractTableNames(conn *sql.DB) (tableNames []string, err error) {
	rows, err := conn.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tableName := ""
		if err = rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func (driver mysqlDriver) ExtractColumns(conn *sql.DB, tableName string) (columnNames []string, columnTypes []*sql.ColumnType, err error) {
	rows, err := conn.Query("SELECT * FROM " + tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columnNames, err = rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	return columnNames, columnTypes, nil
}

func (driver mysqlDriver) ExtractAutoIncColumn(conn *sql.DB, tableName string) (columnName string, err error) {
	dbName, err := extractDBName(conn)
	if err != nil {
		return "", err
	}

	rows, err := conn.Query(`
	SELECT
		COLUMN_NAME
	FROM
		INFORMATION_SCHEMA.COLUMNS
	WHERE
		TABLE_SCHEMA=? AND TABLE_NAME=? AND EXTRA LIKE ?
	`, dbName, tableName, "%auto_increment%")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&columnName); err != nil {
			return "", err
		}
		break
	}

	return columnName, nil
}

func (driver mysqlDriver) ExtractIndexNames(conn *sql.DB, tableName string) (indexNames []string, err error) {
	dbName, err := extractDBName(conn)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(`
	SELECT 
		DISTINCT INDEX_NAME 
	FROM 
		INFORMATION_SCHEMA.STATISTICS 
	WHERE 
		TABLE_SCHEMA=? AND TABLE_NAME=?`, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		indexName := ""
		if err = rows.Scan(&indexName); err != nil {
			return nil, err
		}
		indexNames = append(indexNames, indexName)
	}
	return indexNames, nil

}

func (driver mysqlDriver) ExtractIndex(conn *sql.DB, tableName string, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error) {
	dbName, err := extractDBName(conn)
	if err != nil {
		return nil, false, false, err
	}

	rows, err := conn.Query(`
	SELECT 
		NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX 
	FROM
		INFORMATION_SCHEMA.STATISTICS
	WHERE
		TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=?
	ORDER BY SEQ_IN_INDEX`, dbName, tableName, indexName)
	if err != nil {
		return nil, false, false, err
	}
	defer rows.Close()

	nonUnique := true
	prevSeq := 0
	for rows.Next() {
		columnName := ""
		seq := 0
		if err := rows.Scan(&nonUnique, &columnName, &seq); err != nil {
			return nil, false, false, err
		}

		// Check seq.
		if seq != prevSeq+1 {
			panic(fmt.Errorf("Bad SEQ_IN_INDEX, prev is %d, current is %d", prevSeq, seq))
		}
		prevSeq = seq

		columnNames = append(columnNames, columnName)
	}

	if len(columnNames) == 0 {
		return nil, false, false, fmt.Errorf("Index %+q in table %+q not found", indexName, tableName)
	}

	// https://dev.mysql.com/doc/refman/5.7/en/create-table.html
	// The name of a PRIMARY KEY is always PRIMARY, which thus cannot be used as the name for any other kind of index
	isPrimary = indexName == "PRIMARY"
	isUnique = !nonUnique
	return
}

func (driver mysqlDriver) ExtractFKNames(conn *sql.DB, tableName string) (fkNames []string, err error) {
	dbName, err := extractDBName(conn)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(`
	SELECT
		CONSTRAINT_NAME
	FROM
		INFORMATION_SCHEMA.TABLE_CONSTRAINTS
	WHERE
		TABLE_SCHEMA=? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'FOREIGN KEY'`, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		fkName := ""
		if err := rows.Scan(&fkName); err != nil {
			return nil, err
		}
		fkNames = append(fkNames, fkName)
	}
	return fkNames, nil
}

func (driver mysqlDriver) ExtractFK(conn *sql.DB, tableName string, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error) {
	dbName, err := extractDBName(conn)
	if err != nil {
		return nil, "", nil, err
	}

	rows, err := conn.Query(`
		SELECT
			COLUMN_NAME, ORDINAL_POSITION, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
		FROM
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
			TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME=? ORDER BY ORDINAL_POSITION`, dbName, tableName, fkName)
	if err != nil {
		return nil, "", nil, err
	}
	defer rows.Close()

	prevPos := 0
	for rows.Next() {
		columnName := ""
		refColumnName := ""
		pos := 0
		if err := rows.Scan(&columnName, &pos, &refTableName, &refColumnName); err != nil {
			return nil, "", nil, err
		}

		// Check pos.
		if pos != prevPos+1 {
			panic(fmt.Errorf("Bad ORDINAL_POSITION, prev is %d, current is %d", prevPos, pos))
		}
		prevPos = pos

		columnNames = append(columnNames, columnName)
		refColumnNames = append(refColumnNames, refColumnName)
	}

	if len(columnNames) == 0 {
		return nil, "", nil, fmt.Errorf("FK %+q in table %+q not found", fkName, tableName)
	}

	return columnNames, refTableName, refColumnNames, nil

}

func (driver mysqlDriver) PrimitiveScanType(typ *sql.ColumnType) (string, error) {
	switch typ.ScanType() {
	case scanTypeFloat32:
		return "float32", nil
	case scanTypeFloat64, scanTypeNullFloat:
		return "float64", nil
	case scanTypeInt8:
		return "int8", nil
	case scanTypeInt16:
		return "int16", nil
	case scanTypeInt32:
		return "int32", nil
	case scanTypeInt64, scanTypeNullInt:
		return "int64", nil
	case scanTypeUint8:
		return "uint8", nil
	case scanTypeUint16:
		return "uint16", nil
	case scanTypeUint32:
		return "uint32", nil
	case scanTypeUint64:
		return "uint64", nil
	case scanTypeRawBytes:
		return "[]byte", nil
	case scanTypeNullTime:
		return "time.Time", nil
	}
	return "", fmt.Errorf("Not support column type %s", typ.ScanType().String())
}

func extractDBName(conn *sql.DB) (string, error) {
	dbName := ""
	err := conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
	return dbName, err
}

func init() {
	driver.RegistDrv("mysql", mysqlDriver{})
}
