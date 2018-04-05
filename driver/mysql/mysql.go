package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/huangjunwen/sqlwrapper/driver"
)

type mysqlDriver struct{}

func (driver mysqlDriver) ExtractTableNames(db *sql.DB) (tableNames []string, err error) {
	rows, err := db.Query("SHOW TABLES")
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

func (driver mysqlDriver) ExtractColumns(db *sql.DB, tableName string) (columnNames []string, columnTypes []*sql.ColumnType, err error) {
	rows, err := db.Query("SELECT * FROM " + tableName)
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

func (driver mysqlDriver) ExtractIndexNames(db *sql.DB, tableName string) (indexNames []string, err error) {
	dbName, err := extractDBName(db)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(`
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

func (driver mysqlDriver) ExtractIndex(db *sql.DB, tableName string, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error) {
	dbName, err := extractDBName(db)
	if err != nil {
		return nil, false, false, err
	}

	rows, err := db.Query(`
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

func (driver mysqlDriver) ExtractFKNames(db *sql.DB, tableName string) (fkNames []string, err error) {
	dbName, err := extractDBName(db)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(`
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

func (driver mysqlDriver) ExtractFK(db *sql.DB, tableName string, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error) {
	dbName, err := extractDBName(db)
	if err != nil {
		return nil, "", nil, err
	}

	rows, err := db.Query(`
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

func extractDBName(db *sql.DB) (string, error) {
	dbName := ""
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	return dbName, err
}

func init() {
	driver.RegistDriver("mysql", mysqlDriver{})
}