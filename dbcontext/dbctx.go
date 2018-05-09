package dbcontext

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

var (
	// ConnectTimeout is the database connection timeout.
	ConnectTimeout = time.Second * 5
)

// DBCtx represents the database context the code generator is running with.
type DBCtx struct {
	name     string
	dsn      string
	drv      Drv
	connPool *sql.DB
	conn     *sql.Conn
}

// NewDBCtx creates a new DBCtx.
func NewDBCtx(name, dsn string) (*DBCtx, error) {
	drv := GetDrv(name)
	if drv == nil {
		return nil, fmt.Errorf("Unsupported driver name %+q", name)
	}

	connPool, err := sql.Open(name, dsn)
	if err != nil {
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), ConnectTimeout)
	if err != nil {
		return nil, err
	}
	conn, err := connPool.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return &DBCtx{
		name:     name,
		dsn:      dsn,
		drv:      drv,
		connPool: connPool,
		conn:     conn,
	}, nil

}

// Name returns the driver name.
func (dbctx *DBCtx) Name() string {
	return dbctx.name
}

// DSN returns the data source name.
func (dbctx *DBCtx) DSN() string {
	return dbctx.dsn
}

// Drv returns Drv object.
func (dbctx *DBCtx) Drv() Drv {
	return dbctx.drv
}

// ConnPool returns the connection pool object.
func (dbctx *DBCtx) ConnPool() *sql.DB {
	return dbctx.connPool
}

// Conn returns a single connection object.
func (dbctx *DBCtx) Conn() *sql.Conn {
	return dbctx.conn
}

// Close release resource.
func (dbctx *DBCtx) Close() {
	dbctx.conn.Close()
	dbctx.connPool.Close()
}

// ExtractQueryResultColumns returns result columns of a query.
func (dbctx *DBCtx) ExtractQueryResultColumns(query string) (columns []Column, err error) {
	return dbctx.drv.ExtractQueryResultColumns(dbctx.conn, query)
}

// ExtractTableNames returns all table names in current database.
func (dbctx *DBCtx) ExtractTableNames() (tableNames []string, err error) {
	return dbctx.drv.ExtractTableNames(dbctx.conn)
}

// ExtractColumns returns columns of a given table.
func (dbctx *DBCtx) ExtractColumns(tableName string) (columns []Column, err error) {
	return dbctx.drv.ExtractColumns(dbctx.conn, tableName)
}

// ExtractIndexNames returns all index name for a given table.
func (dbctx *DBCtx) ExtractIndexNames(tableName string) (indexNames []string, err error) {
	return dbctx.drv.ExtractIndexNames(dbctx.conn, tableName)
}

// ExtractIndex returns information of a given index.
func (dbctx *DBCtx) ExtractIndex(tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error) {
	return dbctx.drv.ExtractIndex(dbctx.conn, tableName, indexName)
}

// ExtractFKNames returns all foreign key constraint names for a given table.
func (dbctx *DBCtx) ExtractFKNames(tableName string) (fkNames []string, err error) {
	return dbctx.drv.ExtractFKNames(dbctx.conn, tableName)
}

// ExtractFK returns information of a given foreign key constraint.
func (dbctx *DBCtx) ExtractFK(tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error) {
	return dbctx.drv.ExtractFK(dbctx.conn, tableName, fkName)
}

// ExtractAutoIncColumn returns the 'auto increament' column's name for a given table or "" if not found or not supported.
func (dbctx *DBCtx) ExtractAutoIncColumn(tableName string) (ColumnName string, err error) {
	if drv, ok := dbctx.drv.(DrvWithAutoInc); ok {
		return drv.ExtractAutoIncColumn(dbctx.conn, tableName)
	}
	return "", nil
}
