package dbcontext

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

// DBCtx contains database related information.
type DBCtx struct {
	driverName     string
	dataSourceName string
	connPool       *sql.DB
	conn           *sql.Conn
	drv            driver.Drv
	db             *DBInfo
}

// NewDBCtx creates DBCtx: connects to a database and extract information from it.
func NewDBCtx(driverName, dataSourceName string) (*DBCtx, error) {
	drv := driver.GetDrv(driverName)
	if drv == nil {
		return nil, fmt.Errorf("Unsupported driver %+q", driverName)
	}

	connPool, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	conn, err := connPool.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	db, err := newDBInfo(conn, drv)
	if err != nil {
		return nil, err
	}

	return &DBCtx{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		drv:            drv,
		connPool:       connPool,
		conn:           conn,
		db:             db,
	}, nil

}

// DriverName returns database type (e.g. "mysql")
func (ctx *DBCtx) DriverName() string {
	return ctx.driverName
}

// DataSourceName returns the DSN of the database connected to.
func (ctx *DBCtx) DataSourceName() string {
	return ctx.dataSourceName
}

// ConnPool returns the database connection pool.
func (ctx *DBCtx) ConnPool() *sql.DB {
	return ctx.connPool
}

// Conn returns a database connection.
func (ctx *DBCtx) Conn() *sql.Conn {
	return ctx.conn
}

// Drv returns sqlw database driver.
func (ctx *DBCtx) Drv() driver.Drv {
	return ctx.drv
}

// DB returns the extracted database information.
func (ctx *DBCtx) DB() *DBInfo {
	return ctx.db
}

// Close and release resource.
func (ctx *DBCtx) Close() {
	ctx.conn.Close()
	ctx.connPool.Close()
}
