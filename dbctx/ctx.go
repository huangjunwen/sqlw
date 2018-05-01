package dbctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

// DBCtx contains database related information.
type DBCtx struct {
	driverName     string
	dataSourceName string
	conn           *sql.DB
	drv            driver.Drv
	db             *DBInfo
}

// NewDBCtx creates DBCtx: connects to a database and extract information from it.
func NewDBCtx(driverName, dataSourceName string) (*DBCtx, error) {
	conn, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	drv := driver.GetDrv(driverName)
	if drv == nil {
		return nil, fmt.Errorf("Unsupported driver %+q", driverName)
	}

	db, err := newDBInfo(conn, drv)
	if err != nil {
		return nil, err
	}

	return &DBCtx{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		drv:            drv,
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

// Conn returns the database connection.
func (ctx *DBCtx) Conn() *sql.DB {
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
}
