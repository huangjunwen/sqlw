package dbctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

// DBContext contains database related objects.
type DBContext struct {
	driverName     string
	dataSourceName string
	conn           *sql.DB
	drv            driver.Drv
	db             *DBInfo
}

// NewDBContext creates DBContext: connects to a database and extract information from it.
func NewDBContext(driverName, dataSourceName string) (*DBContext, error) {
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

	return &DBContext{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		drv:            drv,
		conn:           conn,
		db:             db,
	}, nil

}

// DriverName returns database type (e.g. "mysql")
func (ctx *DBContext) DriverName() string {
	return ctx.driverName
}

// DataSourceName returns the DSN of the database connected to.
func (ctx *DBContext) DataSourceName() string {
	return ctx.dataSourceName
}

// Conn returns the database connection.
func (ctx *DBContext) Conn() *sql.DB {
	return ctx.conn
}

// Drv returns sqlw database driver.
func (ctx *DBContext) Drv() driver.Drv {
	return ctx.drv
}

// DB returns the extracted database information.
func (ctx *DBContext) DB() *DBInfo {
	return ctx.db
}

// Close and release resource.
func (ctx *DBContext) Close() {
	ctx.conn.Close()
}
