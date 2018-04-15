package dbctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

type DBContext struct {
	driverName     string
	dataSourceName string
	conn           *sql.DB
	drv            driver.Driver
	db             *DBInfo
}

func NewDBContext(driverName, dataSourceName string) (*DBContext, error) {
	conn, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	drv := driver.GetDriver(driverName)
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

func (ctx *DBContext) DriverName() string {
	return ctx.driverName
}

func (ctx *DBContext) DataSourceName() string {
	return ctx.dataSourceName
}

func (ctx *DBContext) Conn() *sql.DB {
	return ctx.conn
}

func (ctx *DBContext) Drv() driver.Driver {
	return ctx.drv
}

func (ctx *DBContext) DB() *DBInfo {
	return ctx.db
}

func (ctx *DBContext) Close() {
	ctx.conn.Close()
}
