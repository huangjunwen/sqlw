package dbctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlw/driver"
)

type DBContext struct {
	driverName     string
	dataSourceName string
	db             *sql.DB
	drv            driver.Driver
	dbInfo         *DBInfo
}

func NewDBContext(driverName, dataSourceName string) (*DBContext, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	drv := driver.GetDriver(driverName)
	if drv == nil {
		return nil, fmt.Errorf("Unsupported driver %+q", driverName)
	}

	dbInfo, err := newDBInfo(db, drv)
	if err != nil {
		return nil, err
	}

	return &DBContext{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		drv:            drv,
		db:             db,
		dbInfo:         dbInfo,
	}, nil

}

func (ctx *DBContext) DriverName() string {
	return ctx.driverName
}

func (ctx *DBContext) DataSourceName() string {
	return ctx.dataSourceName
}

func (ctx *DBContext) DB() *sql.DB {
	return ctx.db
}

func (ctx *DBContext) Drv() driver.Driver {
	return ctx.drv
}

func (ctx *DBContext) DBInfo() *DBInfo {
	return ctx.dbInfo
}
