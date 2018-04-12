package ctx

import (
	"database/sql"
	"fmt"
	"github.com/huangjunwen/sqlwrapper/driver"
)

type Ctx struct {
	driverName     string
	dataSourceName string
	db             *sql.DB
	drv            driver.Driver
	dbInfo         *DBInfo
}

func NewCtx(driverName, dataSourceName string) (*Ctx, error) {
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

	return &Ctx{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		drv:            drv,
		db:             db,
		dbInfo:         dbInfo,
	}, nil

}

func (ctx *Ctx) DriverName() string {
	return ctx.driverName
}

func (ctx *Ctx) DataSourceName() string {
	return ctx.dataSourceName
}

func (ctx *Ctx) DB() *sql.DB {
	return ctx.db
}

func (ctx *Ctx) Drv() driver.Driver {
	return ctx.drv
}

func (ctx *Ctx) DBInfo() *DBInfo {
	return ctx.dbInfo
}
