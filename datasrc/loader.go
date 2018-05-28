package datasrc

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

// Loader is used to load information from a database.
type Loader struct {
	driverName     string
	dataSourceName string
	driver         Driver
	connPool       *sql.DB
	conn           *sql.Conn
}

// NewLoader creates a new Loader.
func NewLoader(driverName, dataSourceName string) (*Loader, error) {
	driver := GetDriver(driverName)
	if driver == nil {
		return nil, fmt.Errorf("Unsupported driverName %+q", driverName)
	}

	connPool, err := sql.Open(driverName, dataSourceName)
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

	return &Loader{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		driver:         driver,
		connPool:       connPool,
		conn:           conn,
	}, nil
}

// DriverName returns the driver's name.
func (loader *Loader) DriverName() string {
	return loader.driverName
}

// DataSourceName returns the dsn.
func (loader *Loader) DataSourceName() string {
	return loader.dataSourceName
}

// ConnPool returns the connection pool object.
func (loader *Loader) ConnPool() *sql.DB {
	return loader.connPool
}

// Conn returns a single connection object.
func (loader *Loader) Conn() *sql.Conn {
	return loader.conn
}

// Close release resource.
func (loader *Loader) Close() {
	loader.conn.Close()
	loader.connPool.Close()
}

// LoadQueryResultColumns returns result columns of a query.
func (loader *Loader) LoadQueryResultColumns(query string) (columns []*Column, err error) {
	return loader.driver.LoadQueryResultColumns(loader.conn, query)
}

// LoadTableNames returns all table names in current database.
func (loader *Loader) LoadTableNames() (tableNames []string, err error) {
	return loader.driver.LoadTableNames(loader.conn)
}

// LoadColumns returns columns of a given table.
func (loader *Loader) LoadColumns(tableName string) (columns []*TableColumn, err error) {
	return loader.driver.LoadColumns(loader.conn, tableName)
}

// LoadIndexNames returns all index name for a given table.
func (loader *Loader) LoadIndexNames(tableName string) (indexNames []string, err error) {
	return loader.driver.LoadIndexNames(loader.conn, tableName)
}

// LoadIndex returns information of a given index.
func (loader *Loader) LoadIndex(tableName, indexName string) (columnNames []string, isPrimary bool, isUnique bool, err error) {
	return loader.driver.LoadIndex(loader.conn, tableName, indexName)
}

// LoadFKNames returns all foreign key constraint names for a given table.
func (loader *Loader) LoadFKNames(tableName string) (fkNames []string, err error) {
	return loader.driver.LoadFKNames(loader.conn, tableName)
}

// LoadFK returns information of a given foreign key constraint.
func (loader *Loader) LoadFK(tableName, fkName string) (columnNames []string, refTableName string, refColumnNames []string, err error) {
	return loader.driver.LoadFK(loader.conn, tableName, fkName)
}

// LoadAutoIncColumn returns the 'auto increament' column's name for a given table or "" if not found.
func (loader *Loader) LoadAutoIncColumn(tableName string) (columnName string, err error) {
	if driver, ok := loader.driver.(DriverWithAutoInc); ok {
		return driver.LoadAutoIncColumn(loader.conn, tableName)
	}
	return "", nil
}

// DataTypes returns full list of driver-specific type identifiers used in Column.DataType.
func (loader *Loader) DataTypes() []string {
	return loader.driver.DataTypes()
}

// Quote returns the quoted identifier.
func (loader *Loader) Quote(identifier string) string {
	return loader.driver.Quote(identifier)
}
