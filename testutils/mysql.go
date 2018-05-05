package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/ory-am/dockertest.v3"
)

const (
	defaultVersion = "5.7.21"
)

type noopLogger struct{}

func (l noopLogger) Print(v ...interface{}) {}

type mysqlConnCtxKeyType struct{}

var mysqlConnCtxKey mysqlConnCtxKeyType

// MysqlConn returns the mysql connection stored in context if exists.
func MysqlConn(ctx context.Context) *sql.DB {
	v := ctx.Value(mysqlConnCtxKey)
	if v == nil {
		return nil
	}
	return v.(*sql.DB)
}

// WithMysqlConn is a middelware to fn and add a usable mysql connection object to the context.
func WithMysqlConn(fn func(context.Context) error) func(context.Context) error {

	return func(ctx context.Context) (err error) {

		pool := GetPool()
		var (
			resource *dockertest.Resource
			conn     *sql.DB
		)

		// Determin mysql server version.
		ver := os.Getenv("MYSQL_VERSION")
		if ver == "" {
			ver = defaultVersion
			log.Printf("[testutils][mysql] Enviroment MYSQL_VERSION not found, use default version %+q\n", defaultVersion)
		} else {
			log.Printf("[testutils][mysql] Enviroment MYSQL_VERSION found, use version %+q\n", ver)
		}

		// Start container.
		log.Printf("[testutils][mysql] Starting mysql server...\n")
		resource, err = pool.Run("mysql", ver, []string{"MYSQL_ROOT_PASSWORD=123456"})
		if err != nil {
			return
		}

		// Defer purge container.
		defer func() {
			log.Printf("[testutils][mysql] Ready to purge mysql server...\n")
			err = pool.Purge(resource)
		}()

		// Wait and connect.
		log.Printf("[testutils][mysql] Waiting for mysql server...\n")
		// NOTE: Suppress logging
		mysql.SetLogger(noopLogger{})
		if err = pool.Retry(func() error {
			var e error
			conn, e = sql.Open("mysql", fmt.Sprintf("root:123456@(localhost:%s)/mysql", resource.GetPort("3306/tcp")))
			if e != nil {
				return e
			}
			if e = conn.Ping(); e != nil {
				conn.Close()
				return e
			}
			return nil
		}); err != nil {
			return
		}
		defer conn.Close()

		// Done.
		log.Printf("[testutils][mysql] Connected\n")
		ctx2 := context.WithValue(ctx, mysqlConnCtxKey, conn)
		return fn(ctx2)
	}

}
