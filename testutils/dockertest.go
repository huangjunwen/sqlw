package testutils

import (
	"gopkg.in/ory-am/dockertest.v3"
)

var (
	pool *dockertest.Pool
)

// GetPool returns a global dockertest.Pool
func GetPool() *dockertest.Pool {
	if pool != nil {
		return pool
	}
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		panic(err)
	}
	return pool
}
