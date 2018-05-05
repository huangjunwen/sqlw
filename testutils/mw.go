package testutils

import (
	"context"
)

// MW is a middleware type
type MW func(func(context.Context) error) func(context.Context) error

// Chain wraps fn with a stack of middlewares.
func Chain(fn func(context.Context) error, mws ...MW) func(context.Context) error {
	for i := len(mws) - 1; i >= 0; i-- {
		fn = mws[i](fn)
	}
	return fn
}
