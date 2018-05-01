package render

import (
	"fmt"
	"github.com/huangjunwen/sqlw/dbcontext"
	"net/http"
	"os"
	"path"
)

// Option is used to create Renderer.
type Option func(*Renderer) error

// DBCtx sets the database context. (required)
func DBCtx(ctx *dbcontext.DBCtx) Option {
	return func(r *Renderer) error {
		r.ctx = ctx
		return nil
	}
}

// TmplFS sets the template directory. (required)
func TmplFS(tmplFS http.FileSystem) Option {
	return func(r *Renderer) error {
		r.tmplFS = tmplFS
		return nil
	}
}

// StmtDir sets the directory containing statement XMLs.
func StmtDir(stmtDir string) Option {
	return func(r *Renderer) error {
		p := path.Clean(stmtDir)
		fi, err := os.Stat(p)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("%+q is not a directory", p)
		}
		r.stmtDir = p
		return nil
	}
}

// OutputDir sets the directory for generated code. (required)
func OutputDir(outputDir string) Option {
	return func(r *Renderer) error {
		if outputDir == "" {
			return fmt.Errorf("OutputDir is empty")
		}
		p := path.Clean(outputDir)
		if p[len(p)-1] == '/' {
			return fmt.Errorf("OutputDir can't be '/''")
		}
		if err := os.MkdirAll(p, 0755); err != nil {
			return err
		}
		r.outputDir = p
		return nil
	}
}

// OutputPackage sets an alternative package name for generated code.
func OutputPackage(outputPkg string) Option {
	return func(r *Renderer) error {
		r.outputPkg = outputPkg
		return nil
	}
}
