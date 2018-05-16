package render

import (
	"fmt"
	"net/http"
	"os"
	"path"

	"github.com/huangjunwen/sqlw/datasrc"
)

// Option is used to create Renderer.
type Option func(*Renderer) error

// Loader sets the loader. (required)
func Loader(loader *datasrc.Loader) Option {
	return func(r *Renderer) error {
		r.loader = loader
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

// OutputPkg sets an alternative package name for generated code.
func OutputPkg(outputPkg string) Option {
	return func(r *Renderer) error {
		r.outputPkg = outputPkg
		return nil
	}
}

// Whitelist sets the whitelist of table names to render.
func Whitelist(whitelist []string) Option {
	return func(r *Renderer) error {
		r.whitelist = make(map[string]struct{})
		for _, tableName := range whitelist {
			r.whitelist[tableName] = struct{}{}
		}
		return nil
	}
}

// Blacklist sets the blacklist of table names not to render.
func Blacklist(blacklist []string) Option {
	return func(r *Renderer) error {
		r.blacklist = make(map[string]struct{})
		for _, tableName := range blacklist {
			r.blacklist[tableName] = struct{}{}
		}
		return nil
	}
}
