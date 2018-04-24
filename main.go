//go:generate esc -o template.go template
package main

import (
	"flag"
	"log"
	"net/http"
	"path"

	_ "github.com/huangjunwen/sqlw/driver/mysql"

	"github.com/huangjunwen/sqlw/dbctx"
	"github.com/huangjunwen/sqlw/render"
	_ "github.com/huangjunwen/sqlw/stmt/directive"
)

type PrefixFS struct {
	prefix string
	fs     http.FileSystem
}

func newPrefixFS(prefix string, fs http.FileSystem) *PrefixFS {
	return &PrefixFS{
		prefix: prefix,
		fs:     fs,
	}
}

func (fs *PrefixFS) Open(name string) (http.File, error) {
	return fs.fs.Open(path.Join("/template", fs.prefix, name))
}

var (
	driverName     string
	dataSourceName string
	outputDir      string
	outputPackage  string
	stmtDir        string
	tmplDir        string
)

func main() {
	// Parse flags.
	flag.StringVar(&driverName, "driver", "mysql", "Driver name. (e.g. 'mysql')")
	flag.StringVar(&dataSourceName, "dsn", "root:123456@tcp(localhost:3306)/dev?parseTime=true", "Data source name. ")
	flag.StringVar(&outputDir, "out", "models", "Output directory for generated code.")
	flag.StringVar(&outputPackage, "pkg", "", "Alternative package name of the generated code.")
	flag.StringVar(&stmtDir, "stmt", "", "Statement xml directory.")
	flag.StringVar(&tmplDir, "tmpl", "", "Custom templates directory.")
	flag.Parse()
	if driverName == "" {
		log.Fatalf("Missing -driver")
	}
	if dataSourceName == "" {
		log.Fatalf("Missing -dsn")
	}

	// Extract database information.
	ctx, err := dbctx.NewDBContext(driverName, dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer ctx.Close()

	// Choose template.
	fs := http.FileSystem(nil)
	if tmplDir != "" {
		fs = http.Dir(tmplDir)
	} else {
		fs = newPrefixFS(ctx.DriverName(), FS(false))
	}

	// Render.
	renderer, err := render.NewRenderer(
		render.DBContext(ctx),
		render.OutputDir(outputDir),
		render.OutputPackage(outputPackage),
		render.StmtDir(stmtDir),
		render.TemplateFS(fs),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Run!
	if err := renderer.Run(); err != nil {
		log.Fatal(err)
	}

	return
}
