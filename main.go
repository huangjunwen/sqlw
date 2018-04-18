//go:generate esc -o template.go templates
package main

import (
	"flag"
	"log"
	"path"

	"github.com/huangjunwen/sqlw/dbctx"
	_ "github.com/huangjunwen/sqlw/driver/mysql"
	"github.com/huangjunwen/sqlw/render"
	"net/http"
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
	return fs.fs.Open(path.Join("/templates", fs.prefix, name))
}

var (
	driverName     string
	dataSourceName string
	outputPackage  string
	outputDir      string
)

func main() {
	flag.StringVar(&driverName, "driver", "mysql", "Driver name. (e.g. 'mysql')")
	flag.StringVar(&dataSourceName, "dsn", "root:123456@tcp(localhost:3306)/dev?parseTime=true", "Data source name. ")
	flag.StringVar(&outputPackage, "pkg", "models", "Package name of the generated code.")
	flag.StringVar(&outputDir, "out", "models", "Output directory for generated code.")
	flag.Parse()
	if driverName == "" {
		log.Fatalf("Missing -driver")
	}
	if dataSourceName == "" {
		log.Fatalf("Missing -dsn")
	}

	ctx, err := dbctx.NewDBContext(driverName, dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer ctx.Close()

	renderer := &render.Renderer{
		DBContext:     ctx,
		TemplateFS:    newPrefixFS(ctx.DriverName(), FS(false)),
		OutputDir:     outputDir,
		OutputPackage: outputPackage,
	}
	if err := renderer.Run(); err != nil {
		log.Fatal(err)
	}

	return
}
