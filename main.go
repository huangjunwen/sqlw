//go:generate esc -o template.go template
package main

import (
	"flag"
	"log"
	"net/http"

	_ "github.com/huangjunwen/sqlw/driver/mysql"

	"github.com/huangjunwen/sqlw/dbcontext"
	"github.com/huangjunwen/sqlw/render"
)

var (
	driverName     string
	dataSourceName string
	outputDir      string
	outputPkg      string
	stmtDir        string
	tmplDir        string
)

func main() {
	// Parse flags.
	flag.StringVar(&driverName, "driver", "mysql", "Driver name. (e.g. 'mysql')")
	flag.StringVar(&dataSourceName, "dsn", "root:123456@tcp(localhost:3306)/dev?parseTime=true", "Data source name. ")
	flag.StringVar(&outputDir, "out", "models", "Output directory for generated code.")
	flag.StringVar(&outputPkg, "pkg", "", "Alternative package name of the generated code.")
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
	dbctx, err := dbcontext.NewDBCtx(driverName, dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer dbctx.Close()

	// Choose template.
	fs := http.FileSystem(nil)
	if tmplDir != "" {
		fs = http.Dir(tmplDir)
	} else {
		fs = newPrefixFS(dbctx.DriverName(), FS(false))
	}

	// Create Renderer.
	renderer, err := render.NewRenderer(
		render.DBCtx(dbctx),
		render.OutputDir(outputDir),
		render.OutputPkg(outputPkg),
		render.StmtDir(stmtDir),
		render.TmplFS(fs),
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
