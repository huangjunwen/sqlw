//go:generate esc -o template.go template
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	_ "github.com/huangjunwen/sqlw/datasrc/drivers/mysql"

	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/render"
)

type commaSeperatd []string

func (cs *commaSeperatd) String() string {
	return strings.Join(*cs, ",")
}

func (cs *commaSeperatd) Set(s string) error {
	*cs = strings.Split(s, ",")
	return nil
}

var (
	driverName     string
	dataSourceName string
	outputDir      string
	outputPkg      string
	stmtDir        string
	tmplDir        string
	whitelist      commaSeperatd
	blacklist      commaSeperatd
)

func main() {
	// Parse flags.
	flag.StringVar(&driverName, "driver", "mysql", "Driver name. (e.g. 'mysql')")
	flag.StringVar(&dataSourceName, "dsn", "root:123456@tcp(localhost:3306)/dev?parseTime=true", "Data source name. ")
	flag.StringVar(&outputDir, "out", "models", "Output directory for generated code.")
	flag.StringVar(&outputPkg, "pkg", "", "Alternative package name of the generated code.")
	flag.StringVar(&stmtDir, "stmt", "", "Statement xml directory.")
	flag.StringVar(&tmplDir, "tmpl", "", "Custom templates directory.")
	flag.Var(&whitelist, "whitelist", "Comma seperated table names to render.")
	flag.Var(&blacklist, "blacklist", "Comma seperated table names not to render.")
	flag.Parse()
	if driverName == "" {
		log.Fatalf("Missing -driver")
	}
	if dataSourceName == "" {
		log.Fatalf("Missing -dsn")
	}

	// Create loader.
	loader, err := datasrc.NewLoader(driverName, dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer loader.Close()

	// Choose template.
	fs := http.FileSystem(nil)
	if tmplDir != "" {
		fs = http.Dir(tmplDir)
	} else {
		fs = newPrefixFS(loader.DriverName(), FS(false))
	}

	// Create Renderer.
	renderer, err := render.NewRenderer(
		render.Loader(loader),
		render.OutputDir(outputDir),
		render.OutputPkg(outputPkg),
		render.StmtDir(stmtDir),
		render.TmplFS(fs),
		render.Whitelist([]string(whitelist)),
		render.Blacklist([]string(blacklist)),
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
