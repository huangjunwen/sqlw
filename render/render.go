package render

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/dbctx"
)

// Renderer is used for generating code.
type Renderer struct {
	// Options
	ctx       *dbctx.DBContext
	tmplFS    http.FileSystem
	stmtDir   string
	outputDir string
	outputPkg string

	scanTypeMap ScanTypeMap
	templates   map[string]*template.Template
}

// NewRenderer create new Renderer.
func NewRenderer(opts ...Option) (*Renderer, error) {
	r := &Renderer{
		templates:   make(map[string]*template.Template),
		scanTypeMap: DefaultScanTypeMap,
	}
	for _, op := range opts {
		if err := op(r); err != nil {
			return nil, err
		}
	}

	if r.ctx == nil {
		return nil, fmt.Errorf("Missing DBContext")
	}
	if r.tmplFS == nil {
		return nil, fmt.Errorf("Missing FS")
	}
	if r.outputDir == "" {
		return nil, fmt.Errorf("Missing output directory")
	}

	if r.outputPkg == "" {
		r.outputPkg = path.Base(r.outputDir)
	}

	return r, nil

}

func (r *Renderer) render(tmplName, fileName string, data interface{}) error {

	// Open template if not exists.
	tmpl := r.templates[tmplName]
	if tmpl == nil {
		tmplFile, err := r.tmplFS.Open(tmplName)
		if err != nil {
			return err
		}

		tmplContent, err := ioutil.ReadAll(tmplFile)
		if err != nil {
			return err
		}

		tmpl, err = template.New(tmplName).Funcs(r.funcMap()).Parse(string(tmplContent))
		if err != nil {
			return err
		}

		r.templates[tmplName] = tmpl
	}

	// Open output file.
	file, err := os.OpenFile(path.Join(r.outputDir, fileName), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Render.
	buf := &bytes.Buffer{}
	if err := tmpl.Execute(buf, data); err != nil {
		return err
	}

	// Format.
	fmtBuf, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}

	// Write.
	_, err = file.Write(fmtBuf)
	if err != nil {
		return err
	}

	return nil

}

// Run generate code.
func (r *Renderer) Run() error {

	// Parse manifest.
	manifestFile, err := r.tmplFS.Open("/manifest.json")
	if err != nil {
		return err
	}
	defer manifestFile.Close()

	manifest, err := NewManifest(manifestFile)
	if err != nil {
		return err
	}

	// Load custom scan type map.
	if manifest.ScanTypeMap != "" {
		scanTypeMapFile, err := r.tmplFS.Open(manifest.ScanTypeMap)
		if err != nil {
			return err
		}
		defer scanTypeMapFile.Close()

		scanTypeMap := NewScanTypeMap()
		if err := scanTypeMap.Load(scanTypeMapFile); err != nil {
			return err
		}

		r.scanTypeMap = r.scanTypeMap.Merge(scanTypeMap)
	}

	// Render tables.
	if manifest.TableTemplate == "" {
		return fmt.Errorf("Missing 'table_tmpl' in manifest.json")
	}
	for _, table := range r.ctx.DB().Tables() {
		if err := r.render(manifest.TableTemplate, table.TableName()+".go", map[string]interface{}{
			"Table":       table,
			"DBContext":   r.ctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}
	}

	// TODO Render statements.

	// Render extra files.
	for _, tmplName := range manifest.ExtraTemplates {
		// Render.
		fileName := strings.Split(tmplName, ".")[0] + ".go"
		if err := r.render(tmplName, fileName, map[string]interface{}{
			"DBContext":   r.ctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}
	}

	return nil
}
