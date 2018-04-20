package render

import (
	"bufio"
	"bytes"
	"fmt"
	"go/format"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/dbctx"
)

const (
	tableTmplName = "table.tmpl" // Used to render each table.
	stmtTmplName  = "stmt.tmpl"  // Used to render each statement.
)

// Renderer is used for generating code.
type Renderer struct {
	// Options
	ctx       *dbctx.DBContext
	tmplFS    http.FileSystem
	stmtDir   string
	outputDir string
	outputPkg string

	templates map[string]*template.Template
}

// NewRenderer create new Renderer.
func NewRenderer(opts ...Option) (*Renderer, error) {
	r := &Renderer{
		templates: make(map[string]*template.Template),
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

		tmpl, err = template.New(tmplName).Funcs(funcMap(r.ctx)).Parse(string(tmplContent))
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

	// Render tables.
	for _, table := range r.ctx.DB().Tables() {
		if err := r.render(tableTmplName, table.TableName()+".go", map[string]interface{}{
			"Table":       table,
			"DBContext":   r.ctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}
	}

	// TODO Render statements.

	// Render extra files.

	// NOTE: Since esc (and other static assets embedders as well) does not support Readdir,
	// i need to use an extra file to record template file names.
	extraTmpls, err := r.tmplFS.Open("/extra_tmpls")
	if err != nil {
		// Ignore this error
		return nil
	}
	defer extraTmpls.Close()

	reader := bufio.NewReader(extraTmpls)
	end := false
	for !end {
		// Each line should contain one template file name.
		fileName, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			end = true
		}

		fileName = strings.TrimSpace(fileName)
		if len(fileName) == 0 {
			continue
		}

		// Not ends with ".tmpl"
		if !strings.HasSuffix(fileName, ".tmpl") {
			return fmt.Errorf("File name not ends with '.tmpl' in extra_tmpls")
		}

		// Render.
		tmplName := fileName
		fileName = fileName[:len(fileName)-5] + ".go"
		if err := r.render(tmplName, fileName, map[string]interface{}{
			"DBContext":   r.ctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}

	}

	return nil
}
