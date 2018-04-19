package render

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"text/template"

	"github.com/huangjunwen/sqlw/dbctx"
)

type Renderer struct {
	// Database context.
	DBContext *dbctx.DBContext

	// StmtXMLDir specified the directory containing statement xmls.
	StmtXMLDir string

	// TemplateFS contains template files.
	TemplateFS http.FileSystem

	// OutputDir specified output package's directory, default "models".
	OutputDir string

	// OutputPackage specified an alternative package name if not empty.
	OutputPackage string
}

func (r *Renderer) Run() error {

	// Clean output directory.
	if r.OutputDir == "" {
		r.OutputDir = "models"
	}
	r.OutputDir = path.Clean(r.OutputDir)
	if r.OutputDir[len(r.OutputDir)-1] == '/' {
		return fmt.Errorf("Output directory can't be '/'")
	}

	// Mkdir output directory.
	if err := os.MkdirAll(r.OutputDir, 0755); err != nil {
		return err
	}

	// Determine output package name.
	if r.OutputPackage == "" {
		r.OutputPackage = path.Base(r.OutputDir)
	}

	// Render tables.
	tableTemplate, err := r.openTemplate("table")
	if err != nil {
		return err
	}
	for _, table := range r.DBContext.DB().Tables() {
		// Render table.
		buf := &bytes.Buffer{}
		if err := tableTemplate.Execute(buf, map[string]interface{}{
			"Table":       table,
			"DBContext":   r.DBContext,
			"PackageName": r.OutputPackage,
		}); err != nil {
			return err
		}

		// Format
		fmtBuf, err := format.Source(buf.Bytes())
		if err != nil {
			return err
		}

		// Write file.
		out, err := r.openOutputFile(table.TableName())
		if err != nil {
			return err
		}
		_, err = out.Write(fmtBuf)
		if err != nil {
			return err
		}
		out.Close()

	}

	// TODO

	return nil
}

func (r *Renderer) openTemplate(name string) (*template.Template, error) {
	templateFile, err := r.TemplateFS.Open(fmt.Sprintf("%s.tmpl", name))
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(templateFile)
	if err != nil {
		return nil, err
	}

	t, err := template.New(name).Funcs(funcMap(r.DBContext)).Parse(string(b))
	if err != nil {
		return nil, err
	}

	return t, nil

}

func (r *Renderer) openOutputFile(name string) (*os.File, error) {
	p := path.Join(r.OutputDir, fmt.Sprintf("%s.go", name))
	return os.OpenFile(p, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
}
