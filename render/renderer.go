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

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/dbcontext"
	"github.com/huangjunwen/sqlw/statement"
)

// Renderer is used for generating code.
type Renderer struct {
	// Options
	dbctx     *dbcontext.DBCtx
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
		templates: make(map[string]*template.Template),
	}
	for _, op := range opts {
		if err := op(r); err != nil {
			return nil, err
		}
	}

	if r.dbctx == nil {
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

	// Load scan type map.
	if manifest.ScanTypeMap == "" {
		return fmt.Errorf("Missing 'scan_type_map' in manifest.json")
	}
	scanTypeMapFile, err := r.tmplFS.Open(manifest.ScanTypeMap)
	if err != nil {
		return err
	}
	defer scanTypeMapFile.Close()

	scanTypeMap, err := NewScanTypeMap(scanTypeMapFile)
	if err != nil {
		return err
	}
	r.scanTypeMap = scanTypeMap

	// Render tables.
	if manifest.TableTemplate == "" {
		return fmt.Errorf("Missing 'table_tmpl' in manifest.json")
	}
	for _, table := range r.dbctx.DB().Tables() {
		if err := r.render(manifest.TableTemplate, "table_"+table.TableName()+".go", map[string]interface{}{
			"Table":       table,
			"DBContext":   r.dbctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}
	}

	// Render statements.
	if manifest.StmtTemplate == "" {
		return fmt.Errorf("Missing 'stmt_tmpl' in manifest.json")
	}
	if r.stmtDir != "" {
		stmtFileInfos, err := ioutil.ReadDir(r.stmtDir)
		if err != nil {
			return err
		}
		for _, stmtFileInfo := range stmtFileInfos {
			if stmtFileInfo.IsDir() {
				continue
			}
			stmtFileName := stmtFileInfo.Name()
			if !strings.HasSuffix(stmtFileName, ".xml") {
				continue
			}
			doc := etree.NewDocument()
			if err := doc.ReadFromFile(path.Join(r.stmtDir, stmtFileName)); err != nil {
				return err
			}

			stmtInfos := []*statement.StmtInfo{}
			for _, elem := range doc.ChildElements() {
				stmtInfo, err := statement.NewStmtInfo(r.dbctx, elem)
				if err != nil {
					return err
				}
				stmtInfos = append(stmtInfos, stmtInfo)
			}

			if err := r.render(manifest.StmtTemplate, "stmt_"+strings.Split(stmtFileName, ".")[0]+".go", map[string]interface{}{
				"Stmts":       stmtInfos,
				"DBContext":   r.dbctx,
				"PackageName": r.outputPkg,
			}); err != nil {
				return err
			}

		}
	}

	// Render extra files.
	for _, tmplName := range manifest.ExtraTemplates {
		// Render.
		fileName := "extra_" + strings.Split(tmplName, ".")[0] + ".go"
		if err := r.render(tmplName, fileName, map[string]interface{}{
			"DBContext":   r.dbctx,
			"PackageName": r.outputPkg,
		}); err != nil {
			return err
		}
	}

	return nil
}