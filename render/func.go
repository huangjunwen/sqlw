package render

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/infos"
	"github.com/huangjunwen/sqlw/infos/directives/arg"
	_ "github.com/huangjunwen/sqlw/infos/directives/repl"
	"github.com/huangjunwen/sqlw/infos/directives/vars"
	"github.com/huangjunwen/sqlw/infos/directives/wc"
)

var (
	identRe = regexp.MustCompile(`[^A-Za-z]*([A-Za-z])([A-Za-z0-9]*)`)
)

func camel(s string, upper bool) string {

	parts := []string{}

	for _, m := range identRe.FindAllStringSubmatch(s, -1) {

		first, remain := m[1], m[2]

		if len(parts) == 0 && !upper {
			first = strings.ToLower(first)
		} else {
			first = strings.ToUpper(first)
		}

		parts = append(parts, first, strings.ToLower(remain))
	}

	return strings.Join(parts, "")
}

func (r *Renderer) funcMap() template.FuncMap {

	scanType := func(val interface{}, idx int) (string, error) {

		col := (*datasrc.Column)(nil)

		switch v := val.(type) {
		case *datasrc.Column:
			col = v
		case *datasrc.TableColumn:
			col = &v.Column
		case *infos.ColumnInfo:
			col = &v.Col().Column
		default:
			return "", fmt.Errorf("ScanType: Expect table or query result column but got %T", val)
		}

		if col == nil {
			return "", fmt.Errorf("ScanType: Column is nil")
		}

		scanTypes, found := r.scanTypeMap[col.DataType]
		if !found {
			return "", fmt.Errorf("ScanType: Can't get scan type for %+q", col.DataType)
		}

		if idx < 0 {
			if col.HasNullable {
				if col.Nullable {
					idx = 1
				} else {
					idx = 0
				}
			} else {
				// NOTE: no HasNullable, then assume it is nullable since nullable type > not-nullable type.
				idx = 1
			}
		}

		return scanTypes[idx], nil
	}

	return template.FuncMap{
		"UpperCamel": func(s string) string {
			return camel(s, true)
		},

		"LowerCamel": func(s string) string {
			return camel(s, false)
		},

		"Literal": func(s string) string {
			lines := []string{`"" +`}
			for _, line := range strings.Split(s, "\n") {
				// NOTE: "\n" must be preserved. Consider this:
				// "SELECT * -- comment blablabla \n FROM atable"
				lines = append(lines, fmt.Sprintf("%+q +", line+"\n"))
			}
			lines = append(lines, `""`)
			return strings.Join(lines, "\n")
		},

		"ScanType": func(col interface{}) (string, error) {
			return scanType(col, -1)
		},
		"NotNullScanType": func(col interface{}) (string, error) {
			return scanType(col, 0)
		},
		"NullScanType": func(col interface{}) (string, error) {
			return scanType(col, 1)
		},

		"ExtractArgsInfo":      argdir.ExtractArgsInfo,
		"ExtractVarsInfo":      varsdir.ExtractVarsInfo,
		"ExtractWildcardsInfo": wcdir.ExtractWildcardsInfo,
	}
}
