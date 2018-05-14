package render

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/info"
	"github.com/huangjunwen/sqlw/info/directives"
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

		"ScanType": func(v interface{}) (string, error) {
			col := (*datasrc.Column)(nil)
			switch c := v.(type) {
			case *datasrc.Column:
				col = c
			case *info.ColumnInfo:
				col = c.Col()
			default:
				return "", fmt.Errorf("Expect table column or query result column in ScanType but got %T", c)
			}
			if col == nil {
				return "", fmt.Errorf("Column is nil")
			}

			scanTypes, found := r.scanTypeMap[col.DataType]
			if !found {
				return "", fmt.Errorf("Can't get scan type for %+q", col.DataType)
			}

			// NOTE: If not HasNullable, then assume it is nullable since nullable type > not-nullable type.
			nullable := col.Nullable
			if !col.HasNullable {
				nullable = true
			}

			if nullable {
				return scanTypes[1], nil
			}
			return scanTypes[0], nil

		},

		"ExtractArgsInfo":      directives.ExtractArgsInfo,
		"ExtractVarsInfo":      directives.ExtractVarsInfo,
		"ExtractWildcardsInfo": directives.ExtractWildcardsInfo,
	}
}
