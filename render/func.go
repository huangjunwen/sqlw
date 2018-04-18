package render

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/dbctx"
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

func funcMap(ctx *dbctx.DBContext) template.FuncMap {

	return template.FuncMap{
		"UpperCamel": func(s string) string {
			return camel(s, true)
		},

		"LowerCamel": func(s string) string {
			return camel(s, false)
		},

		"Nullable": func(typ *sql.ColumnType) (bool, error) {
			nullable, ok := typ.Nullable()
			if !ok {
				return false, fmt.Errorf("Nullable test not supported for driver %+q", ctx.DriverName())
			}
			return nullable, nil
		},

		"ScanType": func(typ *sql.ColumnType) (string, error) {
			return ctx.Drv().ScanType(typ)
		},
	}
}
