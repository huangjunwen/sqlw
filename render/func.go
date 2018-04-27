package render

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/huangjunwen/sqlw/statement/directive"
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

	ctx := r.ctx

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
			primitiveScanType, err := ctx.Drv().PrimitiveScanType(typ)
			if err != nil {
				return "", err
			}

			nullable, ok := typ.Nullable()
			if !ok {
				return "", fmt.Errorf("Nullable test not supported for driver %+q", ctx.DriverName())
			}

			ms := r.scanTypeMap
			if ms == nil {
				ms = DefaultScanTypeMap
			}

			m := ms[0]
			if nullable {
				m = ms[1]
			}

			scanType, found := m[primitiveScanType]
			if !found {
				return "", fmt.Errorf("Can't get scan type for %+q", primitiveScanType)
			}

			return scanType, nil
		},

		"ExtractArgInfo":      directive.ExtractArgInfo,
		"ExtractWildcardInfo": directive.ExtractWildcardInfo,
	}
}
