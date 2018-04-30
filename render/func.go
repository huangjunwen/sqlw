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

		"Split": strings.Split,

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

		"N": func(args ...int) chan int {
			var start, end, step int
			switch len(args) {
			case 1:
				start = 0
				end = args[0]
				step = 1
			case 2:
				start = args[0]
				end = args[1]
				step = 1
			case 3:
				start = args[0]
				end = args[1]
				step = args[2]
			}
			stream := make(chan int)
			go func() {
				if step > 0 {
					for i := start; i < end; i += step {
						stream <- i
					}
				} else if step < 0 {
					for i := start; i > end; i += step {
						stream <- i
					}
				} else {
					panic(fmt.Errorf("Step can't be 0"))
				}
				close(stream)
			}()
			return stream
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
		"ExtractVarInfo":      directive.ExtractVarInfo,
		"ExtractWildcardInfo": directive.ExtractWildcardInfo,
	}
}
