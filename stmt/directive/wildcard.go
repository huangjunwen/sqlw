package directive

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbctx"
	"github.com/huangjunwen/sqlw/stmt"
)

type wildcardDirective struct {
	statement  *stmt.StatementInfo
	table      *dbctx.TableInfo
	tableAlias string
}

type wildcardDirectiveLocalsKeyType struct{}

var (
	wildcardDirectiveLocalsKey = wildcardDirectiveLocalsKeyType{}
)

// WildcardInfo contain wildcard information in a statement.
type WildcardInfo struct {
	// len(wildcardColumns) == len(wildcardAliases) == len(resultColumns)
	wildcardColumns []*dbctx.ColumnInfo
	wildcardAliases []string

	marker          string
	directives      []*wildcardDirective
	resultProcessed bool
}

func (d *wildcardDirective) expansion() string {
	prefix := d.tableAlias
	if prefix == "" {
		prefix = d.table.TableName()
	}

	fragments := []string{}
	for i := 0; i < d.table.NumColumn(); i++ {
		if i != 0 {
			fragments = append(fragments, ", ")
		}
		column := d.table.Column(i)
		fragments = append(fragments, fmt.Sprintf("%s.%s", prefix, column.ColumnName()))
	}

	return strings.Join(fragments, "")

}

func (d *wildcardDirective) directiveLocals() *WildcardInfo {
	// All wildcardDirective in a statement share the same WildcardInfo.
	locals := d.statement.DirectiveLocals(wildcardDirectiveLocalsKey)
	if locals != nil {
		return locals.(*WildcardInfo)
	}
	ret := newWildcardInfo()
	d.statement.SetDirectiveLocals(wildcardDirectiveLocalsKey, ret)
	return ret
}

func (d *wildcardDirective) Initialize(ctx *dbctx.DBContext, statement *stmt.StatementInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)

	tableName := elem.SelectAttrValue("table", "")
	if tableName == "" {
		return fmt.Errorf("Missing 'table' attribute in <wildcard> directive")
	}

	table, found := ctx.DB().TableByName(tableName)
	if !found {
		return fmt.Errorf("Table %+q not found", tableName)
	}

	as := elem.SelectAttrValue("as", "")

	d.statement = statement
	d.table = table
	d.tableAlias = as
	return nil
}

func (d *wildcardDirective) Generate() (string, error) {
	return d.expansion(), nil
}

func (d *wildcardDirective) GenerateQuery() (string, error) {
	return d.directiveLocals().generateQuery(d), nil
}

func (d *wildcardDirective) ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error {
	return d.directiveLocals().processQueryResult(resultColumnNames, resultColumnTypes)
}

func newWildcardInfo() *WildcardInfo {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	marker := hex.EncodeToString(buf)
	return &WildcardInfo{
		// NOTE: Identiy must starts with letter so add a prefix.
		marker: "wc" + marker,
	}
}

// ExtractWildcardInfo() extract wildcard information from a statement or nil if not exists.
func ExtractWildcardInfo(statement *stmt.StatementInfo) *WildcardInfo {
	locals := statement.DirectiveLocals(wildcardDirectiveLocalsKey)
	if locals != nil {
		return locals.(*WildcardInfo)
	}
	return nil
}

func (info *WildcardInfo) fmtMarker(idx int, isBegin bool) string {
	if isBegin {
		return fmt.Sprintf("%s_%d_b", info.marker, idx)
	}
	return fmt.Sprintf("%s_%d_e", info.marker, idx)
}

func (info *WildcardInfo) parseMarker(s string) (isMarker bool, idx int, isBegin bool) {
	parts := strings.Split(s, "_")
	if len(parts) != 3 || parts[0] != info.marker {
		return false, 0, false
	}

	i, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Errorf("Invalid marker %+q", s))
	}

	switch parts[2] {
	case "b", "e":
	default:
		panic(fmt.Errorf("Invalid marker %+q", s))
	}

	return true, i, parts[2] == "b"

}

func (info *WildcardInfo) generateQuery(d *wildcardDirective) string {
	info.directives = append(info.directives, d)
	idx := len(info.directives) - 1
	return fmt.Sprintf("1 AS %s, %s, 1 AS %s", info.fmtMarker(idx, true), d.expansion(), info.fmtMarker(idx, false))
}

func (info *WildcardInfo) processQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error {
	if info.resultProcessed {
		// Should be run only once per stmt.
		return nil
	}
	info.resultProcessed = true

	resultColumnNames1 := []string{}
	resultColumnTypes1 := []*sql.ColumnType{}
	currentDirective := (*wildcardDirective)(nil)
	currentColumnPos := 0

	for i := 0; i < len(*resultColumnNames); i++ {

		resultColumnName := (*resultColumnNames)[i]
		resultColumnType := (*resultColumnTypes)[i]

		isMarker, idx, isBegin := info.parseMarker(resultColumnName)

		if isMarker {
			// It's a marker column, toggle wildcard mode
			if isBegin {
				// Ready to enter wildcard mode.
				if currentDirective != nil {
					panic(fmt.Errorf("Expect no wildcard directive"))
				}

				currentDirective = info.directives[idx]
				currentColumnPos = 0

			} else {
				// Ready to exit wildcard mode.
				if currentDirective == nil {
					panic(fmt.Errorf("Expect wildcard directive"))
				}

				currentDirective = nil
				currentColumnPos = 0

			}

			continue
		}

		// It's normal column.
		resultColumnNames1 = append(resultColumnNames1, resultColumnName)
		resultColumnTypes1 = append(resultColumnTypes1, resultColumnType)

		if currentDirective == nil {
			info.wildcardColumns = append(info.wildcardColumns, nil)
			info.wildcardAliases = append(info.wildcardAliases, "")

		} else {
			wildcardColumn := currentDirective.table.Column(currentColumnPos)
			currentColumnPos += 1
			// NOTE: In left outer join, the same column may have different ScanType()
			// since the outer table can have null value even the original column can not.
			/*
				if wildcardColumn.ColumnType().ScanType() != resultColumnType.ScanType() {
					panic(fmt.Errorf("Wildcard column type mismatch"))
				}
			*/
			info.wildcardColumns = append(info.wildcardColumns, wildcardColumn)
			info.wildcardAliases = append(info.wildcardAliases, currentDirective.tableAlias)
		}

	}

	// Store normal columns only.
	*resultColumnNames = resultColumnNames1
	*resultColumnTypes = resultColumnTypes1

	return nil
}

// WildcardColumn returns the table column info for the i-th result column
// if it is from a <wildcard> directive or nil otherwise.
func (info *WildcardInfo) WildcardColumn(i int) *dbctx.ColumnInfo {
	if info == nil {
		return nil
	}
	return info.wildcardColumns[i]
}

// WildcardAlias returns the table alias name for the i-th result column
// if it is from a <wildcard> directive or "" otherwise.
func (info *WildcardInfo) WildcardAlias(i int) string {
	if info == nil {
		return ""
	}
	return info.wildcardAliases[i]
}

// Valid return true if this is a non-nil WildcardInfo.
func (info *WildcardInfo) Valid() bool {
	return info != nil
}

func init() {
	stmt.RegistDirectiveFactory(func() stmt.Directive {
		return &wildcardDirective{}
	}, "wildcard")
}
