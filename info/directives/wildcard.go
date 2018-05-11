package directives

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/dbcontext"
	"github.com/huangjunwen/sqlw/info"
)

type wildcardDirective struct {
	info       *WildcardInfo
	table      *info.TableInfo
	tableAlias string
	idx        int // the idx-th wildcard directive in the statement
}

type wildcardLocalsKeyType struct{}

var (
	wildcardLocalsKey = wildcardLocalsKeyType{}
)

// WildcardInfo contain wildcard information in a statement.
type WildcardInfo struct {
	db              *info.DBInfo
	stmt            *info.StmtInfo
	marker          string
	directives      []*wildcardDirective
	resultProcessed bool

	// len(wildcardColumns) == len(wildcardAliases) == len(resultColumns)
	wildcardColumns []*info.ColumnInfo
	wildcardAliases []string
}

func (d *wildcardDirective) expansion() string {

	drv := d.info.db.DBCtx().Drv()

	prefix := d.tableAlias
	if prefix == "" {
		prefix = d.table.TableName()
	}
	prefix = drv.Quote(prefix)

	fragments := []string{}
	for i := 0; i < d.table.NumColumn(); i++ {
		if i != 0 {
			fragments = append(fragments, ", ")
		}
		columnName := drv.Quote(d.table.Column(i).ColumnName())
		fragments = append(fragments, fmt.Sprintf("%s.%s", prefix, columnName))
	}

	return strings.Join(fragments, "")

}

func (d *wildcardDirective) Initialize(db *info.DBInfo, stmt *info.StmtInfo, tok etree.Token) error {

	elem := tok.(*etree.Element)
	tableName := elem.SelectAttrValue("table", "")
	if tableName == "" {
		return fmt.Errorf("Missing 'table' attribute in <wildcard> directive")
	}

	table := db.TableByName(tableName)
	if table == nil {
		return fmt.Errorf("Table %+q not found", tableName)
	}

	as := elem.SelectAttrValue("as", "")

	// Getset locals.
	locals := stmt.Locals(wildcardLocalsKey)
	if locals == nil {
		locals = newWildcardInfo(db, stmt)
		stmt.SetLocals(wildcardLocalsKey, locals)
	}
	info := locals.(*WildcardInfo)
	info.directives = append(info.directives, d)

	// Set fields
	d.info = info
	d.table = table
	d.tableAlias = as
	d.idx = len(info.directives) - 1
	return nil

}

func (d *wildcardDirective) Fragment() (string, error) {
	return d.expansion(), nil
}

func (d *wildcardDirective) QueryFragment() (string, error) {
	return d.info.queryFragment(d), nil
}

func (d *wildcardDirective) ProcessQueryResultColumns(resultCols *[]dbcontext.Col) error {
	return d.info.processQueryResultColumns(resultCols)
}

func newWildcardInfo(db *info.DBInfo, stmt *info.StmtInfo) *WildcardInfo {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	marker := hex.EncodeToString(buf)
	return &WildcardInfo{
		db:   db,
		stmt: stmt,
		// NOTE: Identiy must starts with letter so add a prefix.
		marker: "wc" + marker,
	}
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

func (info *WildcardInfo) queryFragment(d *wildcardDirective) string {
	return fmt.Sprintf("1 AS %s, %s, 1 AS %s", info.fmtMarker(d.idx, true), d.expansion(), info.fmtMarker(d.idx, false))
}

func (info *WildcardInfo) processQueryResultColumns(resultCols *[]dbcontext.Col) error {

	// Should be run only once per stmt.
	if info.resultProcessed {
		return nil
	}
	info.resultProcessed = true

	processedResultCols := []dbcontext.Col{}
	curWildcard := (*wildcardDirective)(nil)
	curWildcardColPos := 0

	for _, resultCol := range *resultCols {

		resultColName := resultCol.Name()
		isMarker, idx, isBegin := info.parseMarker(resultColName)

		// It's a marker column, toggle wildcard mode
		if isMarker {
			if isBegin {

				// Enter wildcard mode
				if curWildcard != nil {
					panic(fmt.Errorf("Expect not in wildcard mode "))
				}
				curWildcard = info.directives[idx]
				curWildcardColPos = 0

			} else {

				// Exit wildcard mode
				if curWildcard == nil {
					panic(fmt.Errorf("Expect in wildcard mode "))
				}
				if curWildcardColPos != curWildcard.table.NumColumn() {
					panic(fmt.Errorf("Expect column pos == %d, but got %d", curWildcard.table.NumColumn(), curWildcardColPos))
				}
				curWildcard = nil
				curWildcardColPos = 0

			}

			continue
		}

		// It's a normal column
		processedResultCols = append(processedResultCols, resultCol)

		if curWildcard == nil {

			// Not in wildcard mode
			info.wildcardColumns = append(info.wildcardColumns, nil)
			info.wildcardAliases = append(info.wildcardAliases, "")

		} else {

			// In wildcard mode
			wildcardColumn := curWildcard.table.Column(curWildcardColPos)
			if !wildcardColumn.Valid() {
				panic(fmt.Errorf("Bad wildcard column: table(%+q) column(%d)", curWildcard.table.String(), curWildcardColPos))
			}
			curWildcardColPos += 1
			info.wildcardColumns = append(info.wildcardColumns, wildcardColumn)
			info.wildcardAliases = append(info.wildcardAliases, curWildcard.tableAlias)

		}

	}

	*resultCols = processedResultCols
	return nil

}

// WildcardColumn returns the table column for the i-th result column
// if it is from a <wildcard> directive and nil otherwise.
func (info *WildcardInfo) WildcardColumn(i int) *info.ColumnInfo {
	if info == nil {
		return nil
	}
	if i < 0 || i >= len(info.wildcardColumns) {
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
	if i < 0 || i >= len(info.wildcardAliases) {
		return ""
	}
	return info.wildcardAliases[i]
}

// Valid return true if info != nil.
func (info *WildcardInfo) Valid() bool {
	return info != nil
}

// ExtractWildcardInfo extracts wildcard information from a statement or nil if not exists.
func ExtractWildcardInfo(stmt *info.StmtInfo) *WildcardInfo {
	locals := stmt.Locals(wildcardLocalsKey)
	if locals != nil {
		return locals.(*WildcardInfo)
	}
	return nil
}

func init() {
	info.RegistDirectiveFactory(func() info.Directive {
		return &wildcardDirective{}
	}, "wildcard")
}
