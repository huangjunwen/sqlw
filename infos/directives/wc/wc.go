package wcdir

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/infos"
)

// WildcardsInfo contain wildcard information in a statement.
type WildcardsInfo struct {
	// len(wildcardColumns) == len(wildcardNames) == len(resultColumns)
	wildcardColumns []*infos.ColumnInfo
	wildcardNames   []string

	loader          *datasrc.Loader
	db              *infos.DBInfo
	stmt            *infos.StmtInfo
	marker          string
	directives      []*wcDirective
	resultProcessed bool
}

type wcDirective struct {
	info       *WildcardsInfo
	table      *infos.TableInfo
	tableAlias string
	idx        int // the idx-th wildcard directive in the statement
}

var (
	_ infos.TerminalDirective = (*wcDirective)(nil)
)

type localsKeyType struct{}

var (
	localsKey = localsKeyType{}
)

// ExtractWildcardsInfo extracts wildcard information from a statement or nil if not exists.
func ExtractWildcardsInfo(stmt *infos.StmtInfo) *WildcardsInfo {
	locals := stmt.Locals(localsKey)
	if locals != nil {
		return locals.(*WildcardsInfo)
	}
	return nil
}

func newWildcardsInfo(loader *datasrc.Loader, db *infos.DBInfo, stmt *infos.StmtInfo) *WildcardsInfo {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	marker := hex.EncodeToString(buf)
	return &WildcardsInfo{
		loader: loader,
		db:     db,
		stmt:   stmt,
		// NOTE: Identiy must starts with letter so add a prefix.
		marker: "wc" + marker,
	}
}

func (info *WildcardsInfo) fmtMarker(idx int, isBegin bool) string {
	if isBegin {
		return fmt.Sprintf("%s_%d_b", info.marker, idx)
	}
	return fmt.Sprintf("%s_%d_e", info.marker, idx)
}

func (info *WildcardsInfo) parseMarker(s string) (isMarker bool, idx int, isBegin bool) {
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

func (info *WildcardsInfo) queryFragment(d *wcDirective) string {
	return fmt.Sprintf("1 AS %s, %s, 1 AS %s", info.fmtMarker(d.idx, true), d.expansion(), info.fmtMarker(d.idx, false))
}

func (info *WildcardsInfo) processQueryResultColumns(resultCols *[]*datasrc.Column) error {

	// Should be run only once per stmt.
	if info.resultProcessed {
		return nil
	}
	info.resultProcessed = true

	processedResultCols := []*datasrc.Column{}
	curWildcard := (*wcDirective)(nil)
	curWildcardColPos := 0

	for _, resultCol := range *resultCols {

		resultColName := resultCol.Name
		isMarker, idx, isBegin := info.parseMarker(resultColName)

		// It's a marker column, toggle wildcard mode
		if isMarker {
			if isBegin {

				// Enter wildcard mode
				if curWildcard != nil {
					return fmt.Errorf("<wc>: Expect not in wildcard mode but already in <wc table=%+q as=%+q>.",
						curWildcard.table.TableName(), curWildcard.tableAlias)
				}
				curWildcard = info.directives[idx]
				curWildcardColPos = 0

			} else {

				// Exit wildcard mode
				if curWildcard == nil {
					return fmt.Errorf("<wc>: Expect in wildcard mode But not.")
				}
				if curWildcardColPos != curWildcard.table.NumColumn() {
					return fmt.Errorf("<wc>: Expect table column pos %d, but got %d.",
						curWildcard.table.NumColumn(), curWildcardColPos)
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
			info.wildcardNames = append(info.wildcardNames, "")

		} else {

			// In wildcard mode
			wildcardColumn := curWildcard.table.Column(curWildcardColPos)
			if !wildcardColumn.Valid() {
				return fmt.Errorf("<wc>: Invalid column pos %d for table %s.",
					curWildcardColPos, curWildcard.table.String())
			}
			if wildcardColumn.Col().DataType != resultCol.DataType {
				return fmt.Errorf("<wc>: Expect data type is %+q but got %+q for table column %s.%s",
					wildcardColumn.Col().DataType, resultCol.DataType, wildcardColumn.Table().TableName(), wildcardColumn.ColumnName())
			}
			curWildcardColPos += 1
			info.wildcardColumns = append(info.wildcardColumns, wildcardColumn)
			info.wildcardNames = append(info.wildcardNames, curWildcard.name())

		}

	}

	if curWildcard != nil {
		return fmt.Errorf("<wc>: Wildcard mode is not exited.")
	}

	*resultCols = processedResultCols
	return nil

}

// Valid return true if info != nil.
func (info *WildcardsInfo) Valid() bool {
	return info != nil
}

// WildcardColumn returns the table column for the i-th result column
// if it is from a <wc> directive and nil otherwise.
func (info *WildcardsInfo) WildcardColumn(i int) *infos.ColumnInfo {
	if info == nil {
		return nil
	}
	if i < 0 || i >= len(info.wildcardColumns) {
		return nil
	}
	return info.wildcardColumns[i]
}

// WildcardName returns the wildcard name (table name or alias) for the i-th result column
// if it is from a <wc> directive or "" otherwise.
func (info *WildcardsInfo) WildcardName(i int) string {
	if info == nil {
		return ""
	}
	if i < 0 || i >= len(info.wildcardNames) {
		return ""
	}
	return info.wildcardNames[i]
}

func (d *wcDirective) Initialize(loader *datasrc.Loader, db *infos.DBInfo, stmt *infos.StmtInfo, tok etree.Token) error {

	// Getset WildcardsInfo.
	locals := stmt.Locals(localsKey)
	if locals == nil {
		locals = newWildcardsInfo(loader, db, stmt)
		stmt.SetLocals(localsKey, locals)
	}
	info := locals.(*WildcardsInfo)

	// Extract Table info.
	elem := tok.(*etree.Element)
	tableName := elem.SelectAttrValue("table", "")
	if tableName == "" {
		return fmt.Errorf("Missing 'table' attribute in <wildcard> directive")
	}

	table := db.TableByName(tableName)
	if table == nil {
		return fmt.Errorf("Table %+q not found", tableName)
	}

	// Optinally alias
	as := elem.SelectAttrValue("as", "")

	// Set fields
	d.info = info
	d.table = table
	d.tableAlias = as
	d.idx = len(info.directives)

	// Check wildcard name uniqueness
	for _, directive := range info.directives {
		if d.name() == directive.name() {
			return fmt.Errorf("Duplicated wildcard name %+q, please use an alternative alias", d.name())
		}
	}

	// Add to WildcardsInfo
	info.directives = append(info.directives, d)

	return nil

}

func (d *wcDirective) QueryFragment() (string, error) {
	return d.info.queryFragment(d), nil
}

func (d *wcDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
	return d.info.processQueryResultColumns(resultCols)
}

func (d *wcDirective) Fragment() (string, error) {
	return d.expansion(), nil
}

func (d *wcDirective) expansion() string {

	loader := d.info.loader

	prefix := d.tableAlias
	if prefix == "" {
		prefix = d.table.TableName()
	}
	prefix = loader.Quote(prefix)

	fragments := []string{}
	for i := 0; i < d.table.NumColumn(); i++ {
		if i != 0 {
			fragments = append(fragments, ", ")
		}
		columnName := loader.Quote(d.table.Column(i).ColumnName())
		fragments = append(fragments, fmt.Sprintf("%s.%s", prefix, columnName))
	}

	return strings.Join(fragments, "")

}

func (d *wcDirective) name() string {
	if d.tableAlias != "" {
		return d.tableAlias
	}
	return d.table.TableName()
}

func init() {
	infos.RegistDirectiveFactory(func() infos.Directive {
		return &wcDirective{}
	}, "wc")
}
