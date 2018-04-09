package info

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/beevik/etree"
	"strconv"
	"strings"
)

type StmtInfo struct {
	stmtType string // 'SELECT'/'UPDATE'/'INSERT'/'DELETE'
	stmtName string
	argNames []string
	argTypes []string
	env      map[string]string
	stmtText string

	// for SELECT stmt only
	resultColumnNames []string
	resultColumnTypes []*sql.ColumnType
	wildcardColumns   []*ColumnInfo
	wildcardAlias     []string
}

var (
	pathArg  = etree.MustCompilePath("./arg")
	pathEnv  = etree.MustCompilePath("./env")
	pathStmt = etree.MustCompilePath("./stmt[1]")
)

func ExtractStmtInfo(db *sql.DB, dbInfo *DBInfo, elem *etree.Element) (*StmtInfo, error) {
	stmtInfo := &StmtInfo{
		env: map[string]string{},
	}

	// Element's tag as type
	stmtInfo.stmtType = strings.ToUpper(elem.Tag)
	switch stmtInfo.stmtType {
	case "SELECT", "UPDATE", "INSERT", "DELETE":
	default:
		return nil, fmt.Errorf("Unknown statement type %+q", stmtInfo.stmtType)
	}

	// name attribute
	stmtInfo.stmtName = elem.SelectAttrValue("name", "")
	if stmtInfo.stmtName == "" {
		return nil, fmt.Errorf("Missing 'name' attribute of <%q>", stmtInfo.stmtType)
	}

	// args
	argNames := map[string]struct{}{}
	for _, argElem := range elem.FindElementsPath(pathArg) {
		argName := argElem.SelectAttrValue("name", "")
		argType := argElem.SelectAttrValue("type", "")
		if argName == "" {
			return nil, fmt.Errorf("Missing 'name' attribute of <arg> in statement %+q", stmtInfo.stmtName)
		}
		if _, ok := argNames[argName]; ok {
			return nil, fmt.Errorf("Duplicate arg name %+q in statement %+q", argName, stmtInfo.stmtName)
		}
		argNames[argName] = struct{}{}
		if argType == "" {
			return nil, fmt.Errorf("Missing 'type' attribute of <arg name='%q'> in statement %+q", argName, stmtInfo.stmtName)
		}
		stmtInfo.argNames = append(stmtInfo.argNames, argName)
		stmtInfo.argTypes = append(stmtInfo.argTypes, argType)
	}

	// env
	for _, envElem := range elem.FindElementsPath(pathEnv) {
		envName := envElem.SelectAttrValue("name", "")
		envValue := envElem.SelectAttrValue("value", "")
		if envName == "" {
			return nil, fmt.Errorf("Missing 'name' attribute of <env> in statement %+q", stmtInfo.stmtName)
		}
		if _, ok := stmtInfo.env[envName]; ok {
			return nil, fmt.Errorf("Duplicate env name %+q in statement %+q", envName, stmtInfo.stmtName)
		}
		if envValue == "" {
			return nil, fmt.Errorf("Missing 'value' attribute of <env name='%q'> in statement %+q", envName, stmtInfo.stmtName)
		}
		stmtInfo.env[envName] = envValue
	}

	// stmt
	stmtElem := elem.FindElementPath(pathStmt)
	if stmtElem == nil {
		return nil, fmt.Errorf("Missng <stmt> in statement %+q", stmtInfo.stmtName)
	}

	switch stmtInfo.stmtType {
	case "SELECT":
		if err := stmtInfo.processSelectStmt(db, dbInfo, stmtElem); err != nil {
			return nil, err
		}
	default:
	}

	//TODO

	return stmtInfo, nil

}

func (info *StmtInfo) processSelectStmt(db *sql.DB, dbInfo *DBInfo, stmtElem *etree.Element) error {

	// Construct text for query
	stmtText4Query, wcTableNames, wcAliases, wcMarker, err := constructStmtText4Query(dbInfo, stmtElem)
	if err != nil {
		return err
	}

	// Query database to obtain meta data
	rows, err := db.Query(stmtText4Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	resultColumnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	resultColumnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Wildcard mode variables.
	inWc := false
	wcTableInfo := (*TableInfo)(nil)
	wcAlias := ""
	wcColumnPos := 0

	// For each result column.
	for i, resultColumnName := range resultColumnNames {

		isMarker, wcIdx, isBegin := parseMarker(resultColumnName, wcMarker)
		if isMarker {
			// It's a marker column, toggle wildcard mode.
			if !inWc {
				if !isBegin {
					panic(fmt.Errorf("Expect begin marker but got end marker"))
				}

				// Enter wildcard mode.
				wcTableInfo = dbInfo.TableByNameM(wcTableNames[wcIdx])
				wcAlias = wcAliases[wcIdx]
				wcColumnPos = 0
				inWc = true

			} else {
				if isBegin {
					panic(fmt.Errorf("Expect end marker but got begin marker"))
				}

				// Exit wildcard mode.
				wcTableInfo = nil
				wcAlias = ""
				wcColumnPos = 0
				inWc = false
			}

			continue

		}

		// Normal column.
		info.resultColumnNames = append(info.resultColumnNames, resultColumnName)
		resultColumnType := resultColumnTypes[i]
		info.resultColumnTypes = append(info.resultColumnTypes, resultColumnType)

		if !inWc {
			// Not in wildcard mode.
			info.wildcardColumns = append(info.wildcardColumns, nil)
			info.wildcardAlias = append(info.wildcardAlias, "")
			continue
		}

		wildcardColumn := wcTableInfo.Column(wcColumnPos)
		wcColumnPos += 1
		if wildcardColumn.ColumnType().ScanType() != resultColumnType.ScanType() {
			panic(fmt.Errorf("Wildcard expansion column type mismatch"))
		}
		info.wildcardColumns = append(info.wildcardColumns, wildcardColumn)
		info.wildcardAlias = append(info.wildcardAlias, wcAlias)
	}

	return nil
}

// constructStmtText4Query build statement text for querying against database to get result columns info.
// This function also expands wildcard and returns wildcard info, for example:
//
// 	<stmt>
// 	  SELECT <wildcard table="user" as="u" /> FROM user AS u
// 	</stmt>
//
// Should return something like:
//
// 	"SELECT 1 AS yaePovo3_0_b, u.id, u.name, ..., 1 AS yaePovo3_0_e FROM user AS u"
//
// the wildcard xml expands to columns of the user table with markers before/after these columns to detect wildcard columns' postion.
func constructStmtText4Query(dbInfo *DBInfo, stmtElem *etree.Element) (stmtText string, wcTableNames []string, wcAliases []string, wcMarker string, err error) {

	wcMarker = genMarker()
	fragments := []string{}

	for _, t := range stmtElem.Child {
		switch tok := t.(type) {
		case *etree.CharData:
			// Append char data directly.
			fragments = append(fragments, tok.Data)

		case *etree.Element:
			// We support <wildcard> and <replace>
			switch tok.Tag {
			case "wildcard":
				// Get wildcard table and its info
				wcTableName := tok.SelectAttrValue("table", "")
				if wcTableName == "" {
					err = fmt.Errorf("Missing attribute 'table' in <wildcard>")
					return
				}

				wcTableInfo, found := dbInfo.TableByName(wcTableName)
				if !found {
					err = fmt.Errorf("Can't find table %+q", wcTableName)
					return
				}

				// Maybe has alias
				wcAlias := tok.SelectAttrValue("as", "")

				// Use alias first
				prefix := wcAlias
				if prefix == "" {
					prefix = wcTableName
				}

				// Store wildcard info.
				wcTableNames = append(wcTableNames, wcTableName)
				wcAliases = append(wcAliases, wcAlias)
				wcIdx := len(wcTableNames) - 1

				// Add beign marker then columns then end marker.
				fragments = append(fragments, fmt.Sprintf("1 AS %s, ", fmtBeginMarker(wcMarker, wcIdx)))
				for i := 0; i < wcTableInfo.NumColumn(); i++ {
					// XXX: object identifier quote here?
					fragments = append(fragments, fmt.Sprintf("%s.%s, ", prefix, wcTableInfo.Column(i).ColumnName()))
				}
				fragments = append(fragments, fmt.Sprintf("1 AS %s", fmtEndMarker(wcMarker, wcIdx)))

			case "replace":
				// Use inner text for query
				fragments = append(fragments, tok.Text())

			default:
				err = fmt.Errorf("Unknown processor <%q>", tok.Tag)
				return

			}

		default:
			// ignore
		}

	}

	stmtText = strings.Join(fragments, "")
	return
}

func genMarker() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func fmtBeginMarker(marker string, idx int) string {
	return fmt.Sprintf("%s_%d_b", marker, idx)
}

func fmtEndMarker(marker string, idx int) string {
	return fmt.Sprintf("%s_%d_e", marker, idx)
}

func parseMarker(s, marker string) (isMarker bool, idx int, begin bool) {
	parts := strings.Split(s, "_")
	if len(parts) != 3 || parts[0] != marker {
		return
	}

	// Check idx part.
	i, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Errorf("Invalid marker format %+q %+q", s, marker))
	}

	// Check suffix part.
	switch parts[2] {
	case "b", "e":
	default:
		panic(fmt.Errorf("Invalid marker format %+q %+q", s, marker))
	}

	// OK
	isMarker = true
	idx = i
	begin = parts[2] == "b"
	return
}

/*
func constructStmtText(dbInfo *DBInfo, stmtElem *etree.Element) (stmtText string, err error) {

	fragments := []string{}

	for _, t := range stmtElem.Child {

		switch tok := t.(type) {
		case *etree.CharData:
			// Append char data directly.
			fragments = append(fragments, tok.Data)

		case *etree.Element:
			// We support <wildcard> and <replace>
			switch tok.Tag {
			case "wildcard":
				// Get wildcard table and its info
				wcTableName := tok.SelectAttrValue("table", "")
				if wcTableName == "" {
					err = fmt.Errorf("Missing attribute 'table' in <wildcard>")
					return
				}

				wcTableInfo, found := dbInfo.TableByName(wcTableName)
				if !found {
					err = fmt.Errorf("Can't find table %+q", wcTableName)
					return
				}

				// Maybe has alias
				wcAlias := tok.SelectAttrValue("as", "")

				// Use alias first
				prefix := wcAlias
				if prefix == "" {
					prefix = wcTableName
				}

				// Add columns.
				for i := 0; i < wcTableInfo.NumColumn(); i++ {
					if i != 0 {
						fragments = append(fragments, ", ")
					}
					// XXX: object identifier quote here?
					fragments = append(fragments, fmt.Sprintf("%s.%s", prefix, wcTableInfo.Column(i).ColumnName()))
				}

			case "replace":
				// TODO
				// Use inner text for query
				fragments = append(fragments, tok.Text())

			default:
				err = fmt.Errorf("Unknown processor <%q>", tok.Tag)
				return

			}

		default:
			// ignore

		}
	}

	stmtText = strings.Join(fragments, "")
	return
}
*/

func (info *StmtInfo) Valid() bool {
	return info != nil
}

func (info *StmtInfo) String() string {
	return info.stmtName
}

func (info *StmtInfo) StmtName() string {
	return info.stmtName
}

func (info *StmtInfo) NumArg() int {
	return len(info.argNames)
}

func (info *StmtInfo) ArgName(i int) string {
	return info.argNames[i]
}

func (info *StmtInfo) ArgType(i int) string {
	return info.argTypes[i]
}

func (info *StmtInfo) Env(name string) string {
	return info.env[name]
}

func (info *StmtInfo) StmtType() string {
	return info.stmtType
}

func (info *StmtInfo) NumResultColumn() int {
	return len(info.resultColumnNames)
}

func (info *StmtInfo) ResultColumnName(i int) string {
	return info.resultColumnNames[i]
}

func (info *StmtInfo) ResultColumnType(i int) *sql.ColumnType {
	return info.resultColumnTypes[i]
}

func (info *StmtInfo) WildcardColumn(i int) *ColumnInfo {
	return info.wildcardColumns[i]
}

func (info *StmtInfo) WildcardAlias(i int) string {
	return info.wildcardAlias[i]
}
