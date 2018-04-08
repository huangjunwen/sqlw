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
	stmtTextForQuery, wildcardTables, wildcardAliases, marker, err := constructStmtTextForQuery(dbInfo, stmtElem)
	if err != nil {
		return err
	}

	// Query database to obtain meta data
	rows, err := db.Query(stmtTextForQuery)
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
	inWildcard := false
	wildcardTableInfo := (*TableInfo)(nil)
	wildcardAlias := ""
	wildcardColumnPos := 0

	// For each result column.
	for i, resultColumnName := range resultColumnNames {

		isMarker, wildcardIdx, isBegin := parseMarker(resultColumnName, marker)
		if isMarker {
			// It's a marker column, toggle wildcard mode.
			if !inWildcard {
				if !isBegin {
					panic(fmt.Errorf("Expect begin marker but got end marker"))
				}

				// Enter wildcard mode.
				wildcardTableInfo = dbInfo.TableByNameM(wildcardTables[wildcardIdx])
				wildcardAlias = wildcardAliases[wildcardIdx]
				wildcardColumnPos = 0
				inWildcard = true

			} else {
				if isBegin {
					panic(fmt.Errorf("Expect end marker but got begin marker"))
				}

				// Exit wildcard mode.
				wildcardTableInfo = nil
				wildcardAlias = ""
				wildcardColumnPos = 0
				inWildcard = false
			}

			continue

		}

		// Normal column.
		info.resultColumnNames = append(info.resultColumnNames, resultColumnName)
		resultColumnType := resultColumnTypes[i]
		info.resultColumnTypes = append(info.resultColumnTypes, resultColumnType)

		if !inWildcard {
			info.wildcardColumns = append(info.wildcardColumns, nil)
			info.wildcardAlias = append(info.wildcardAlias, "")
			continue
		}

		wildcardColumn := wildcardTableInfo.Column(wildcardColumnPos)
		wildcardColumnPos += 1
		if wildcardColumn.ColumnType().ScanType() != resultColumnType.ScanType() {
			panic(fmt.Errorf("Wildcard expansion column type mismatch"))
		}
		info.wildcardColumns = append(info.wildcardColumns, wildcardColumn)
		info.wildcardAlias = append(info.wildcardAlias, wildcardAlias)
	}

	return nil
}

func constructStmtTextForQuery(dbInfo *DBInfo, stmtElem *etree.Element) (stmtText string, wildcardTables []string, wildcardAliases []string, wildcardMarker string, err error) {
	wildcardMarker = genMarker()

	fragments := []string{}
	for _, t := range stmtElem.Child {
		switch tok := t.(type) {
		case *etree.CharData:
			fragments = append(fragments, tok.Data)

		case *etree.Element:
			switch tok.Tag {
			case "wildcard":
				// Get wildcard table and its info
				wildcardTable := tok.SelectAttrValue("table", "")
				if wildcardTable == "" {
					err = fmt.Errorf("Missing attribute 'table' in <wildcard>")
					return
				}

				tableInfo, found := dbInfo.TableByName(wildcardTable)
				if !found {
					err = fmt.Errorf("Can't find table %+q", wildcardTable)
					return
				}

				// Maybe has alias
				wildcardAlias := tok.SelectAttrValue("as", "")

				// Use alias first
				prefix := wildcardAlias
				if prefix == "" {
					prefix = tableInfo.TableName()
				}

				// Store wildcard info.
				wildcardTables = append(wildcardTables, wildcardTable)
				wildcardAliases = append(wildcardAliases, wildcardAlias)
				wildcardIdx := len(wildcardTables) - 1

				// Add beign marker then columns then end marker.
				fragments = append(fragments, fmt.Sprintf("1 AS %s, ", fmtBeginMarker(wildcardMarker, wildcardIdx)))
				for i := 0; i < tableInfo.NumColumn(); i++ {
					// XXX: object identifier quote here?
					fragments = append(fragments, fmt.Sprintf("%s.%s, ", prefix, tableInfo.Column(i).ColumnName()))
				}
				fragments = append(fragments, fmt.Sprintf("1 AS %s", fmtEndMarker(wildcardMarker, wildcardIdx)))

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
