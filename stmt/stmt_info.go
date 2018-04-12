package info

import (
	"database/sql"
	"fmt"
	"github.com/beevik/etree"
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
	wildcardAliases   []string
}

var (
	pathArg  = etree.MustCompilePath("./arg")
	pathEnv  = etree.MustCompilePath("./env")
	pathStmt = etree.MustCompilePath("./stmt[1]")
)

func NewStmtInfo(db *sql.DB, dbInfo *DBInfo, elem *etree.Element) (*StmtInfo, error) {
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

	// Process the stmt.
	state := &stmtProcessState{}
	if err := state.process(db, dbInfo, stmtElem, stmtInfo); err != nil {
		return nil, err
	}

	return stmtInfo, nil

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
	return info.wildcardAliases[i]
}
