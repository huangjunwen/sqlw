package stmt

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbctx"
)

type StatementInfo struct {
	stmtType        string // 'SELECT'/'UPDATE'/'INSERT'/'DELETE'
	stmtName        string
	stmtText        string
	directiveLocals map[interface{}]interface{}

	// for SELECT stmt only
	resultColumnNames []string
	resultColumnTypes []*sql.ColumnType
}

func NewStatementInfo(ctx *dbctx.DBContext, elem *etree.Element) (*StatementInfo, error) {
	info := &StatementInfo{
		directiveLocals: map[interface{}]interface{}{},
	}

	// Element's tag as type
	info.stmtType = strings.ToUpper(elem.Tag)
	switch info.stmtType {
	case "SELECT", "UPDATE", "INSERT", "DELETE":
	default:
		return nil, fmt.Errorf("Unknown statement type %+q", info.stmtType)
	}

	// Name attribute
	info.stmtName = elem.SelectAttrValue("name", "")
	if info.stmtName == "" {
		return nil, fmt.Errorf("Missing 'name' attribute of <%q>", info.stmtType)
	}

	// Process it.
	if err := info.processElem(ctx, elem); err != nil {
		return nil, err
	}

	return info, nil

}

func (info *StatementInfo) processElem(ctx *dbctx.DBContext, elem *etree.Element) error {

	// Convert elem to a list of StmtDirective
	directives := []Directive{}

	for _, t := range elem.Child {

		directive := Directive(nil)

		switch tok := t.(type) {
		case *etree.CharData:
			directive = &textDirective{}

		case *etree.Element:
			factory := directiveFactories[tok.Tag]
			if factory == nil {
				return fmt.Errorf("Unknown directive <%s>", tok.Tag)
			}
			directive = factory()
		}

		if err := directive.Initialize(ctx, info, t); err != nil {
			return err
		}

		directives = append(directives, directive)

	}

	if info.StmtType() == "SELECT" {

		// GenerateQuery()
		fragments := []string{}

		for _, directive := range directives {

			fragment, err := directive.GenerateQuery()
			if err != nil {
				return err
			}
			fragments = append(fragments, fragment)

		}

		stmtTextQuery := strings.Join(fragments, "")

		// Query
		rows, err := ctx.Conn().Query(stmtTextQuery)
		if err != nil {
			return err
		}

		resultColumnNames, err := rows.Columns()
		if err != nil {
			return err
		}

		resultColumnTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}

		// Process query result
		for _, directive := range directives {
			if err := directive.ProcessQueryResult(&resultColumnNames, &resultColumnTypes); err != nil {
				return err
			}
		}

		// Save
		info.resultColumnNames = resultColumnNames
		info.resultColumnTypes = resultColumnTypes

	}

	// Generate()
	fragments := []string{}

	for _, directive := range directives {

		fragment, err := directive.Generate()
		if err != nil {
			return err
		}
		fragments = append(fragments, fragment)

	}

	info.stmtText = strings.Join(fragments, "")

	return nil
}

func (info *StatementInfo) Valid() bool {
	return info != nil
}

func (info *StatementInfo) String() string {
	return info.stmtName
}

func (info *StatementInfo) StmtName() string {
	return info.stmtName
}

func (info *StatementInfo) StmtType() string {
	return info.stmtType
}

func (info *StatementInfo) NumResultColumn() int {
	return len(info.resultColumnNames)
}

func (info *StatementInfo) ResultColumnName(i int) string {
	return info.resultColumnNames[i]
}

func (info *StatementInfo) ResultColumnType(i int) *sql.ColumnType {
	return info.resultColumnTypes[i]
}

func (info *StatementInfo) DirectiveLocals(key interface{}) interface{} {
	return info.directiveLocals[key]
}

func (info *StatementInfo) SetDirectiveLocals(key, val interface{}) {
	info.directiveLocals[key] = val
}
