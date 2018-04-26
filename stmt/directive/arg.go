package directive

import (
	"database/sql"
	"fmt"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbctx"
	"github.com/huangjunwen/sqlw/stmt"
)

type argDirective struct {
	argName string
	argType string
}

type argDirectiveLocalsKeyType struct{}

var (
	argDirectiveLocalsKey = argDirectiveLocalsKeyType{}
)

// ArgInfo contains wrapper function argument information in a statement.
type ArgInfo struct {
	directives []*argDirective
}

func (d *argDirective) Initialize(ctx *dbctx.DBContext, statement *stmt.StatementInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)

	// Extract name/type from xml.
	name := elem.SelectAttrValue("name", "")
	if name == "" {
		return fmt.Errorf("Missing 'name' attribute in <arg> directive")
	}
	typ := elem.SelectAttrValue("type", "")
	if typ == "" {
		return fmt.Errorf("Missing 'type' attribute in <arg> directive")
	}
	d.argName = name
	d.argType = typ

	// Add the directive to ArgInfo.
	locals := statement.DirectiveLocals(argDirectiveLocalsKey)
	if locals == nil {
		locals = &ArgInfo{}
		statement.SetDirectiveLocals(argDirectiveLocalsKey, locals)
	}
	info := locals.(*ArgInfo)
	info.directives = append(info.directives, d)

	return nil
}

func (d *argDirective) Generate() (string, error) {
	return "", nil
}

func (d *argDirective) GenerateQuery() (string, error) {
	return "", nil
}

func (d *argDirective) ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error {
	return nil
}

func (info *ArgInfo) Valid() bool {
	return info != nil
}

func (info *ArgInfo) NumArg() int {
	return len(info.directives)
}

func (info *ArgInfo) ArgName(i int) string {
	return info.directives[i].argName
}

func (info *ArgInfo) ArgType(i int) string {
	return info.directives[i].argType
}

// ExtractArgInfo extracts arg information from a statement or nil if not exists.
func ExtractArgInfo(statement *stmt.StatementInfo) *ArgInfo {
	locals := statement.DirectiveLocals(argDirectiveLocalsKey)
	if locals != nil {
		return locals.(*ArgInfo)
	}
	return nil
}

func init() {
	stmt.RegistDirectiveFactory(func() stmt.Directive {
		return &argDirective{}
	}, "arg")
}
