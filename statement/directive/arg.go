package directive

import (
	"database/sql"
	"fmt"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbcontext"
	"github.com/huangjunwen/sqlw/statement"
)

type argDirective struct {
	argName string
	argType string
}

type argLocalsKeyType struct{}

var (
	argLocalsKey = argLocalsKeyType{}
)

// ArgInfo contains wrapper function argument information in a statement.
type ArgInfo struct {
	directives []*argDirective
}

func (d *argDirective) Initialize(ctx *dbcontext.DBCtx, stmt *statement.StmtInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)

	// Extract name/type from xml.
	argName := elem.SelectAttrValue("name", "")
	if argName == "" {
		return fmt.Errorf("Missing 'name' attribute in <arg> directive")
	}
	argType := elem.SelectAttrValue("type", "")
	if argType == "" {
		return fmt.Errorf("Missing 'type' attribute in <arg> directive")
	}
	d.argName = argName
	d.argType = argType

	// Add the directive to ArgInfo.
	locals := stmt.Locals(argLocalsKey)
	if locals == nil {
		locals = &ArgInfo{}
		stmt.SetLocals(argLocalsKey, locals)
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

// Valid returns true if info != nil.
func (info *ArgInfo) Valid() bool {
	return info != nil
}

// NumArg returns the number of arguments in the statement. It returns 0 if info is nil or there is no args at all.
func (info *ArgInfo) NumArg() int {
	if info == nil {
		return 0
	}
	return len(info.directives)
}

// ArgName returns the i-th argument's name. It returns "" if info is nil or i is out of range.
func (info *ArgInfo) ArgName(i int) string {
	if info == nil {
		return ""
	}
	if i < 0 || i >= len(info.directives) {
		return ""
	}
	return info.directives[i].argName
}

// ArgType returns the i-th argument's type. It returns "" if info is nil or i is out of range.
func (info *ArgInfo) ArgType(i int) string {
	if info == nil {
		return ""
	}
	if i < 0 || i >= len(info.directives) {
		return ""
	}
	return info.directives[i].argType
}

// ExtractArgInfo extracts arg information from a statement or nil if not exists.
func ExtractArgInfo(stmt *statement.StmtInfo) *ArgInfo {
	locals := stmt.Locals(argLocalsKey)
	if locals != nil {
		return locals.(*ArgInfo)
	}
	return nil
}

func init() {
	statement.RegistDirectiveFactory(func() statement.Directive {
		return &argDirective{}
	}, "arg")
}
