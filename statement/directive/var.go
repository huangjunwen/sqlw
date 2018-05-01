package directive

import (
	"database/sql"
	"fmt"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbcontext"
	"github.com/huangjunwen/sqlw/statement"
)

type varDirective struct{}

type varLocalsKeyType struct{}

var (
	varLocalsKey = varLocalsKeyType{}
)

// VarInfo contains custom variables in a statement.
type VarInfo struct {
	values map[string]string
}

func (d *varDirective) Initialize(dbctx *dbcontext.DBCtx, stmt *statement.StmtInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)

	// Get var name and (optinal) value.
	varName := elem.SelectAttrValue("name", "")
	if varName == "" {
		return fmt.Errorf("Missing 'name' attribute in <var> directive")
	}
	varValue := elem.SelectAttrValue("value", "")

	// Get/set locals
	locals := stmt.Locals(varLocalsKey)
	if locals == nil {
		locals = &VarInfo{
			values: make(map[string]string),
		}
		stmt.SetLocals(varLocalsKey, locals)
	}
	info := locals.(*VarInfo)

	// Store name/value pair.
	info.values[varName] = varValue

	return nil

}

func (d *varDirective) Generate() (string, error) {
	return "", nil
}

func (d *varDirective) GenerateQuery() (string, error) {
	return "", nil
}

func (d *varDirective) ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error {
	return nil
}

// Valid returns true if info != nil
func (info *VarInfo) Valid() bool {
	return info != nil
}

// Has returns true if the named var exists. It returns false if info is nil or not exists.
func (info *VarInfo) Has(name string) bool {
	if info == nil {
		return false
	}
	_, ok := info.values[name]
	return ok
}

// Value returns the named var's value. It returns "" if info is nil or not exists or the value is just "".
func (info *VarInfo) Value(name string) string {
	if info == nil {
		return ""
	}
	return info.values[name]
}

// ExtractVarInfo extracts custom var information from a statement or nil if not exists.
func ExtractVarInfo(stmt *statement.StmtInfo) *VarInfo {
	locals := stmt.Locals(varLocalsKey)
	if locals != nil {
		return locals.(*VarInfo)
	}
	return nil
}

func init() {
	statement.RegistDirectiveFactory(func() statement.Directive {
		return &varDirective{}
	}, "var")
}
