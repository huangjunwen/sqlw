package darg

import (
	"fmt"

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/info"
)

// ArgsInfo contains wrapper function arguments information in a statement.
type ArgsInfo struct {
	argInfos []*ArgInfo
}

// ArgInfo contains single wrapper function argument information in a statement.
type ArgInfo argDirective

type argDirective struct {
	argName string
	argType string
}

var (
	_ info.TerminalDirective = (*argDirective)(nil)
)

type localsKeyType struct{}

var (
	localsKey = localsKeyType{}
)

// ExtractArgsInfo extracts arg information from a statement or nil if not exists.
func ExtractArgsInfo(stmt *info.StmtInfo) *ArgsInfo {
	locals := stmt.Locals(localsKey)
	if locals != nil {
		return locals.(*ArgsInfo)
	}
	return nil
}

// Valid returns true if info != nil.
func (info *ArgsInfo) Valid() bool {
	return info != nil
}

// NumArg returns the number of arguments in the statement. It returns 0 if info is nil or there is no args at all.
func (info *ArgsInfo) NumArg() int {
	if info == nil {
		return 0
	}
	return len(info.argInfos)
}

// Args returns all ArgInfo in the statement.
func (info *ArgsInfo) Args() []*ArgInfo {
	if info == nil {
		return nil
	}
	return info.argInfos
}

// ArgName returns the argument's name.
func (info *ArgInfo) ArgName() string {
	return info.argName
}

// ArgType returns the argument's type.
func (info *ArgInfo) ArgType() string {
	return info.argType
}

func (d *argDirective) Initialize(loader *datasrc.Loader, db *info.DBInfo, stmt *info.StmtInfo, tok etree.Token) error {

	// Get/set ArgsInfo
	locals := stmt.Locals(localsKey)
	if locals == nil {
		locals = &ArgsInfo{}
		stmt.SetLocals(localsKey, locals)
	}
	info := locals.(*ArgsInfo)

	// Extract name/type from xml.
	elem := tok.(*etree.Element)
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

	// Add ArgInfo
	info.argInfos = append(info.argInfos, (*ArgInfo)(d))

	return nil
}

func (d *argDirective) QueryFragment() (string, error) {
	return "", nil
}

func (d *argDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
	return nil
}

func (d *argDirective) Fragment() (string, error) {
	return "", nil
}

func init() {
	info.RegistDirectiveFactory(func() info.Directive {
		return &argDirective{}
	}, "arg")
}
