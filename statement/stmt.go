package statement

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbctx"
)

// StmtInfo contains information of a statement.
type StmtInfo struct {
	stmtType string // 'SELECT'/'UPDATE'/'INSERT'/'DELETE'
	stmtName string
	stmtText string
	locals   map[interface{}]interface{}

	// for SELECT stmt only
	resultColumnNames []string
	resultColumnTypes []*sql.ColumnType
}

// NewStmtInfo creates a new StmtInfo from an xml element, example statement xml element:
//
//   <select name="BlogByUser">
//     <arg name="userId" type="int" />
//     SELECT <wildcard table="blog" /> FROM blog WHERE user_id=<replace with=":userId">1</replace>
//   </select>
//
// A statement xml element contains SQL statement fragments and special directives.
func NewStmtInfo(ctx *dbctx.DBCtx, elem *etree.Element) (*StmtInfo, error) {

	info := &StmtInfo{
		locals: map[interface{}]interface{}{},
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

func (info *StmtInfo) processElem(ctx *dbctx.DBCtx, elem *etree.Element) error {

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

		stmtTextQuery := strings.TrimSpace(strings.Join(fragments, ""))

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

	info.stmtText = strings.TrimSpace(strings.Join(fragments, ""))

	return nil
}

// Valid returns true if info != nil.
func (info *StmtInfo) Valid() bool {
	return info != nil
}

// String is same as StmtName.
func (info *StmtInfo) String() string {
	return info.StmtName()
}

// StmtName returns the name of the StmtInfo. It returns "" if info is nil.
func (info *StmtInfo) StmtName() string {
	if info == nil {
		return ""
	}
	return info.stmtName
}

// StmtType returns the statement type, one of "SELECT"/"UPDATE"/"INSERT"/"UPDATE". It returns "" if info is nil.
func (info *StmtInfo) StmtType() string {
	if info == nil {
		return ""
	}
	return info.stmtType
}

// StmtText returns the statment text. It returns "" if info is nil.
func (info *StmtInfo) StmtText() string {
	if info == nil {
		return ""
	}
	return info.stmtText
}

// NumResultColumn returns the number of result columns. It returns 0 if info is nil or it is not "SELECT" statement.
func (info *StmtInfo) NumResultColumn() int {
	if info == nil {
		return 0
	}
	return len(info.resultColumnNames)
}

// ResultColumnName returns the i-th result column name. It returns "" if info is nil or i is out of range.
func (info *StmtInfo) ResultColumnName(i int) string {
	if info == nil {
		return ""
	}
	if i < 0 || i >= len(info.resultColumnNames) {
		return ""
	}
	return info.resultColumnNames[i]
}

// ResultColumnNames returns all result column names. It returns nil if info is nil.
func (info *StmtInfo) ResultColumnNames() []string {
	if info == nil {
		return nil
	}
	return info.resultColumnNames
}

// ResultColumnType returns the i-th result column type. It returns nil if info is nil or i is out of range.
func (info *StmtInfo) ResultColumnType(i int) *sql.ColumnType {
	if info == nil {
		return nil
	}
	if i < 0 || i >= len(info.resultColumnTypes) {
		return nil
	}
	return info.resultColumnTypes[i]
}

// ResultColumnTypes returns all result column types. It returns nil if info is nil.
func (info *StmtInfo) ResultColumnTypes() []*sql.ColumnType {
	if info == nil {
		return nil
	}
	return info.resultColumnTypes
}

// Locals returns the associated value for the given key in StmtInfo's locals map.
// This map is used by directives to store directive specific variables.
func (info *StmtInfo) Locals(key interface{}) interface{} {
	return info.locals[key]
}

// SetLocals set key/value into StmtInfo's locals map. See document in Locals.
func (info *StmtInfo) SetLocals(key, val interface{}) {
	info.locals[key] = val
}
