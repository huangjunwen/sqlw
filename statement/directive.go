package statement

import (
	"database/sql"

	"github.com/beevik/etree"

	"github.com/huangjunwen/sqlw/dbctx"
)

// Directive represents a fragment of a statement.
type Directive interface {
	// Initialize the directive.
	Initialize(ctx *dbctx.DBCtx, stmt *StmtInfo, tok etree.Token) error

	// Generate generates the final text fragment of this directive.
	Generate() (string, error)

	// GenerateQuery generates the text fragment of this directive for SELECT query to obtain result column information.
	// This can be different from Generate().
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	GenerateQuery() (string, error)

	// ProcessQueryResult processes the result column information (in place) of SELECT query.
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error
}

// textDirective is a special directive.
type textDirective struct {
	data string
}

func (d *textDirective) Initialize(ctx *dbctx.DBCtx, stmt *StmtInfo, tok etree.Token) error {
	d.data = tok.(*etree.CharData).Data
	return nil
}

func (d *textDirective) Generate() (string, error) {
	return d.data, nil
}

func (d *textDirective) GenerateQuery() (string, error) {
	return d.data, nil
}

func (d *textDirective) ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error {
	return nil
}

var (
	// Map tag name -> factory
	directiveFactories = map[string]func() Directive{}
)

// RegistDirectiveFactory regist directive factories.
func RegistDirectiveFactory(factory func() Directive, tags ...string) {
	for _, tag := range tags {
		directiveFactories[tag] = factory
	}
}
