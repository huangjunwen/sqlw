package info

import (
	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
)

// Directive represents a fragment of a statement.
type Directive interface {
	// Initialize the directive.
	Initialize(loader *datasrc.Loader, db *DBInfo, stmt *StmtInfo, tok etree.Token) error

	// QueryFragment returns the fragment of this directive to construct a valid SQL query.
	// The SQL query is used to determine statement type, to obtain result column information for SELECT query,
	// and optionally to check SQL correctness.
	QueryFragment() (string, error)

	// ProcessQueryResultColumns processes the result column information (in place) for SELECT query.
	// This method is called only when the query is a SELECT.
	ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error

	// Fragment returns the final fragment of this directive to construct a final statement text.
	// The statement text is no need to be a valid SQL query. It is up to the template to determine how to use it.
	Fragment() (string, error)
}

// textDirective is a special directive.
type textDirective struct {
	data string
}

func (d *textDirective) Initialize(loader *datasrc.Loader, db *DBInfo, stmt *StmtInfo, tok etree.Token) error {
	d.data = tok.(*etree.CharData).Data
	return nil
}

func (d *textDirective) QueryFragment() (string, error) {
	return d.data, nil
}

func (d *textDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
	return nil
}

func (d *textDirective) Fragment() (string, error) {
	return d.data, nil
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
