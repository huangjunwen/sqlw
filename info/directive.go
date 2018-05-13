package info

import (
	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
)

// Directive represents a fragment of a statement.
type Directive interface {
	// Initialize the directive.
	Initialize(loader *datasrc.Loader, db *DBInfo, stmt *StmtInfo, tok etree.Token) error

	// Fragment returns the final text fragment of this directive.
	Fragment() (string, error)

	// QueryFragment returns the text fragment of this directive for SELECT query to obtain result column information.
	// This can be different from Fragment.
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	QueryFragment() (string, error)

	// ProcessQueryResult processes the result column information (in place) of SELECT query.
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error
}

// textDirective is a special directive.
type textDirective struct {
	data string
}

func (d *textDirective) Initialize(loader *datasrc.Loader, db *DBInfo, stmt *StmtInfo, tok etree.Token) error {
	d.data = tok.(*etree.CharData).Data
	return nil
}

func (d *textDirective) Fragment() (string, error) {
	return d.data, nil
}

func (d *textDirective) QueryFragment() (string, error) {
	return d.data, nil
}

func (d *textDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
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
