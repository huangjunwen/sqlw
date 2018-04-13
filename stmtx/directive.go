package stmt

import (
	"database/sql"
	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlwrapper/dbctx"
)

// StmtDirective represents a fragment of a statement.
type StmtDirective interface {
	// Initialize the directive.
	Initialize(ctx *dbctx.DBContext, stmt *StmtInfo, tok etree.Token) error

	// Generate() should generate the text fragment.
	Generate() (string, error)

	// GenerateQuery() should generate the text fragment for SELECT query to obtain result column information.
	// This can be different from the Generate().
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	GenerateQuery() (string, error)

	// ProcessQueryResult() should process the result column information of SELECT query.
	//
	// NOTE: If the directive is not for SELECT query, an error should be returned.
	ProcessQueryResult(resultColumnNames *[]string, resultColumnTypes *[]*sql.ColumnType) error
}

type textDirective struct {
	data string
}

func (d *textDirective) Initialize(ctx *dbctx.DBContext, stmt *StmtInfo, tok etree.Token) error {
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
