package drepl

import (
	"fmt"

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/info"
)

type replDirective struct {
	origin string
	with   string
}

func (d *replDirective) Initialize(loader *datasrc.Loader, db *info.DBInfo, stmt *info.StmtInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)
	with := elem.SelectAttrValue("with", "")
	if with == "" {
		return fmt.Errorf("Missing 'with' attribute in <repl> directive")
	}
	d.origin = elem.Text()
	d.with = with
	return nil
}

func (d *replDirective) QueryFragment() (string, error) {
	return d.origin, nil
}

func (d *replDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
	return nil
}

func (d *replDirective) Fragment() (string, error) {
	return d.with, nil
}

func init() {
	info.RegistDirectiveFactory(func() info.Directive {
		return &replDirective{}
	}, "repl")
}
