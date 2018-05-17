package directives

import (
	"fmt"

	"github.com/beevik/etree"
	"github.com/huangjunwen/sqlw/datasrc"
	"github.com/huangjunwen/sqlw/info"
)

type replaceDirective struct {
	origin string
	with   string
}

func (d *replaceDirective) Initialize(loader *datasrc.Loader, db *info.DBInfo, stmt *info.StmtInfo, tok etree.Token) error {
	elem := tok.(*etree.Element)
	with := elem.SelectAttrValue("with", "")
	if with == "" {
		return fmt.Errorf("Missing 'with' attribute in <replace> directive")
	}
	d.origin = elem.Text()
	d.with = with
	return nil
}

func (d *replaceDirective) QueryFragment() (string, error) {
	return d.origin, nil
}

func (d *replaceDirective) ProcessQueryResultColumns(resultCols *[]*datasrc.Column) error {
	return nil
}

func (d *replaceDirective) Fragment() (string, error) {
	return d.with, nil
}

func init() {
	info.RegistDirectiveFactory(func() info.Directive {
		return &replaceDirective{}
	}, "replace")
}
