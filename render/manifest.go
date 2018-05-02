package render

import (
	"encoding/json"
	"io"
)

type Manifest struct {
	ScanTypeMap    string   `json:"scan_type_map"` // required
	TableTemplate  string   `json:"table_tmpl"`    // required
	StmtTemplate   string   `json:"stmt_tmpl"`     // required
	ExtraTemplates []string `json:"extra_tmpls"`   // optional
}

func NewManifest(r io.Reader) (*Manifest, error) {
	ret := &Manifest{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(ret); err != nil {
		return nil, err
	}
	return ret, nil
}
