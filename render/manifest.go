package render

import (
	"encoding/json"
	"io"
)

type Manifest struct {
	ScanTypeMap    string   `json:"scan_type_map"`
	TableTemplate  string   `json:"table_tmpl"`
	ExtraTemplates []string `json:"extra_tmpls"`
}

func NewManifest(r io.Reader) (*Manifest, error) {
	ret := &Manifest{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(ret); err != nil {
		return nil, err
	}
	return ret, nil
}
