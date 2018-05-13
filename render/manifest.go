package render

import (
	"encoding/json"
	"fmt"
	"io"
)

type Manifest struct {
	ScanTypeMap string `json:"scan_type_map"` // required
	Templates   struct {
		Table     string   `json:"table"`      // required
		TableTest string   `json:"table_test"` // optional
		Stmt      string   `json:"stmt"`       // required
		StmtTest  string   `json:"stmt_test"`  // optional
		Extra     []string `json:"extra"`      // optional
	} `json:"tmpl"`
}

func NewManifest(r io.Reader) (*Manifest, error) {
	ret := &Manifest{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(ret); err != nil {
		return nil, err
	}

	if ret.ScanTypeMap == "" {
		return nil, fmt.Errorf("Missing 'scan_type_map' in manifest.json")
	}
	if ret.Templates.Table == "" {
		return nil, fmt.Errorf("Missing 'tmpl' > 'table' in manifest.json")
	}
	if ret.Templates.Stmt == "" {
		return nil, fmt.Errorf("Missing 'tmpl' > 'stmt' in manifest.json")
	}
	return ret, nil
}
