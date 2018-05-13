package render

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/huangjunwen/sqlw/datasrc"
)

// ScanTypeMap maps data type to scan type.
// [0] is for not nullable types.
// [1] is for nullable types.
type ScanTypeMap map[string][2]string

// NewScanTypeMap loads scan type map from io.Reader.
func NewScanTypeMap(loader *datasrc.Loader, r io.Reader) (ScanTypeMap, error) {
	ret := ScanTypeMap{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&ret); err != nil {
		return nil, err
	}
	for _, dataType := range loader.DataTypes() {
		v, ok := ret[dataType]
		if !ok {
			// If some data type is missing, filled it with "[]byte"
			ret[dataType] = [2]string{"[]byte", "[]byte"}
			continue
		}
		if v[0] == "" {
			return nil, fmt.Errorf("Data type %+q has no not-nullable scan type", dataType)
		}
		if v[1] == "" {
			return nil, fmt.Errorf("Data type %+q has no nullable scan type", dataType)
		}
	}
	return ret, nil

}
