package render

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/huangjunwen/sqlw/driver"
)

// ScanTypeMap maps primitive scan type to scan type.
// [0] is for not nullable types.
// [1] is for nullable types.
type ScanTypeMap map[string][2]string

// NewScanTypeMap loads scan type map from io.Reader.
func NewScanTypeMap(r io.Reader) (ScanTypeMap, error) {
	ret := ScanTypeMap{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&ret); err != nil {
		return nil, err
	}
	for _, k := range driver.PrimitiveScanTypes {
		v, ok := ret[k]
		if !ok {
			return nil, fmt.Errorf("Scan type map has no primitive scan type %+q", k)
		}
		if v[0] == "" {
			return nil, fmt.Errorf("Primitive scan type %+q has no not-nullable scan type", k)
		}
		if v[1] == "" {
			return nil, fmt.Errorf("Primitive scan type %+q has no nullable scan type", k)
		}
	}
	return ret, nil

}
