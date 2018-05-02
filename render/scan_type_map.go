package render

import (
	"encoding/json"
	"fmt"
	"io"
)

var (
	// DefaultScanTypeMap maps all types to null.XXXX (github.com/guregu/null)
	DefaultScanTypeMap = ScanTypeMap{
		"int":       [2]string{"null.Int", "null.Int"},
		"uint":      [2]string{"null.Int", "null.Int"},
		"int8":      [2]string{"null.Int", "null.Int"},
		"uint8":     [2]string{"null.Int", "null.Int"},
		"int16":     [2]string{"null.Int", "null.Int"},
		"uint16":    [2]string{"null.Int", "null.Int"},
		"int32":     [2]string{"null.Int", "null.Int"},
		"uint32":    [2]string{"null.Int", "null.Int"},
		"int64":     [2]string{"null.Int", "null.Int"},
		"uint64":    [2]string{"null.Int", "null.Int"},
		"float32":   [2]string{"null.Float", "null.Float"},
		"float64":   [2]string{"null.Float", "null.Float"},
		"bool":      [2]string{"null.Bool", "null.Bool"},
		"[]byte":    [2]string{"null.String", "null.String"},
		"string":    [2]string{"null.String", "null.String"},
		"time.Time": [2]string{"null.Time", "null.Time"},
	}
)

// ScanTypeMap maps primitive scan type to scan type.
// [0] is for not nullable types.
// [1] is for nullable types.
type ScanTypeMap map[string][2]string

// Load scan type map from io.Reader.
func (m *ScanTypeMap) Load(r io.Reader) error {
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(m); err != nil {
		return err
	}
	for k, _ := range DefaultScanTypeMap {
		v, ok := (*m)[k]
		if !ok {
			return fmt.Errorf("Type %+q in scan type map is missing", k)
		}
		if v[0] == "" {
			return fmt.Errorf("Type %+q in scan type map has empty value for not nullable type", k)
		}
		if v[1] == "" {
			return fmt.Errorf("Type %+q in scan type map has empty value for nullable type", k)
		}
	}
	return nil
}

// Copy the scan type map.
func (m *ScanTypeMap) Copy() ScanTypeMap {
	ret := ScanTypeMap{}
	for k, v := range *m {
		ret[k] = v
	}
	return ret
}
