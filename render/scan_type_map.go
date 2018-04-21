package render

import (
	"encoding/json"
	"fmt"
	"io"
)

var (
	// DefaultScanTypeMap maps all types to null.XXXX (github.com/guregu/null)
	DefaultScanTypeMap = ScanTypeMap{
		// For not nullable.
		map[string]string{
			"int":       "null.Int",
			"uint":      "null.Int",
			"int8":      "null.Int",
			"uint8":     "null.Int",
			"int16":     "null.Int",
			"uint16":    "null.Int",
			"int32":     "null.Int",
			"uint32":    "null.Int",
			"int64":     "null.Int",
			"uint64":    "null.Int",
			"float32":   "null.Float",
			"float64":   "null.Float",
			"bool":      "null.Bool",
			"[]byte":    "null.String",
			"string":    "null.String",
			"time.Time": "null.Time",
		},
		// For nullable.
		map[string]string{
			"int":       "null.Int",
			"uint":      "null.Int",
			"int8":      "null.Int",
			"uint8":     "null.Int",
			"int16":     "null.Int",
			"uint16":    "null.Int",
			"int32":     "null.Int",
			"uint32":    "null.Int",
			"int64":     "null.Int",
			"uint64":    "null.Int",
			"float32":   "null.Float",
			"float64":   "null.Float",
			"bool":      "null.Bool",
			"[]byte":    "null.String",
			"string":    "null.String",
			"time.Time": "null.Time",
		},
	}
)

// ScanTypeMap maps primitive scan type to scan type.
// [0] is for not nullable types.
// [1] is for nullable types.
type ScanTypeMap []map[string]string

func NewScanTypeMap() ScanTypeMap {
	return ScanTypeMap{
		map[string]string{},
		map[string]string{},
	}
}

func (m *ScanTypeMap) Load(r io.Reader) error {
	*m = NewScanTypeMap()
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(m); err != nil {
		return err
	}
	if len(*m) != 2 {
		return fmt.Errorf("Scan type map should have length 2 (one for not-nullable and one for nullable)")
	}
	return nil
}

// Merge two ScanTypeMap and return the new merged one.
func (m ScanTypeMap) Merge(n ScanTypeMap) ScanTypeMap {
	ret := NewScanTypeMap()
	for i := 0; i < 2; i++ {
		for k, v := range m[i] {
			ret[i][k] = v
		}
	}
	for i := 0; i < 2; i++ {
		for k, v := range n[i] {
			ret[i][k] = v
		}
	}
	return ret
}
