package render

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
