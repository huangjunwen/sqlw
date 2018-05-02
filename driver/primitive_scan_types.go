package driver

var (
	// PrimitiveScanTypes is a list of primitive scan types.
	PrimitiveScanTypes = []string{
		"int", "uint", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
		"float32", "float64",
		"bool",
		"[]byte", "string",
		"time.Time",
	}
	primitiveScanTypes = map[string]struct{}{}
)

// IsPrimitiveScanType returns true if typ is one of PrimitiveScanTypes.
func IsPrimitiveScanType(typ string) bool {
	_, ok := primitiveScanTypes[typ]
	return ok
}

func init() {
	for _, typ := range PrimitiveScanTypes {
		primitiveScanTypes[typ] = struct{}{}
	}
}
