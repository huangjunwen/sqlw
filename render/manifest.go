package render

import (
	"encoding/json"
	"io"
)

type manifest struct {
	Templates []string `json:"templates"`
}

func newManifest(r io.Reader) (*manifest, error) {
	ret := &manifest{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(ret); err != nil {
		return nil, err
	}
	return ret, nil
}
