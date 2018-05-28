package models

import (
	"fmt"
)

// Slicer represents an appendable slice.
type Slicer interface {
	// Len returns the length of the slice.
	Len() int

	// Item returns the i-th item of the slice. i should be within [0, Len()), otherwise a panic will be raised.
	Item(i int) interface{}

	// Append an item to the slice. If item is nil, then an empty item should be appended.
	Append(item interface{})
}

// Slicers represents a slice of Slicer.
type Slicers interface {
	// Len returns the number of Slicer.
	Len() int

	// Slicer returns the i-th Slicer.
	Slicer(i int) Slicer

	// Append appends an empty Slicer.
	Append()
}

// groupBy groups a slice of items by table row in each item.
//
// `src` is the source slice, a table row (TableRowWithPrimary) is extracted from each item of the slice through `selector`.
//
// `trs` is the slice of the table row. `groups` is slice of slice of items.
func groupBy(src Slicer, selector func(interface{}) TableRowWithPrimary, trs Slicer, groups Slicers) {

	if trs.Len() != 0 || groups.Len() != 0 {
		panic(fmt.Errorf("Expect empty slices"))
	}

	p2Idx := make(map[TableRowPrimaryValue]int) // PrimaryValue -> index of trs/groups
	for i := 0; i < src.Len(); i++ {

		item := src.Item(i)
		tr := selector(item)
		p := tr.PrimaryValue()

		// Skip NULL primary values.
		if p == nil {
			continue
		}

		idx, ok := p2Idx[p]
		if !ok {
			// A new PrimaryValue found.
			trs.Append(tr)
			groups.Append()
			idx = trs.Len() - 1
			p2Idx[p] = idx
		}

		groups.Slicer(idx).Append(item)

	}

}
