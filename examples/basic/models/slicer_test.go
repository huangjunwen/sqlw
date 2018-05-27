package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/volatiletech/null.v6"
)

type Tst struct {
	Id null.Int
}

type nxTstPrimaryValue struct {
	Id null.Int
}

type nxTstSlice []*Tst

type TstResult struct {
	Tst  Tst
	RRId int
}

type nxTstResultSlice []*TstResult

type nxTstResultSlices []nxTstResultSlice

var (
	_ TableRowWithPrimary  = (*Tst)(nil)
	_ TableRowPrimaryValue = nxTstPrimaryValue{}
	_ Slicer               = (*nxTstSlice)(nil)
	_ Slicer               = (*nxTstResultSlice)(nil)
	_ Slicers              = (*nxTstResultSlices)(nil)
)

var (
	tstMeta = NewTableMeta(
		"tst",
		[]string{"id"},
		OptPrimaryColumnNames("id"),
	)
)

func (tr *Tst) TableMeta() *TableMeta {
	return tstMeta
}

func (tr *Tst) ColumnValuers(dest *[]interface{}) {
	*dest = append(*dest,
		tr.Id,
	)
}

func (tr *Tst) ColumnScanners(dest *[]interface{}) {
	*dest = append(*dest,
		&tr.Id,
	)
}

func (tr *Tst) PrimaryValue() TableRowPrimaryValue {
	return nxTstPrimaryValue{
		Id: tr.Id,
	}
}

func (p nxTstPrimaryValue) IsNull() bool {
	return p.Id.IsZero()
}

func (p nxTstPrimaryValue) PrimaryValuers(dest *[]interface{}) {
	*dest = append(*dest,
		p.Id,
	)
}

func (slice *nxTstSlice) Len() int {
	return len(*slice)
}

func (slice *nxTstSlice) Item(i int) interface{} {
	return (*slice)[i]
}

func (slice *nxTstSlice) Append(item interface{}) {
	obj := (*Tst)(nil)
	if item != nil {
		obj = item.(*Tst)
	}
	*slice = append(*slice, obj)
}

func (slice *nxTstResultSlice) Len() int {
	return len(*slice)
}

func (slice *nxTstResultSlice) Item(i int) interface{} {
	return (*slice)[i]
}

func (slice *nxTstResultSlice) Append(item interface{}) {
	obj := (*TstResult)(nil)
	if item != nil {
		obj = item.(*TstResult)
	}
	*slice = append(*slice, obj)
}

func (slice *nxTstResultSlices) Len() int {
	return len(*slice)
}

func (slice *nxTstResultSlices) Slicer(i int) Slicer {
	return &(*slice)[i]
}

func (slices *nxTstResultSlices) Append() {
	*slices = append(*slices, nxTstResultSlice{})
}

func TestGroupBy(t *testing.T) {

	assert := assert.New(t)

	{
		src := nxTstResultSlice{
			&TstResult{
				Tst: Tst{},
			},
			&TstResult{
				Tst: Tst{},
			},
		}

		trs := nxTstSlice{}
		groups := nxTstResultSlices{}

		groupBy(&src, func(item interface{}) TableRowWithPrimary {
			return &item.(*TstResult).Tst
		}, &trs, &groups)

		assert.Len(trs, 0)
		assert.Len(groups, 0)

	}

	{
		src := nxTstResultSlice{
			&TstResult{
				Tst: Tst{
					Id: null.Int{Int: 100, Valid: true},
				},
				RRId: 0,
			},
			&TstResult{
				Tst: Tst{
					Id: null.Int{Int: 200, Valid: true},
				},
				RRId: 1,
			},
			&TstResult{
				Tst: Tst{
					Id: null.Int{Int: 100, Valid: true},
				},
				RRId: 2,
			},
			&TstResult{
				Tst:  Tst{},
				RRId: 3,
			},
		}

		trs := nxTstSlice{}
		groups := nxTstResultSlices{}

		groupBy(&src, func(item interface{}) TableRowWithPrimary {
			return &item.(*TstResult).Tst
		}, &trs, &groups)

		assert.Len(trs, 2)
		assert.Len(groups, 2)

		assert.Len(groups[0], 2)
		assert.Equal(0, groups[0][0].RRId)
		assert.Equal(2, groups[0][1].RRId)

		assert.Len(groups[1], 1)
		assert.Equal(1, groups[1][0].RRId)

	}

}
