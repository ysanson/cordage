package ingest

import "sync"

// Column holds one schema column's worth of values across a Batch, in
// the type-specific slice matching its ColumnType. Only one of Strs,
// I64s, F64s is populated, chosen by Type.
//
// Storing columns rather than row structs avoids one allocation per
// field per row at 1BRC scale, and gives later stages (aggregation,
// percentile/distinct-count structures) a layout that's natural to
// operate on in tight, vectorizable loops.
type Column struct {
	Type ColumnType
	Strs []string
	I64s []int64
	F64s []float64
}

func (c *Column) reset() {
	c.Strs = c.Strs[:0]
	c.I64s = c.I64s[:0]
	c.F64s = c.F64s[:0]
}

func (c *Column) appendString(v string)   { c.Strs = append(c.Strs, v) }
func (c *Column) appendInt64(v int64)     { c.I64s = append(c.I64s, v) }
func (c *Column) appendFloat64(v float64) { c.F64s = append(c.F64s, v) }

// Batch is a fixed-shape group of rows, one Column per schema column.
// Batches are produced by Ingest and pooled: call Release when done
// with one so its backing slices can be reused for a future batch.
type Batch struct {
	NumRows int
	Cols    []Column

	pool *batchPool
}

// Release returns the batch to its pool for reuse. After calling
// Release, the batch's contents must not be read or modified.
func (b *Batch) Release() {
	if b.pool != nil {
		b.pool.put(b)
	}
}

// batchPool produces and recycles Batches shaped to a fixed schema and
// batch size, avoiding per-batch allocation of the column slices.
type batchPool struct {
	schema    Schema
	batchSize int
	pool      sync.Pool
}

func newBatchPool(schema Schema, batchSize int) *batchPool {
	p := &batchPool{schema: schema, batchSize: batchSize}
	p.pool.New = func() any { return p.newBatch() }
	return p
}

func (p *batchPool) newBatch() *Batch {
	cols := make([]Column, len(p.schema.Columns))
	for i, cs := range p.schema.Columns {
		cols[i].Type = cs.Type
		switch cs.Type {
		case TypeString:
			cols[i].Strs = make([]string, 0, p.batchSize)
		case TypeInt64:
			cols[i].I64s = make([]int64, 0, p.batchSize)
		case TypeFloat64:
			cols[i].F64s = make([]float64, 0, p.batchSize)
		}
	}
	return &Batch{Cols: cols, pool: p}
}

func (p *batchPool) get() *Batch {
	b := p.pool.Get().(*Batch)
	b.NumRows = 0
	for i := range b.Cols {
		b.Cols[i].reset()
	}
	return b
}

func (p *batchPool) put(b *Batch) {
	p.pool.Put(b)
}
