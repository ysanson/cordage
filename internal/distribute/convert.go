// Package distribute implements the M3 gRPC coordinator/worker split: a
// coordinator plans newline-aligned byte-range shards of one input file
// (reusing internal/ingest's existing PlanChunks/AlignChunks), dispatches
// one shard per worker over gRPC, and folds the workers' raw, mergeable
// accumulator state (internal/aggregate's ExportState/MergeState) back
// into a single result. internal/aggregate stays proto-agnostic; every
// conversion between its plain Go types and internal/distproto's
// generated protobuf messages lives in this file.
package distribute

import (
	"fmt"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/ingest"
)

func columnTypeToProto(t ingest.ColumnType) (distproto.ColumnType, error) {
	switch t {
	case ingest.TypeString:
		return distproto.ColumnType_COLUMN_TYPE_STRING, nil
	case ingest.TypeInt64:
		return distproto.ColumnType_COLUMN_TYPE_INT64, nil
	case ingest.TypeFloat64:
		return distproto.ColumnType_COLUMN_TYPE_FLOAT64, nil
	default:
		return distproto.ColumnType_COLUMN_TYPE_UNSPECIFIED, fmt.Errorf("distribute: unknown ingest.ColumnType %d", int(t))
	}
}

func protoToColumnType(t distproto.ColumnType) (ingest.ColumnType, error) {
	switch t {
	case distproto.ColumnType_COLUMN_TYPE_STRING:
		return ingest.TypeString, nil
	case distproto.ColumnType_COLUMN_TYPE_INT64:
		return ingest.TypeInt64, nil
	case distproto.ColumnType_COLUMN_TYPE_FLOAT64:
		return ingest.TypeFloat64, nil
	default:
		return 0, fmt.Errorf("distribute: unknown or unspecified ColumnType %v", t)
	}
}

func columnKindToProto(k ingest.ColumnKind) (distproto.ColumnKind, error) {
	switch k {
	case ingest.KindDimension:
		return distproto.ColumnKind_COLUMN_KIND_DIMENSION, nil
	case ingest.KindMeasure:
		return distproto.ColumnKind_COLUMN_KIND_MEASURE, nil
	case ingest.KindIgnore:
		return distproto.ColumnKind_COLUMN_KIND_IGNORE, nil
	default:
		return distproto.ColumnKind_COLUMN_KIND_UNSPECIFIED, fmt.Errorf("distribute: unknown ingest.ColumnKind %d", int(k))
	}
}

func protoToColumnKind(k distproto.ColumnKind) (ingest.ColumnKind, error) {
	switch k {
	case distproto.ColumnKind_COLUMN_KIND_DIMENSION:
		return ingest.KindDimension, nil
	case distproto.ColumnKind_COLUMN_KIND_MEASURE:
		return ingest.KindMeasure, nil
	case distproto.ColumnKind_COLUMN_KIND_IGNORE:
		return ingest.KindIgnore, nil
	default:
		return 0, fmt.Errorf("distribute: unknown or unspecified ColumnKind %v", k)
	}
}

func schemaToProto(s ingest.Schema) (*distproto.Schema, error) {
	cols := make([]*distproto.ColumnSchema, len(s.Columns))
	for i, c := range s.Columns {
		t, err := columnTypeToProto(c.Type)
		if err != nil {
			return nil, err
		}
		k, err := columnKindToProto(c.Kind)
		if err != nil {
			return nil, err
		}
		cols[i] = &distproto.ColumnSchema{Name: c.Name, Type: t, Kind: k}
	}
	return &distproto.Schema{
		Delimiter:  uint32(s.Delimiter),
		HasHeader:  s.HasHeader,
		QuoteAware: s.QuoteAware,
		Columns:    cols,
	}, nil
}

func protoToSchema(p *distproto.Schema) (ingest.Schema, error) {
	if p == nil {
		return ingest.Schema{}, fmt.Errorf("distribute: nil Schema")
	}
	cols := make([]ingest.ColumnSchema, len(p.GetColumns()))
	for i, c := range p.GetColumns() {
		t, err := protoToColumnType(c.GetType())
		if err != nil {
			return ingest.Schema{}, err
		}
		k, err := protoToColumnKind(c.GetKind())
		if err != nil {
			return ingest.Schema{}, err
		}
		cols[i] = ingest.ColumnSchema{Name: c.GetName(), Type: t, Kind: k}
	}
	return ingest.Schema{
		Delimiter:  byte(p.GetDelimiter()),
		HasHeader:  p.GetHasHeader(),
		QuoteAware: p.GetQuoteAware(),
		Columns:    cols,
	}, nil
}

func funcKindToProto(k aggregate.AggFuncKind) (distproto.FuncKind, error) {
	switch k {
	case aggregate.FuncSum:
		return distproto.FuncKind_FUNC_KIND_SUM, nil
	case aggregate.FuncMin:
		return distproto.FuncKind_FUNC_KIND_MIN, nil
	case aggregate.FuncMax:
		return distproto.FuncKind_FUNC_KIND_MAX, nil
	case aggregate.FuncAvg:
		return distproto.FuncKind_FUNC_KIND_AVG, nil
	case aggregate.FuncDistinct:
		return distproto.FuncKind_FUNC_KIND_DISTINCT, nil
	case aggregate.FuncPercentile:
		return distproto.FuncKind_FUNC_KIND_PERCENTILE, nil
	default:
		return distproto.FuncKind_FUNC_KIND_UNSPECIFIED, fmt.Errorf("distribute: unknown aggregate.AggFuncKind %d", int(k))
	}
}

func protoToFuncKind(k distproto.FuncKind) (aggregate.AggFuncKind, error) {
	switch k {
	case distproto.FuncKind_FUNC_KIND_SUM:
		return aggregate.FuncSum, nil
	case distproto.FuncKind_FUNC_KIND_MIN:
		return aggregate.FuncMin, nil
	case distproto.FuncKind_FUNC_KIND_MAX:
		return aggregate.FuncMax, nil
	case distproto.FuncKind_FUNC_KIND_AVG:
		return aggregate.FuncAvg, nil
	case distproto.FuncKind_FUNC_KIND_DISTINCT:
		return aggregate.FuncDistinct, nil
	case distproto.FuncKind_FUNC_KIND_PERCENTILE:
		return aggregate.FuncPercentile, nil
	default:
		return 0, fmt.Errorf("distribute: unknown or unspecified FuncKind %v", k)
	}
}

func aggFuncToProto(f aggregate.AggFunc) (*distproto.AggFunc, error) {
	kind, err := funcKindToProto(f.Kind())
	if err != nil {
		return nil, err
	}
	return &distproto.AggFunc{Kind: kind, Quantile: f.Quantile()}, nil
}

func protoToAggFunc(p *distproto.AggFunc) (aggregate.AggFunc, error) {
	if p == nil {
		return aggregate.AggFunc{}, fmt.Errorf("distribute: nil AggFunc")
	}
	kind, err := protoToFuncKind(p.GetKind())
	if err != nil {
		return aggregate.AggFunc{}, err
	}
	return aggregate.AggFuncFromKind(kind, p.GetQuantile())
}

func specToProto(s aggregate.AggSpec) (*distproto.AggSpec, error) {
	measures := make([]*distproto.MeasureSpec, len(s.Measures))
	for i, ms := range s.Measures {
		funcs := make([]*distproto.AggFunc, len(ms.Funcs))
		for j, f := range ms.Funcs {
			pf, err := aggFuncToProto(f)
			if err != nil {
				return nil, err
			}
			funcs[j] = pf
		}
		measures[i] = &distproto.MeasureSpec{Column: ms.Column, Funcs: funcs}
	}
	return &distproto.AggSpec{
		GroupBy:  append([]string(nil), s.GroupBy...),
		Measures: measures,
	}, nil
}

func protoToSpec(p *distproto.AggSpec) (aggregate.AggSpec, error) {
	if p == nil {
		return aggregate.AggSpec{}, fmt.Errorf("distribute: nil AggSpec")
	}
	measures := make([]aggregate.MeasureSpec, len(p.GetMeasures()))
	for i, ms := range p.GetMeasures() {
		funcs := make([]aggregate.AggFunc, len(ms.GetFuncs()))
		for j, f := range ms.GetFuncs() {
			af, err := protoToAggFunc(f)
			if err != nil {
				return aggregate.AggSpec{}, err
			}
			funcs[j] = af
		}
		measures[i] = aggregate.MeasureSpec{Column: ms.GetColumn(), Funcs: funcs}
	}
	return aggregate.AggSpec{
		GroupBy:  append([]string(nil), p.GetGroupBy()...),
		Measures: measures,
	}, nil
}

func valueToProto(v ingest.Value) (*distproto.Value, error) {
	switch v.Type {
	case ingest.TypeString:
		return &distproto.Value{Kind: &distproto.Value_Str{Str: v.Str}}, nil
	case ingest.TypeInt64:
		return &distproto.Value{Kind: &distproto.Value_I64{I64: v.I64}}, nil
	case ingest.TypeFloat64:
		return &distproto.Value{Kind: &distproto.Value_F64{F64: v.F64}}, nil
	default:
		return nil, fmt.Errorf("distribute: unknown ingest.ColumnType %d", int(v.Type))
	}
}

func protoToValue(p *distproto.Value) (ingest.Value, error) {
	if p == nil {
		return ingest.Value{}, fmt.Errorf("distribute: nil Value")
	}
	switch k := p.GetKind().(type) {
	case *distproto.Value_Str:
		return ingest.Value{Type: ingest.TypeString, Str: k.Str}, nil
	case *distproto.Value_I64:
		return ingest.Value{Type: ingest.TypeInt64, I64: k.I64}, nil
	case *distproto.Value_F64:
		return ingest.Value{Type: ingest.TypeFloat64, F64: k.F64}, nil
	default:
		return ingest.Value{}, fmt.Errorf("distribute: Value has no kind set")
	}
}

func measureStateToProto(m aggregate.MeasureState) (*distproto.MeasureState, error) {
	colType, err := columnTypeToProto(m.ColType)
	if err != nil {
		return nil, err
	}
	return &distproto.MeasureState{
		ColType:      colType,
		SumI64:       m.SumI64,
		MinI64:       m.MinI64,
		MaxI64:       m.MaxI64,
		SumF64:       m.SumF64,
		MinF64:       m.MinF64,
		MaxF64:       m.MaxF64,
		HaveMinMax:   m.HaveMinMax,
		HllRegisters: m.HLLRegisters,
	}, nil
}

func protoToMeasureState(p *distproto.MeasureState) (aggregate.MeasureState, error) {
	if p == nil {
		return aggregate.MeasureState{}, fmt.Errorf("distribute: nil MeasureState")
	}
	colType, err := protoToColumnType(p.GetColType())
	if err != nil {
		return aggregate.MeasureState{}, err
	}
	return aggregate.MeasureState{
		ColType:      colType,
		SumI64:       p.GetSumI64(),
		MinI64:       p.GetMinI64(),
		MaxI64:       p.GetMaxI64(),
		SumF64:       p.GetSumF64(),
		MinF64:       p.GetMinF64(),
		MaxF64:       p.GetMaxF64(),
		HaveMinMax:   p.GetHaveMinMax(),
		HLLRegisters: p.GetHllRegisters(),
	}, nil
}

func groupStateToProto(g aggregate.GroupState) (*distproto.GroupState, error) {
	key := make([]*distproto.Value, len(g.Key))
	for i, v := range g.Key {
		pv, err := valueToProto(v)
		if err != nil {
			return nil, err
		}
		key[i] = pv
	}
	measures := make([]*distproto.MeasureState, len(g.Measures))
	for i, m := range g.Measures {
		pm, err := measureStateToProto(m)
		if err != nil {
			return nil, err
		}
		measures[i] = pm
	}
	return &distproto.GroupState{Key: key, Count: g.Count, Measures: measures}, nil
}

func protoToGroupState(p *distproto.GroupState) (aggregate.GroupState, error) {
	if p == nil {
		return aggregate.GroupState{}, fmt.Errorf("distribute: nil GroupState")
	}
	key := make([]ingest.Value, len(p.GetKey()))
	for i, v := range p.GetKey() {
		gv, err := protoToValue(v)
		if err != nil {
			return aggregate.GroupState{}, err
		}
		key[i] = gv
	}
	measures := make([]aggregate.MeasureState, len(p.GetMeasures()))
	for i, m := range p.GetMeasures() {
		gm, err := protoToMeasureState(m)
		if err != nil {
			return aggregate.GroupState{}, err
		}
		measures[i] = gm
	}
	return aggregate.GroupState{Key: key, Count: p.GetCount(), Measures: measures}, nil
}

func groupStatesToProto(gs []aggregate.GroupState) ([]*distproto.GroupState, error) {
	out := make([]*distproto.GroupState, len(gs))
	for i, g := range gs {
		pg, err := groupStateToProto(g)
		if err != nil {
			return nil, err
		}
		out[i] = pg
	}
	return out, nil
}

func protoToGroupStates(gs []*distproto.GroupState) ([]aggregate.GroupState, error) {
	out := make([]aggregate.GroupState, len(gs))
	for i, g := range gs {
		gg, err := protoToGroupState(g)
		if err != nil {
			return nil, err
		}
		out[i] = gg
	}
	return out, nil
}

func onErrorToProto(p ingest.ErrorPolicy) (string, error) {
	switch p {
	case ingest.ErrorPolicySkip:
		return "skip", nil
	case ingest.ErrorPolicyFail:
		return "fail", nil
	default:
		return "", fmt.Errorf("distribute: unknown ingest.ErrorPolicy %d", int(p))
	}
}

func protoToOnError(s string) (ingest.ErrorPolicy, error) {
	switch s {
	case "", "skip":
		return ingest.ErrorPolicySkip, nil
	case "fail":
		return ingest.ErrorPolicyFail, nil
	default:
		return 0, fmt.Errorf("distribute: invalid on_error %q (want skip or fail)", s)
	}
}
