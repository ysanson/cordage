package query

import (
	"reflect"
	"testing"

	"github.com/ysanson/cordage/internal/aggregate"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    aggregate.AggSpec
		wantErr bool
	}{
		{
			name:  "basic",
			query: "SELECT city, AVG(temp) GROUP BY city",
			want: aggregate.AggSpec{
				GroupBy:  []string{"city"},
				Measures: []aggregate.MeasureSpec{{Column: "temp", Funcs: []aggregate.AggFunc{aggregate.Avg}}},
			},
		},
		{
			name:  "group by column not redundantly selected",
			query: "SELECT AVG(temp) GROUP BY city",
			want: aggregate.AggSpec{
				GroupBy:  []string{"city"},
				Measures: []aggregate.MeasureSpec{{Column: "temp", Funcs: []aggregate.AggFunc{aggregate.Avg}}},
			},
		},
		{
			name:  "count star only, no group by",
			query: "SELECT COUNT(*)",
			want:  aggregate.AggSpec{},
		},
		{
			name:  "count star with group by",
			query: "SELECT COUNT(*) GROUP BY city",
			want:  aggregate.AggSpec{GroupBy: []string{"city"}},
		},
		{
			name:  "lowercase keywords, merged funcs on one column",
			query: "select city, count(*), avg(temp), sum(temp) group by city",
			want: aggregate.AggSpec{
				GroupBy:  []string{"city"},
				Measures: []aggregate.MeasureSpec{{Column: "temp", Funcs: []aggregate.AggFunc{aggregate.Avg, aggregate.Sum}}},
			},
		},
		{
			name:  "multi-column group by, no measures",
			query: "SELECT city, region GROUP BY city, region",
			want:  aggregate.AggSpec{GroupBy: []string{"city", "region"}},
		},
		{
			name:  "percentile function",
			query: "SELECT p95(latency) GROUP BY city",
			want: aggregate.AggSpec{
				GroupBy:  []string{"city"},
				Measures: []aggregate.MeasureSpec{{Column: "latency", Funcs: []aggregate.AggFunc{aggregate.Percentile(95)}}},
			},
		},

		{name: "empty query", query: "", wantErr: true},
		{name: "whitespace only", query: "   ", wantErr: true},
		{name: "missing SELECT", query: "city, AVG(temp) GROUP BY city", wantErr: true},
		{name: "trailing comma in select list", query: "SELECT city,", wantErr: true},
		{name: "trailing comma before GROUP", query: "SELECT city, GROUP BY city", wantErr: true},
		{name: "group by no columns", query: "SELECT AVG(temp) GROUP BY", wantErr: true},
		{name: "group without by", query: "SELECT AVG(temp) GROUP", wantErr: true},
		{name: "bare column not in group by", query: "SELECT temp GROUP BY city", wantErr: true},
		{name: "trailing garbage", query: "SELECT AVG(temp) GROUP BY city extra", wantErr: true},
		{name: "star outside count", query: "SELECT AVG(*) GROUP BY city", wantErr: true},
		{name: "bare star", query: "SELECT *", wantErr: true},
		{name: "unknown function", query: "SELECT median(temp) GROUP BY temp", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) = %+v, want error", tt.query, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.query, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.query, got, tt.want)
			}
		})
	}
}
