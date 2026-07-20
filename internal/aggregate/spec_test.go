package aggregate

import "testing"

func TestParseAggFunc(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    AggFunc
		wantErr bool
	}{
		{name: "sum", in: "sum", want: Sum},
		{name: "sum upper", in: "SUM", want: Sum},
		{name: "min mixed case", in: "Min", want: Min},
		{name: "max", in: "max", want: Max},
		{name: "avg", in: "avg", want: Avg},
		{name: "distinct", in: "distinct", want: Distinct},
		{name: "p50", in: "p50", want: Percentile(50)},
		{name: "p99.9", in: "p99.9", want: Percentile(99.9)},
		{name: "p uppercase", in: "P75", want: Percentile(75)},
		{name: "p0 invalid", in: "p0", wantErr: true},
		{name: "p100 invalid", in: "p100", wantErr: true},
		{name: "p negative", in: "p-5", wantErr: true},
		{name: "p non-numeric", in: "pxyz", wantErr: true},
		{name: "unknown function", in: "median", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAggFunc(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseAggFunc(%q) = %+v, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAggFunc(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseAggFunc(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}
