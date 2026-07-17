package ingest

import "testing"

func TestLoadSchemaFile(t *testing.T) {
	s, err := LoadSchemaFile("testdata/schema.json")
	if err != nil {
		t.Fatalf("LoadSchemaFile: %v", err)
	}
	if s.Delimiter != ',' {
		t.Errorf("Delimiter = %q, want ','", s.Delimiter)
	}
	if !s.HasHeader {
		t.Errorf("HasHeader = false, want true")
	}
	if len(s.Columns) != 2 {
		t.Fatalf("len(Columns) = %d, want 2", len(s.Columns))
	}
	if s.Columns[0].Name != "city" || s.Columns[0].Type != TypeString || s.Columns[0].Kind != KindDimension {
		t.Errorf("Columns[0] = %+v, want city/string/dimension", s.Columns[0])
	}
	if s.Columns[1].Name != "temperature" || s.Columns[1].Type != TypeFloat64 || s.Columns[1].Kind != KindMeasure {
		t.Errorf("Columns[1] = %+v, want temperature/float64/measure", s.Columns[1])
	}
}

func TestLoadSchemaFileMissing(t *testing.T) {
	if _, err := LoadSchemaFile("testdata/does-not-exist.json"); err == nil {
		t.Fatal("expected error for missing schema file")
	}
}

func TestSchemaWithInferredColumns(t *testing.T) {
	var s Schema
	s = s.withInferredColumns([]string{"a", "b", "c"})
	if len(s.Columns) != 3 {
		t.Fatalf("len(Columns) = %d, want 3", len(s.Columns))
	}
	for i, name := range []string{"a", "b", "c"} {
		if s.Columns[i].Name != name || s.Columns[i].Type != TypeString || s.Columns[i].Kind != KindDimension {
			t.Errorf("Columns[%d] = %+v, want %s/string/dimension", i, s.Columns[i], name)
		}
	}

	// Explicit columns are left untouched.
	explicit := Schema{Columns: []ColumnSchema{{Name: "x", Type: TypeInt64, Kind: KindMeasure}}}
	got := explicit.withInferredColumns([]string{"ignored"})
	if len(got.Columns) != 1 || got.Columns[0].Name != "x" {
		t.Errorf("withInferredColumns overwrote explicit columns: %+v", got.Columns)
	}
}

func TestColumnTypeAndKindString(t *testing.T) {
	if TypeString.String() != "string" || TypeInt64.String() != "int64" || TypeFloat64.String() != "float64" {
		t.Errorf("unexpected ColumnType.String() output")
	}
	if KindDimension.String() != "dimension" || KindMeasure.String() != "measure" || KindIgnore.String() != "ignore" {
		t.Errorf("unexpected ColumnKind.String() output")
	}
}
