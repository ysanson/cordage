// Package ingest reads tabular input (files or streams) into typed,
// column-oriented batches, independent of where the bytes come from.
package ingest

import (
	"encoding/json"
	"fmt"
	"os"
)

// ColumnType is the value type a column is parsed into.
type ColumnType int

const (
	TypeString ColumnType = iota
	TypeInt64
	TypeFloat64
)

func (t ColumnType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	default:
		return fmt.Sprintf("ColumnType(%d)", int(t))
	}
}

// ColumnKind describes the role a column plays in queries: a group-by
// label, a numeric value to aggregate, or a column to parse but drop.
type ColumnKind int

const (
	KindDimension ColumnKind = iota
	KindMeasure
	KindIgnore
)

func (k ColumnKind) String() string {
	switch k {
	case KindDimension:
		return "dimension"
	case KindMeasure:
		return "measure"
	case KindIgnore:
		return "ignore"
	default:
		return fmt.Sprintf("ColumnKind(%d)", int(k))
	}
}

// ColumnSchema describes one column of the input.
type ColumnSchema struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
	Kind ColumnKind `json:"kind"`
}

// Schema describes how to split rows into fields and interpret those
// fields. It is the "explainer schema" that makes ingestion generic:
// callers say which column is a group-by key, which are measures, and
// what type each one is.
type Schema struct {
	// Delimiter separates fields on a line. Defaults to ',' if zero.
	Delimiter byte
	// HasHeader indicates the first line names the columns rather than
	// containing data.
	HasHeader bool
	// QuoteAware enables RFC4180-style quoted-field handling (fields
	// wrapped in '"', with delimiters/newlines embedded and '""' as an
	// escaped quote). Off by default: the common case (fixed-width
	// numeric datasets, e.g. 1BRC-style input) has no quoting, and
	// skipping this path keeps the hot field-splitting loop simple.
	QuoteAware bool
	// Columns describes each field in order. If empty and HasHeader is
	// true, columns are inferred from the header line: every column is
	// treated as a string dimension. That's enough for count-style
	// queries; typed measures require an explicit schema.
	Columns []ColumnSchema
}

// delimiterOrDefault returns the configured delimiter, defaulting to ','.
func (s Schema) delimiterOrDefault() byte {
	if s.Delimiter == 0 {
		return ','
	}
	return s.Delimiter
}

// schemaJSON mirrors Schema for JSON (de)serialization, spelling
// Delimiter as a single-character string rather than a raw byte — JSON
// has no byte/uint8 literal, and a one-character string reads far more
// naturally in a hand-written schema file than an escaped byte value.
type schemaJSON struct {
	Delimiter  string         `json:"delimiter"`
	HasHeader  bool           `json:"hasHeader"`
	QuoteAware bool           `json:"quoteAware"`
	Columns    []ColumnSchema `json:"columns"`
}

func (s Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(schemaJSON{
		Delimiter:  string(s.Delimiter),
		HasHeader:  s.HasHeader,
		QuoteAware: s.QuoteAware,
		Columns:    s.Columns,
	})
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	var raw schemaJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw.Delimiter) > 1 {
		return fmt.Errorf("ingest: schema delimiter must be a single byte, got %q", raw.Delimiter)
	}
	var delim byte
	if len(raw.Delimiter) == 1 {
		delim = raw.Delimiter[0]
	}
	s.Delimiter = delim
	s.HasHeader = raw.HasHeader
	s.QuoteAware = raw.QuoteAware
	s.Columns = raw.Columns
	return nil
}

// LoadSchemaFile reads a Schema from a JSON file.
func LoadSchemaFile(path string) (Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Schema{}, fmt.Errorf("ingest: read schema file: %w", err)
	}
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return Schema{}, fmt.Errorf("ingest: parse schema file %s: %w", path, err)
	}
	return s, nil
}

// withInferredColumns returns a copy of the schema with Columns filled in
// from a header line, when the schema didn't specify them explicitly.
// Every inferred column is a string dimension.
func (s Schema) withInferredColumns(header []string) Schema {
	if len(s.Columns) > 0 {
		return s
	}
	cols := make([]ColumnSchema, len(header))
	for i, name := range header {
		cols[i] = ColumnSchema{Name: name, Type: TypeString, Kind: KindDimension}
	}
	s.Columns = cols
	return s
}
