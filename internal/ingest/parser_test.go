package ingest

import (
	"io"
	"strings"
	"testing"
)

func TestLineReaderBasic(t *testing.T) {
	lr := newLineReader(strings.NewReader("a\nbb\r\nccc"), 16)

	var got []string
	for {
		line, err := lr.readLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("readLine: %v", err)
		}
		got = append(got, string(line))
	}

	want := []string{"a", "bb", "ccc"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLineReaderLongLineExceedsBuffer(t *testing.T) {
	long := strings.Repeat("x", 1000)
	lr := newLineReader(strings.NewReader(long+"\nshort\n"), 16) // buffer much smaller than the line

	line, err := lr.readLine()
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	if string(line) != long {
		t.Fatalf("readLine returned %d bytes, want %d", len(line), len(long))
	}

	line, err = lr.readLine()
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	if string(line) != "short" {
		t.Errorf("second line = %q, want %q", line, "short")
	}

	if _, err := lr.readLine(); err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestSplitFields(t *testing.T) {
	cases := []struct {
		line string
		want []string
	}{
		{"a,b,c", []string{"a", "b", "c"}},
		{"", []string{""}},
		{"a,,c", []string{"a", "", "c"}},
		{"onlyone", []string{"onlyone"}},
	}
	for _, c := range cases {
		got := splitFields(nil, []byte(c.line), ',')
		if len(got) != len(c.want) {
			t.Fatalf("splitFields(%q) = %v, want %v", c.line, got, c.want)
		}
		for i := range c.want {
			if string(got[i]) != c.want[i] {
				t.Errorf("splitFields(%q)[%d] = %q, want %q", c.line, i, got[i], c.want[i])
			}
		}
	}
}

func TestSplitFieldsQuoted(t *testing.T) {
	line := `Tokyo,"largest, city"`
	fields, err := splitFieldsQuoted(nil, []byte(line), ',')
	if err != nil {
		t.Fatalf("splitFieldsQuoted: %v", err)
	}
	want := []string{"Tokyo", "largest, city"}
	for i, w := range want {
		if string(fields[i]) != w {
			t.Errorf("fields[%d] = %q, want %q", i, fields[i], w)
		}
	}

	escaped := `Osaka,"quote "" inside"`
	fields, err = splitFieldsQuoted(nil, []byte(escaped), ',')
	if err != nil {
		t.Fatalf("splitFieldsQuoted: %v", err)
	}
	if string(fields[1]) != `quote " inside` {
		t.Errorf("fields[1] = %q, want %q", fields[1], `quote " inside`)
	}

	if _, err := splitFieldsQuoted(nil, []byte(`"unterminated`), ','); err == nil {
		t.Error("expected error for unterminated quoted field")
	}
}

func TestDefaultFieldParsers(t *testing.T) {
	v, err := parseStringField([]byte("hello"))
	if err != nil || v.Type != TypeString || v.Str != "hello" {
		t.Errorf("parseStringField = %+v, %v", v, err)
	}

	v, err = parseInt64Field([]byte("42"))
	if err != nil || v.Type != TypeInt64 || v.I64 != 42 {
		t.Errorf("parseInt64Field = %+v, %v", v, err)
	}
	if _, err := parseInt64Field([]byte("nope")); err == nil {
		t.Error("expected error parsing non-numeric int64 field")
	}

	v, err = parseFloat64Field([]byte("3.5"))
	if err != nil || v.Type != TypeFloat64 || v.F64 != 3.5 {
		t.Errorf("parseFloat64Field = %+v, %v", v, err)
	}
	if _, err := parseFloat64Field([]byte("nope")); err == nil {
		t.Error("expected error parsing non-numeric float64 field")
	}
}
