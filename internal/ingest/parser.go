package ingest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// lineReader splits a byte stream into lines on '\n', trimming a
// trailing '\r' for CRLF input. It grows its own accumulation buffer
// rather than relying on bufio.Scanner's fixed token-size limit, so
// arbitrarily long lines are supported.
type lineReader struct {
	br  *bufio.Reader
	buf []byte
}

func newLineReader(r io.Reader, bufSize int) *lineReader {
	if bufSize <= 0 {
		bufSize = defaultBufferSize
	}
	return &lineReader{br: bufio.NewReaderSize(r, bufSize)}
}

// readLine returns the next line, without its terminator. The returned
// slice aliases the lineReader's internal buffer and is only valid until
// the next call to readLine. It returns io.EOF once no more data remains.
func (lr *lineReader) readLine() ([]byte, error) {
	lr.buf = lr.buf[:0]
	for {
		chunk, err := lr.br.ReadSlice('\n')
		lr.buf = append(lr.buf, chunk...)
		if err == nil {
			break
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			if len(lr.buf) == 0 {
				return nil, io.EOF
			}
			break
		}
		return nil, err
	}

	line := lr.buf
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

// splitFields splits line on delim using repeated bytes.IndexByte scans
// (vectorized by the runtime on amd64/arm64), appending results into dst
// to avoid a fresh slice-of-slices allocation per line. This is the hot
// path for the common, non-quoted case.
func splitFields(dst [][]byte, line []byte, delim byte) [][]byte {
	dst = dst[:0]
	rest := line
	offset := 0
	for {
		idx := bytes.IndexByte(rest, delim)
		if idx < 0 {
			dst = append(dst, line[offset:])
			return dst
		}
		dst = append(dst, line[offset:offset+idx])
		rest = rest[idx+1:]
		offset += idx + 1
	}
}

// splitFieldsQuoted splits line on delim with RFC4180-style quoting:
// a field wrapped in '"' may contain delim or literal '"' (escaped as
// '""'). It does NOT support a quoted field spanning multiple lines
// (an embedded literal newline) — Cordage's ingestion reads line-by-line,
// so that case is rejected as an error rather than silently mishandled.
func splitFieldsQuoted(dst [][]byte, line []byte, delim byte) ([][]byte, error) {
	dst = dst[:0]
	i, n := 0, len(line)
	for {
		if i < n && line[i] == '"' {
			var field []byte
			i++
			for {
				if i >= n {
					return nil, fmt.Errorf("unterminated quoted field")
				}
				if line[i] == '"' {
					if i+1 < n && line[i+1] == '"' {
						field = append(field, '"')
						i += 2
						continue
					}
					i++
					break
				}
				field = append(field, line[i])
				i++
			}
			dst = append(dst, field)
			switch {
			case i < n && line[i] == delim:
				i++
				continue
			case i >= n:
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected character after quoted field at offset %d", i)
			}
		}

		start := i
		idx := bytes.IndexByte(line[i:], delim)
		if idx < 0 {
			dst = append(dst, line[start:])
			return dst, nil
		}
		dst = append(dst, line[start:start+idx])
		i = start + idx + 1
	}
}

// Value is a small tagged union produced by parsing one field, tagged by
// which of Str/I64/F64 is meaningful.
type Value struct {
	Type ColumnType
	Str  string
	I64  int64
	F64  float64
}

// FieldParser converts a raw field's bytes into a typed Value. It is a
// strategy, not a hardcoded call, specifically so the numeric variants
// can be swapped later for hand-rolled byte-level parsing (a common
// 1BRC-style optimization) without changing RowParser.
type FieldParser func(field []byte) (Value, error)

func parseStringField(field []byte) (Value, error) {
	return Value{Type: TypeString, Str: string(field)}, nil
}

func parseInt64Field(field []byte) (Value, error) {
	v, err := strconv.ParseInt(string(field), 10, 64)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeInt64, I64: v}, nil
}

func parseFloat64Field(field []byte) (Value, error) {
	v, err := strconv.ParseFloat(string(field), 64)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeFloat64, F64: v}, nil
}

func defaultFieldParser(t ColumnType) FieldParser {
	switch t {
	case TypeInt64:
		return parseInt64Field
	case TypeFloat64:
		return parseFloat64Field
	default:
		return parseStringField
	}
}

func fieldParsersFor(schema Schema) []FieldParser {
	parsers := make([]FieldParser, len(schema.Columns))
	for i, cs := range schema.Columns {
		parsers[i] = defaultFieldParser(cs.Type)
	}
	return parsers
}

// rowResult is the outcome of one RowParser.next call.
type rowResult int

const (
	rowOK rowResult = iota
	rowSkipped
	rowEOF
)

// RowParser reads and parses rows from a single reader (one chunk's
// worth of a source) into a shared Batch. schema.Columns must already be
// resolved (never inferred lazily here) since the Batch handed to next
// is shaped from the schema before any RowParser is constructed.
// skipHeader, when true, causes the first line read to be discarded as
// a header rather than parsed as data.
type RowParser struct {
	schema        Schema
	parsers       []FieldParser
	lr            *lineReader
	fieldsBuf     [][]byte
	valuesBuf     []Value
	lineNo        int64
	skipHeader    bool
	headerHandled bool
	onError       ErrorPolicy
	onSkippedRow  func(lineNo int64, err error)
}

func newRowParser(r io.Reader, schema Schema, bufSize int, skipHeader bool, onError ErrorPolicy, onSkippedRow func(int64, error)) *RowParser {
	return newRowParserFromReader(newLineReader(r, bufSize), schema, skipHeader, onError, onSkippedRow)
}

// newRowParserFromReader builds a RowParser around an already-primed
// lineReader — used when the caller already consumed the header line
// from lr while resolving the schema, and must keep reading from that
// same reader (its internal buffer may hold bytes beyond the header).
func newRowParserFromReader(lr *lineReader, schema Schema, skipHeader bool, onError ErrorPolicy, onSkippedRow func(int64, error)) *RowParser {
	return &RowParser{
		schema:       schema,
		parsers:      fieldParsersFor(schema),
		lr:           lr,
		skipHeader:   skipHeader,
		onError:      onError,
		onSkippedRow: onSkippedRow,
	}
}

func (rp *RowParser) handleHeader() (rowResult, error) {
	rp.headerHandled = true
	if !rp.skipHeader {
		return rowOK, nil
	}
	_, err := rp.lr.readLine()
	if err == io.EOF {
		return rowEOF, nil
	}
	if err != nil {
		return rowSkipped, fmt.Errorf("ingest: read header line: %w", err)
	}
	return rowOK, nil
}

// next reads and parses one data row into batch. It returns rowEOF once
// the underlying reader is exhausted. A non-nil error is always fatal
// (I/O failure, or a content error under ErrorPolicyFail); a content
// error under ErrorPolicySkip instead yields (rowSkipped, nil) after
// invoking onSkippedRow.
func (rp *RowParser) next(batch *Batch) (rowResult, error) {
	if !rp.headerHandled {
		if res, err := rp.handleHeader(); res != rowOK || err != nil {
			return res, err
		}
	}

	line, err := rp.lr.readLine()
	if err == io.EOF {
		return rowEOF, nil
	}
	if err != nil {
		return rowSkipped, fmt.Errorf("ingest: read line: %w", err)
	}
	rp.lineNo++

	var splitErr error
	if rp.schema.QuoteAware {
		rp.fieldsBuf, splitErr = splitFieldsQuoted(rp.fieldsBuf, line, rp.schema.delimiterOrDefault())
	} else {
		rp.fieldsBuf = splitFields(rp.fieldsBuf, line, rp.schema.delimiterOrDefault())
	}
	if splitErr != nil {
		return rp.handleRowError(rp.lineNo, splitErr)
	}
	if len(rp.fieldsBuf) != len(rp.parsers) {
		return rp.handleRowError(rp.lineNo, fmt.Errorf("expected %d fields, got %d", len(rp.parsers), len(rp.fieldsBuf)))
	}

	// Parse every field into a scratch buffer first and only append to
	// batch once the whole row is known-good — otherwise a failure on a
	// later column would leave earlier columns with an orphaned value
	// for a row that's ultimately skipped, misaligning every column.
	if cap(rp.valuesBuf) < len(rp.fieldsBuf) {
		rp.valuesBuf = make([]Value, len(rp.fieldsBuf))
	}
	values := rp.valuesBuf[:len(rp.fieldsBuf)]
	for i, field := range rp.fieldsBuf {
		v, err := rp.parsers[i](field)
		if err != nil {
			return rp.handleRowError(rp.lineNo, fmt.Errorf("column %q: %w", rp.schema.Columns[i].Name, err))
		}
		values[i] = v
	}

	for i, v := range values {
		switch v.Type {
		case TypeString:
			batch.Cols[i].appendString(v.Str)
		case TypeInt64:
			batch.Cols[i].appendInt64(v.I64)
		case TypeFloat64:
			batch.Cols[i].appendFloat64(v.F64)
		}
	}
	batch.NumRows++
	return rowOK, nil
}

func (rp *RowParser) handleRowError(lineNo int64, err error) (rowResult, error) {
	if rp.onError == ErrorPolicyFail {
		return rowSkipped, fmt.Errorf("ingest: line %d: %w", lineNo, err)
	}
	if rp.onSkippedRow != nil {
		rp.onSkippedRow(lineNo, err)
	}
	return rowSkipped, nil
}
