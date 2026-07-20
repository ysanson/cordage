// Package query implements Cordage's small, fixed query grammar and
// translates it directly onto aggregate.AggSpec -- the struct the engine
// already knows how to execute. This is not a general SQL parser: no
// WHERE, no JOIN, no expressions (out of the whole project's scope, see
// DesignDocs/cordage-project-plan.md).
package query

import (
	"fmt"
	"strings"

	"github.com/ysanson/cordage/internal/aggregate"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokStar
	tokLParen
	tokRParen
	tokComma
)

type token struct {
	kind tokenKind
	text string // meaningful only for tokIdent
	pos  int    // byte offset into the source string, for error messages
}

// lex scans s into tokens, always ending with a trailing tokEOF so the
// parser never special-cases end-of-input.
func lex(s string) ([]token, error) {
	var toks []token
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, token{kind: tokLParen, pos: i})
			i++
		case c == ')':
			toks = append(toks, token{kind: tokRParen, pos: i})
			i++
		case c == ',':
			toks = append(toks, token{kind: tokComma, pos: i})
			i++
		case c == '*':
			toks = append(toks, token{kind: tokStar, pos: i})
			i++
		case isIdentStart(c):
			start := i
			i++
			for i < n && isIdentPart(s[i]) {
				i++
			}
			toks = append(toks, token{kind: tokIdent, text: s[start:i], pos: start})
		default:
			return nil, fmt.Errorf("query: unexpected character %q at position %d", c, i)
		}
	}
	toks = append(toks, token{kind: tokEOF, pos: n})
	return toks, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isIdentPart includes '.' so a percentile function name like "p99.9"
// lexes as a single token -- there is no separate NUMBER token class in
// this grammar; quantile literals piggyback on aggregate.ParseAggFunc's
// own float parsing of the ident's suffix.
func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '.'
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) advance() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) expectKeyword(word string) error {
	t := p.peek()
	if t.kind != tokIdent || !strings.EqualFold(t.text, word) {
		return fmt.Errorf("query: expected %s at position %d", word, t.pos)
	}
	p.advance()
	return nil
}

func (p *parser) expectIdent() (string, error) {
	t := p.peek()
	if t.kind != tokIdent {
		return "", fmt.Errorf("query: expected an identifier at position %d", t.pos)
	}
	p.advance()
	return t.text, nil
}

func (p *parser) expect(k tokenKind, what string) error {
	t := p.peek()
	if t.kind != k {
		return fmt.Errorf("query: expected %s at position %d", what, t.pos)
	}
	p.advance()
	return nil
}

type itemKind int

const (
	itemCount itemKind = iota // COUNT(*) -- pseudo-item, contributes nothing to AggSpec
	itemFunc                  // FUNC(column)
	itemBare                  // bare column name
)

type selectItem struct {
	kind   itemKind
	column string // itemFunc, itemBare
	fn     string // itemFunc: raw function-name text, resolved via aggregate.ParseAggFunc
}

// parseSelectItem reads one select-item; disambiguating its three forms
// needs only 1-2 tokens of lookahead after the first ident.
func (p *parser) parseSelectItem() (selectItem, error) {
	name, err := p.expectIdent()
	if err != nil {
		return selectItem{}, fmt.Errorf("query: expected a column, function, or COUNT(*) in the SELECT list at position %d", p.peek().pos)
	}

	if p.peek().kind != tokLParen {
		return selectItem{kind: itemBare, column: name}, nil
	}
	p.advance() // consume '('

	if p.peek().kind == tokStar {
		if !strings.EqualFold(name, "COUNT") {
			return selectItem{}, fmt.Errorf("query: '*' is only valid inside COUNT(*), not %s(*)", name)
		}
		p.advance() // consume '*'
		if err := p.expect(tokRParen, "')'"); err != nil {
			return selectItem{}, err
		}
		return selectItem{kind: itemCount}, nil
	}

	column, err := p.expectIdent()
	if err != nil {
		return selectItem{}, fmt.Errorf("query: expected a column name inside %s(...) at position %d", name, p.peek().pos)
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return selectItem{}, err
	}
	return selectItem{kind: itemFunc, fn: name, column: column}, nil
}

func (p *parser) parseSelectList() ([]selectItem, error) {
	item, err := p.parseSelectItem()
	if err != nil {
		return nil, err
	}
	items := []selectItem{item}
	for p.peek().kind == tokComma {
		p.advance()
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (p *parser) parseColList() ([]string, error) {
	col, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("query: expected at least one column after GROUP BY at position %d", p.peek().pos)
	}
	cols := []string{col}
	for p.peek().kind == tokComma {
		p.advance()
		col, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("query: expected a column after ',' at position %d", p.peek().pos)
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// Parse parses a Cordage query string s into an aggregate.AggSpec.
//
// Grammar (case-insensitive keywords SELECT/GROUP/BY/COUNT; identifiers
// are unquoted alphanumeric/underscore/dot tokens -- there are no string
// literals or quoting in this grammar):
//
//	query       := "SELECT" select-list [ "GROUP" "BY" col-list ]
//	select-list := select-item { "," select-item }
//	select-item := "COUNT" "(" "*" ")"
//	             | ident "(" ident ")"
//	             | ident
//	col-list    := ident { "," ident }
//
// Parse enforces grammar structure plus exactly one cross-reference rule:
// every bare (unwrapped) SELECT item must also appear in GROUP BY --
// mirroring the familiar SQL rule that a plain selected column must be a
// grouping key. The converse is deliberately NOT required: a GROUP BY
// column need not be repeated in SELECT ("SELECT AVG(temp) GROUP BY
// city" is valid) because aggregate.Result always reports every
// GroupByColumns entry regardless of what Measures were requested --
// requiring redundant repetition in SELECT would be an artificial
// restriction the underlying engine doesn't need.
//
// Parse does not consult a schema and cannot catch "column does not
// exist" or "wrong type for this function" -- those are checked
// downstream by aggregate.New, exactly as the CLI's existing
// -group-by/-measure flags already are.
func Parse(s string) (aggregate.AggSpec, error) {
	if strings.TrimSpace(s) == "" {
		return aggregate.AggSpec{}, fmt.Errorf("query: empty query")
	}

	toks, err := lex(s)
	if err != nil {
		return aggregate.AggSpec{}, err
	}
	p := &parser{toks: toks}

	if err := p.expectKeyword("SELECT"); err != nil {
		return aggregate.AggSpec{}, err
	}
	items, err := p.parseSelectList()
	if err != nil {
		return aggregate.AggSpec{}, err
	}

	var groupBy []string
	if t := p.peek(); t.kind == tokIdent && strings.EqualFold(t.text, "GROUP") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return aggregate.AggSpec{}, err
		}
		groupBy, err = p.parseColList()
		if err != nil {
			return aggregate.AggSpec{}, err
		}
	}

	if p.peek().kind != tokEOF {
		return aggregate.AggSpec{}, fmt.Errorf("query: unexpected trailing input at position %d", p.peek().pos)
	}

	groupSet := make(map[string]bool, len(groupBy))
	for _, g := range groupBy {
		groupSet[g] = true
	}

	var measures []aggregate.MeasureSpec
	byColumn := make(map[string]int, len(items))
	for _, it := range items {
		switch it.kind {
		case itemCount:
			// No-op: row count is already always present in GroupResult.
		case itemBare:
			if !groupSet[it.column] {
				return aggregate.AggSpec{}, fmt.Errorf("query: column %q is a bare SELECT item but is not in GROUP BY (add it to GROUP BY, or wrap it in an aggregate function)", it.column)
			}
		case itemFunc:
			fn, err := aggregate.ParseAggFunc(it.fn)
			if err != nil {
				return aggregate.AggSpec{}, fmt.Errorf("query: %w", err)
			}
			if idx, ok := byColumn[it.column]; ok {
				measures[idx].Funcs = append(measures[idx].Funcs, fn)
			} else {
				byColumn[it.column] = len(measures)
				measures = append(measures, aggregate.MeasureSpec{Column: it.column, Funcs: []aggregate.AggFunc{fn}})
			}
		}
	}

	return aggregate.AggSpec{GroupBy: groupBy, Measures: measures}, nil
}
