package authalog

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

type scanner struct {
	r  *bufio.Reader
	db *Database
}

func newScanner(input io.Reader, db *Database) scanner {
	return scanner{bufio.NewReader(input), db}
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isLowerCase(ch rune) bool {
	return (ch >= 'a' && ch <= 'z')
}

func isUpperCase(ch rune) bool {
	return (ch >= 'A' && ch <= 'Z')
}

func isNumber(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

func isLetter(ch rune) bool {
	return isLowerCase(ch) || isUpperCase(ch)
}

func isSingleQuote(ch rune) bool {
	return ch == '\''
}

func isTerminal(ch rune) bool {
	return ch == '?' || ch == '.' || ch == '~'
}

func commandForTerminal(ch rune) CommandType {
	switch ch {
	case '.':
		return CommandAssert
	case '?':
		return CommandQuery
	case '~':
		return CommandRetract
	default:
		panic("invalid terminal rune.")
	}
}

func isAllowedBodyRune(ch rune) bool {
	return isLetter(ch) ||
		isNumber(ch) ||
		(ch == '_' || ch == '-')
}

var eof = rune(0)

func (s scanner) mustConsume(r rune) error {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return err
	}
	if ch != r {
		return fmt.Errorf("Expected %v, but got %v", string(r), string(ch))
	}
	return nil
}

func (s scanner) consumeRestOfLine() {
	for {
		ch, _, err := s.r.ReadRune()
		if err != nil || ch == '\n' {
			break
		}
	}
}

func (s scanner) consumeWhitespace() {
	for {
		ch, _, err := s.r.ReadRune()
		if err != nil || !isWhitespace(ch) {
			if ch == '%' {
				s.consumeRestOfLine()
			} else {
				s.r.UnreadRune()
				return
			}
		}
	}
}

func (s scanner) scanIdentifier() (str string, isAtom bool, err error) {
	s.consumeWhitespace()
	ch, _, err := s.r.ReadRune()
	if !isLetter(ch) && !isNumber(ch) && !isSingleQuote(ch) {
		return str, false, fmt.Errorf("Expected a term startign with a letter or number, but got %v", string(ch))
	}
	if !isSingleQuote(ch) {
		str = str + string(ch)
	} else {
		isAtom = true
	}
	for {
		ch, _, err = s.r.ReadRune()

		if !isAllowedBodyRune(ch) {
			if !isSingleQuote(ch) {
				s.r.UnreadRune()
			}
			return
		}
		str = str + string(ch)
	}
}

// TODO lock??
func (db *Database) intern(str string) int64 {
	if _, ok := db.interned[str]; !ok {
		db.interned[str] = db.internCount
		db.internedLookup[db.internCount] = str
		db.internCount++
	}
	return db.interned[str]
}

func (db *Database) lookup(v int64) string {
	return db.internedLookup[v]
}

func (db *Database) storeSet(s groundSet) int64 {
	db.setLookup[db.internCount] = s
	db.internCount++
	return db.internCount - 1
}

func (db *Database) getSet(v int64) groundSet {
	if s, ok := db.setLookup[v]; ok {
		return s
	}
	panic("Set not found. This should never happen")
}

func (s scanner) scanTerm() (t Term, err error) {
	id, isAtom, err := s.scanIdentifier()

	if err != nil {
		return t, err
	}

	return s.makeTerm(id, isAtom), nil
}

func (s scanner) makeTerm(id string, isAtom bool) (t Term) {
	leading, _ := utf8.DecodeRuneInString(id)

	t.Value = s.db.intern(id)
	if !isUpperCase(leading) || isAtom {
		t.IsConstant = true
	}
	return
}

// the preliminary literal comes over carrying a variable name in
func (s scanner) scanInSet(negated bool, leading string, isAtom bool) (lit Literal, err error) {
	err = s.mustConsume('i')
	if err != nil {
		return
	}
	err = s.mustConsume('n')
	if err != nil {
		return
	}
	s.consumeWhitespace()
	err = s.mustConsume('[')
	if err != nil {
		return
	}
	var r rune
	var t Term
	vals := []int64{}
	for {
		s.consumeWhitespace()
		r, _, err = s.r.ReadRune()
		if err != nil {
			return
		}
		if r == ']' {
			break
		} else {
			err = s.r.UnreadRune()
			if err != nil {
				return
			}
		}

		t, err = s.scanTerm()
		if err != nil {
			return
		}
		if !t.IsConstant {
			return lit, fmt.Errorf("Only constant terms allowed in sets, got: %v", s.db.lookup(t.Value))
		}
		vals = append(vals, t.Value)

		s.consumeWhitespace()
		// Consume an optional comma
		// TODO make commas non optional?

		r, _, err = s.r.ReadRune()
		if err != nil {
			return
		}
		if r != ',' {
			err = s.r.UnreadRune()
			if err != nil {
				return
			}
		}
	}

	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	sVal := s.db.storeSet(groundSet{vals})

	// Reconstitute a literal
	lit.Negated = negated
	lit.Predicate = "in"
	lit.Terms = []Term{
		s.makeTerm(leading, isAtom),
		Term{
			IsConstant: true,
			Value:      sVal,
		},
	}
	return lit, nil
}

func (s scanner) scanLiteral() (lit Literal, err error) {
	negated := false
	leading, _, err := s.r.ReadRune()
	if err != nil {
		return
	}
	if leading == '!' {
		negated = true
		s.consumeWhitespace()
	} else {
		s.r.UnreadRune()
	}

	name, isAtom, err := s.scanIdentifier()
	if err != nil {
		return
	}

	lit = Literal{
		Negated:   negated,
		Predicate: name,
	}
	s.consumeWhitespace()

	// We might have  a 0-arity Literal, so check if we have a period, and return if so.

	ch, _, err := s.r.ReadRune()
	if err != nil {
		return lit, err
	}
	s.r.UnreadRune()
	if isTerminal(ch) {
		return
	}
	// If it's a 'i', we are in a 'A in {}' expression
	if ch == 'i' {
		l, e := s.scanInSet(negated, name, isAtom)
		return l, e
	}

	err = s.mustConsume('(')
	if err != nil {
		return
	}
	// Check if its a zero-arity
	ch, _, err = s.r.ReadRune()
	if err != nil {
		return lit, err
	}
	// ')' closes the Literal
	if ch == ')' {
		return
	}
	s.r.UnreadRune()
	for {
		s.consumeWhitespace()

		t, err := s.scanTerm()
		if err != nil {
			return lit, err
		}
		lit.Terms = append(lit.Terms, t)

		s.consumeWhitespace()

		ch, _, err := s.r.ReadRune()
		if err != nil {
			return lit, err
		}
		// ')' closes the Literal
		if ch == ')' {
			break
		}
		s.r.UnreadRune()
		s.mustConsume(',')
	}
	return
}

func (s scanner) scanCommand() (cmd Command, err error) {
	s.consumeWhitespace()
	cmd.Head, err = s.scanLiteral()
	if err != nil {
		return
	}

	s.consumeWhitespace()
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return cmd, err
	}

	if isTerminal(ch) {
		cmd.CommandType = commandForTerminal(ch)
		return
	}

	s.r.UnreadRune()
	err = s.mustConsume(':')
	if err != nil {
		return
	}
	err = s.mustConsume('-')
	if err != nil {
		return
	}

	for {
		var l Literal
		s.consumeWhitespace()
		l, err = s.scanLiteral()
		if err != nil {
			return
		}
		cmd.Body = append(cmd.Body, l)

		s.consumeWhitespace()

		// Check for terminus
		ch, _, err = s.r.ReadRune()
		if err != nil {
			return
		}
		if ch == '.' {
			return
		}
		if ch == ',' {
			continue
		}
		err = fmt.Errorf("Expected '.' or ',', but got %v", string(ch))
		return
	}
}

func (s scanner) scanOneCommand() (Command, bool, error) {
	s.consumeWhitespace()
	ch, _, err := s.r.ReadRune()

	if ch == eof || err != nil {
		return Command{}, true, nil
	}
	s.r.UnreadRune()

	c, err := s.scanCommand()
	return c, false, err
}

func (db *Database) termString(t Term) string {
	if interned, ok := db.internedLookup[t.Value]; ok {
		leading, _ := utf8.DecodeRuneInString(interned)
		if isUpperCase(leading) {
			// TODO: if we start with a number or a lowercase letter, we don't need quotes
			return "'" + interned + "'"
		} else {
			return interned
		}
	}
	return fmt.Sprintf("Unknown:%v", t.Value)
}

// Should we instead write commands back to disk,
// and focus on providing utility methods to convert Clauses back to commands?
// TODO:consider this.
func (db *Database) writeLiteral(w io.Writer, l *Literal) error {
	_, err := io.WriteString(w, l.Predicate)
	if err != nil {
		return err
	}
	if len(l.Terms) > 0 {
		_, err := io.WriteString(w, "(")
		if err != nil {
			return err
		}
		strs := make([]string, len(l.Terms))
		for i, t := range l.Terms {
			strs[i] = db.termString(t)
		}
		_, err = io.WriteString(w, strings.Join(strs, ", "))
		if err != nil {
			return err
		}

		_, err = io.WriteString(w, ")")
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) writeClause(w io.Writer, c *Clause, t CommandType) error {
	err := db.writeLiteral(w, &c.Head)
	if err != nil {
		return err
	}
	if len(c.Body) > 0 {
		_, err := io.WriteString(w, " :- ")
		for i, l := range c.Body {
			if i > 0 {
				_, err := io.WriteString(w, ", ")
				if err != nil {
					return err
				}
			}
			err = db.writeLiteral(w, &l)
			if err != nil {
				return err
			}
		}
	}
	switch t {
	case CommandAssert:
		_, err = io.WriteString(w, ".\n")
	case CommandQuery:
		_, err = io.WriteString(w, "?\n")
	case CommandRetract:
		_, err = io.WriteString(w, "~\n")
	}
	return err
}

type vardef struct {
	name string
}

func V(s string) interface{} {
	return vardef{s}
}

func C(head Literal, body ...Literal) Clause {
	return Clause{
		Head: head,
		Body: body,
	}
}

func Ask(l Literal) Command {
	return Command{
		Head:        l,
		CommandType: CommandQuery,
	}
}

func Assert(l Literal) Command {
	return Command{
		Head:        l,
		CommandType: CommandAssert,
	}
}

func Negate(l Literal) Literal {
	l.Negated = true
	return l
}

func (db *Database) termFromInterface(t interface{}) Term {
	switch t.(type) {
	case vardef:
		return Term{
			IsConstant: false,
			Value:      db.intern(t.(vardef).name),
		}
	default:
		return Term{
			IsConstant: true,
			Value:      db.intern(fmt.Sprint(t)),
		}
	}
}

func (db *Database) InSet(item interface{}, set ...interface{}) Literal {

	vals := []int64{}

	for _, s := range set {
		st := db.termFromInterface(s)
		if !st.IsConstant {
			// TODO: is there a better way?
			panic("Only pass constant terms to InSet's set")
		}
		vals = append(vals, st.Value)
	}

	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	sVal := db.storeSet(groundSet{vals})

	return Literal{
		Predicate: "in",
		Terms:     []Term{db.termFromInterface(item), Term{IsConstant: true, Value: sVal}},
	}
}

func (db *Database) L(predicate string, terms ...interface{}) Literal {
	ts := make([]Term, len(terms))
	for i, v := range terms {
		ts[i] = db.termFromInterface(v)
	}
	return Literal{
		Predicate: predicate,
		Terms:     ts,
	}
}
