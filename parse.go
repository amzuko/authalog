package authalog

import (
	"bufio"
	"fmt"
	"io"
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
		return Assert
	case '?':
		return Query
	case '~':
		return Retract
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

func (db *Database) intern(str string) int64 {
	if _, ok := db.interned[str]; !ok {
		db.interned[str] = int64(len(db.interned))
		db.internedLookup[db.interned[str]] = str
	}
	return db.interned[str]
}

func (db *Database) lookup(v int64) string {
	return db.internedLookup[v]
}

func (s scanner) scanTerm() (t Term, err error) {

	id, isAtom, err := s.scanIdentifier()

	if err != nil {
		return t, err
	}

	leading, _ := utf8.DecodeRuneInString(id)

	t.Value = s.db.intern(id)
	if !isUpperCase(leading) || isAtom {
		t.IsConstant = true
	}
	return
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

	name, _, err := s.scanIdentifier()
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

	err = s.mustConsume('(')
	if err != nil {
		return
	}
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

func (s scanner) scanCommand() (cmd DatalogCommand, err error) {
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

func (s scanner) scanOneCommand() (DatalogCommand, bool, error) {
	s.consumeWhitespace()
	ch, _, err := s.r.ReadRune()

	if ch == eof || err != nil {
		return DatalogCommand{}, true, nil
	}
	s.r.UnreadRune()

	c, err := s.scanCommand()
	return c, false, err
}

// TODO: UUUID
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
	case Assert:
		_, err = io.WriteString(w, ".\n")
	case Query:
		_, err = io.WriteString(w, "?\n")
	case Retract:
		_, err = io.WriteString(w, "~\n")
	}
	return err
}
