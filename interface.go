package authalog

import (
	"fmt"
	"io"
	"strings"
)

// Term contains either a variable or a constant.
type Term struct {
	// TODO: we could bitpack this into the top bit of the int64 part.
	// 2^63 is still plenty of symbols
	IsConstant bool
	// If term is a constant, value is the constant value.
	// If term is not a constant (ie, is a variable), value contains
	// the variable's id.
	Value int64
}

// CommandType differentiates different possible datalog commands.
type CommandType int

const (
	// Assert - this fact will be added to a database upon application.
	Assert CommandType = iota
	// Query - this command will return the results of querying a database
	// upon application.
	Query
	// TODO
	Retract
)

// DatalogCommand a command to mutate or query a gotalog database.
type DatalogCommand struct {
	Head        Literal
	Body        []Literal
	CommandType CommandType
}

// Parse consumes a reader, producing a slice of datalogCommands.
func (db *Database) Parse(input io.Reader) ([]DatalogCommand, error) {
	s := newScanner(input, db)

	commands := make([]DatalogCommand, 0)

	for {
		c, finished, err := s.scanOneCommand()
		if err != nil || finished {
			return commands, err
		}
		commands = append(commands, c)
	}
}

func (db *Database) ParseCommandOrPanic(str string) DatalogCommand {
	s := newScanner(strings.NewReader(str), db)
	c, _, err := s.scanOneCommand()
	if err != nil {
		panic(err)
	}
	return c
}

// Apply applies a single command.
func (db *Database) Apply(cmd DatalogCommand) ([]result, error) {
	switch cmd.CommandType {
	case Assert:
		c := Clause{
			Head: cmd.Head,
			Body: cmd.Body,
		}
		err := db.checkClause(c)
		if err != nil {
			return nil, err
		}
		c = preprocess(c)

		db.assert(c)
		return nil, nil
	case Query:
		res := db.ask(cmd.Head)
		return res, nil
	default:
		return nil, fmt.Errorf("bogus command - this should never happen")
	}
}

// ToString reformats results for display.
// Coincidentally, it also generates valid datalog.
func (db *Database) ToString(results []result) string {
	str := ""
	for _, result := range results {
		str += result.Literal.Predicate
		if len(result.Literal.Terms) > 0 {
			str += "("
			termStrings := make([]string, len(result.Literal.Terms))

			for i, t := range result.Literal.Terms {
				termStrings[i] = db.termString(t)
			}
			str += strings.Join(termStrings, ", ")
			str += ")"
		}
		str += ".\n"
	}
	return str
}
