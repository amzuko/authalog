package authalog

import (
	"bytes"
	"fmt"

	uuid "github.com/satori/go.uuid"
)

// l must have been asked directly or returned from a previous ask of the database
func (db *Database) ProofOf(l Literal) ([]proof, bool) {
	id := l.id()
	ps, ok := db.proofs[id]
	return ps, ok
}

func (db *Database) ProofString(l Literal) string {
	result := bytes.NewBufferString("")

	prooved := map[uuid.UUID]struct{}{}
	toProove := []Literal{l}

	for len(toProove) > 0 {
		l = toProove[0]
		toProove = toProove[1:]
		id := l.id()
		if _, ok := prooved[id]; ok {
			continue
		} else {
			prooved[id] = struct{}{}
		}

		ps, ok := db.ProofOf(l)
		if !ok {
			panic(fmt.Sprint("Something went wrong, could not find proof for", l))
		}
		// Work with the first proof, and only the first proof
		p := ps[0]

		// If the fact came from an external relation.
		// TODO: better way to signal this?
		if p.Clause == uuid.Nil {
			db.writeLiteral(result, &l)
			result.WriteString(". % From idb\n")
			continue
		}

		c := db.clauses[p.Clause]
		substituted := p.substitutions.rewriteClause(c)
		db.writeClause(result, &substituted, Assert)
		if len(substituted.Body) > 0 {
			toProove = append(substituted.Body, toProove...)
		}
	}

	return result.String()
}
