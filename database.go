package authalog

import (
	"bytes"
	"encoding/binary"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
)

type Database struct {
	clauseMutex sync.RWMutex
	// Map id to Clause
	clauses map[uuid.UUID]Clause
	// Unindexed external rules
	externalRelations []ExternalRelation

	resultsMutex sync.RWMutex
	// Map Literal id to proof
	// TODO: consider not storing this for all derived facts?
	proofs map[uuid.UUID][]proof
	// Map subgoal hash to results
	// TODO: we only really need the literal for future things
	results map[uuid.UUID][]result
	// Map subgoal hash to other subgoal hashes that depend on it
	invalidations map[uuid.UUID]*invalidation

	internMutex sync.RWMutex
	// Used to freshen all stored clauses, so that there are no name collisions between scopes
	vars int64
	// maps variables and string constants to ints
	interned       map[string]int64
	internedLookup map[int64]string
}

func NewDatabase() *Database {
	d := Database{
		clauses:           map[uuid.UUID]Clause{},
		externalRelations: []ExternalRelation{},
		invalidations:     map[uuid.UUID]*invalidation{},
		proofs:            map[uuid.UUID][]proof{},
		results:           map[uuid.UUID][]result{},
		vars:              0,
		interned:          map[string]int64{},
		internedLookup:    map[int64]string{},
	}
	return &d
}

func (db *Database) AddExternalRelations(er ...ExternalRelation) {
	db.externalRelations = append(db.externalRelations, er...)
}

type proof struct {
	// Success indicates whether the corresponding literal was proven. False indicates that it
	// was not successfully prooven.
	success bool
	Clause  uuid.UUID
	// Substitutions should map variable -> constant for all variables that appear in 'Clause'
	substitutions environment
}

type result struct {
	isFailure bool
	env       environment
	Literal   Literal
	proof     proof
	// subgoal ids to literals
	// TODO: do we need all this? If we just store the uuid's, we can retrieve that literals from the other subgoals in the
	// goal structure.
	invalidators map[uuid.UUID]Literal
}

// Clause id is a hash of the contents. Two Clauses that differ only
// in variable names will have different ids.
func (c Clause) id() uuid.UUID {
	hasher := murmur3.New128()
	hasher.Write([]byte(c.Head.Predicate))
	var buf bytes.Buffer
	//ignore error
	_ = binary.Write(&buf, binary.LittleEndian, c.Head.Terms)

	binary.Write(hasher, binary.LittleEndian, c.Head.Terms)
	for _, l := range c.Body {
		hasher.Write([]byte(l.Predicate))
		binary.Write(hasher, binary.LittleEndian, l.Terms)
	}
	return idFromInts(hasher.Sum128())
}

// This is a tag. two Literals have the same id() if there exists a variable renaming
// wherein they are identical.
func (l Literal) id() uuid.UUID {
	hasher := murmur3.New128()
	if l.Negated {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	hasher.Write([]byte(l.Predicate))
	ids := map[int64]int64{}
	terms := make([]Term, len(l.Terms))
	for i, v := range l.Terms {
		if v.IsConstant {
			terms[i] = v
		} else {
			if newID, ok := ids[v.Value]; ok {
				v.Value = newID
			} else {
				ids[v.Value] = int64(len(ids))
				v.Value = ids[v.Value]
			}
			terms[i] = v
		}
	}
	binary.Write(hasher, binary.LittleEndian, terms)
	return idFromInts(hasher.Sum128())
}

func writeStructuralTag(hasher murmur3.Hash128, literals []Literal) {
	ids := map[int64]int64{}
	for _, l := range literals {
		terms := make([]Term, len(l.Terms))

		if l.Negated {
			hasher.Write([]byte{1})
		} else {
			hasher.Write([]byte{0})
		}
		hasher.Write([]byte(l.Predicate))
		for i, v := range l.Terms {
			if v.IsConstant {
				terms[i] = v
			} else {
				if newID, ok := ids[v.Value]; ok {
					v.Value = newID
				} else {
					ids[v.Value] = int64(len(ids))
					v.Value = ids[v.Value]
				}
				terms[i] = v
			}
			binary.Write(hasher, binary.LittleEndian, terms)
		}
	}
}

func (db *Database) ask(l Literal) []result {
	// Initialize
	goal := goal{
		db:       db,
		l:        l,
		subgoals: map[uuid.UUID]*subgoal{},
		chains:   map[uuid.UUID]*chain{},
		varCount: db.vars,
	}
	id, _ := goal.putSubgoal(l, emptyEnvironment(), []dependent{})

	goal.visitSubgoal(id)

	db.resultsMutex.Lock()
	for id, sg := range goal.subgoals {
		trace("merging", id, sg.Literal.id(), sg.Literal)
		db.mergeResults(sg.Literal, id, sg.results)
		db.recordInvalidations(sg.Literal, id, sg.invalidators)
	}
	db.resultsMutex.Unlock()

	var results []result
	for _, r := range goal.subgoals[id].results {
		results = append(results, r)
	}

	return results
}

func (db *Database) assert(c Clause) {
	db.internMutex.Lock()
	db.clauseMutex.Lock()
	defer db.internMutex.Unlock()
	defer db.clauseMutex.Unlock()
	fresh, _ := freshen(c, &db.vars)
	id := fresh.id()
	db.clauses[id] = fresh
}
