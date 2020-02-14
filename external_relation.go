package authalog

import "fmt"

type interner interface {
	// Interns a string. Multiple calls with the same string value will
	// return the same int64b
	intern(str string) int64
	// Looks up a string
	lookup(value int64) string
	// Stores a set. Unlike interning a string, there is no garuntee identical
	// sets will be iven the same id
	storeSet(s groundSet) int64
	// Gets a set
	getSet(value int64) groundSet
}

type ExternalRelation struct {
	head Literal
	// External relations are responsible for correctly implementing several things:
	// 	1. All possible variable/ground combinations for input terms
	//  2. Looking up interned strings for constant atoms
	//	3. Interning results
	// external relations must return only constant terms
	// TODO: why not have the infrastructure handle interning and conversion?
	run func(interner, []Term) ([][]Term, error)
}

func (g *goal) runExternalRule(sg *subgoal, rel ExternalRelation) {
	tuples, err := rel.run(g.db, sg.Literal.Terms)
	if err != nil {
		panic(fmt.Sprintf("got error: %v", err))
	}
	for _, tuple := range tuples {
		r := Literal{Predicate: sg.Literal.Predicate, Terms: tuple}
		// Unify with the target. Doing this allows us to generate an env,
		// and make sure that any multiply-bound variables (eg, foo(A, B, A)?)
		// unify correctly, without requiring that all external rules handle that logic.
		env := emptyEnvironment()
		ok := unify(r, sg.Literal, &env)
		if ok {
			g.mergeResultIntoSubgoal(sg, result{
				env:     env,
				Literal: r,
				// TODO: proof? invalidators?
			})
		} else {
			trace("Did not unify", r, "into", sg.Literal)
		}
	}
}
