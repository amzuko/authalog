package authalog

import "fmt"

type interner interface {
	intern(str string) int64
	lookup(value int64) string
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
		ok, env := unify(r, sg.Literal, emptyEnvironment())
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
