package authalog

import (
	"fmt"
	"sort"
)

func (db *Database) checkClause(c Clause) error {
	if c.Head.Negated {
		return fmt.Errorf("Clause heads cannot be negated")
	}

	// Check if all variables in the head are bound in the body
	headVariables := map[int64]struct{}{}
	bodyPositiveVariables := map[int64]struct{}{}
	bodyNegativeVariables := map[int64]struct{}{}

	for _, t := range c.Head.Terms {
		if !t.IsConstant {
			headVariables[t.Value] = struct{}{}
		}
	}
	for _, l := range c.Body {
		for _, t := range l.Terms {
			if !t.IsConstant {
				if l.Negated {
					bodyNegativeVariables[t.Value] = struct{}{}
				} else {
					bodyPositiveVariables[t.Value] = struct{}{}
				}
			}
		}
	}

	for k := range headVariables {
		if _, ok := bodyNegativeVariables[k]; ok {
			continue
		}
		if _, ok := bodyPositiveVariables[k]; ok {
			continue
		}
		return fmt.Errorf("variable \"%v\" bound in clause head, but not in body. All variables in clause heads must also be bound in their bodies", db.termString(Term{Value: k}))
	}

	// Make sure all variables in negated literals are bound in positive ones.
	for k := range bodyNegativeVariables {
		if _, ok := bodyPositiveVariables[k]; ok {
			continue
		}
		return fmt.Errorf("variable '%v' bound in negated literal, not bound in positive literal. All variables bound in a negated literal must also be bound in a positive one", db.termString(Term{Value: k}))
	}
	return nil
}

func preprocess(c Clause) Clause {
	if len(c.Body) == 0 {
		return c
	}
	// Copy the clause
	n := Clause{
		Head: c.Head,
		Body: make([]Literal, len(c.Body)),
	}
	for i, l := range c.Body {
		n.Body[i] = l
	}

	// Push all negated literals to the end of the bodies.
	sort.SliceStable(n.Body, func(i, j int) bool {
		if !n.Body[i].Negated && n.Body[j].Negated {
			return true
		}
		return false
	})
	return n
}
