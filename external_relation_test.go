package authalog

import (
	"strings"
	"testing"
)

func c(i interner, s string) Term {
	return Term{
		IsConstant: true,
		IsAtom:     true,
		Value:      i.intern(s),
	}
}

var testRelation = ExternalRelation{
	head: Literal{
		Predicate: "external",
		Terms: []Term{
			Term{
				Value: 0,
			},
			Term{
				Value: 1,
			},
		},
	},
	run: func(i interner, terms []Term) ([][]Term, error) {
		d := [][]string{
			{"a", "b"},
			{"a", "c"},
			{"a", "d"},
			{"b", "c"},
		}

		var results [][]Term
		for _, ss := range d {
			r := []Term{}
			for _, s := range ss {
				r = append(r, c(i, s))
			}
			results = append(results, r)
		}

		return results, nil
	},
}

func TestExternalRule(t *testing.T) {
	db := NewDatabase([]ExternalRelation{})
	db.ExternalRelations = append(db.ExternalRelations, testRelation)
	cmds, err := db.Parse(strings.NewReader(`
	foo(a).
	foo(b).
	foo(d).
	foo(e).
	bar(X, Y) :- 
		external(X, Y),
		foo(X),
		foo(Y).
	bar(X, Y)?`))
	if err != nil {
		t.Error(err)
	}
	var results []result
	for _, c := range cmds {
		results, err = Apply(c, db)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	r := db.ToString(results)
	compareDatalogResult(t, r,
		`bar(a, b).
bar(a, d).
`)

}
