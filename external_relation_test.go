package authalog

import (
	"strings"
	"testing"
	"time"
)

func c(i interner, s string) Term {
	return Term{
		IsConstant: true,
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
	db := NewDatabase()
	db.AddExternalRelations(testRelation)
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
		results, err = db.Apply(c)
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

func TestInvalidatingExternalRule(t *testing.T) {
	db := NewDatabase()
	ttl := NewTTLInvalidator(db, 100*time.Millisecond, 10*time.Millisecond)
	db.AddExternalRelations(ttl.InvalidatingRelation(testRelation))

	c := db.ParseCommandOrPanic("external(X, Y)?")

	r, err := db.Apply(c)
	if len(r) != 4 {
		t.Error("Expected 4 results")
	}
	if err != nil {
		t.Error(err)
	}

	if len(db.results) != 1 {
		t.Error("Expected 1 result before starting invalidator")
	}
	// Start the invalidator
	ttl.Start()
	// Give it some time to clean out it's queue
	time.Sleep(200 * time.Millisecond)
	if len(db.results) != 0 {
		t.Error("Expected 0 results after starting invalidator, but got", len(db.results))
	}
}
