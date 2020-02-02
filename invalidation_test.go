package authalog

import (
	"fmt"
	"strings"
	"testing"
)

func dbFromString(t *testing.T, str string) *Database {
	db := NewDatabase([]ExternalRelation{})
	cs, err := db.Parse(strings.NewReader(str))
	if err != nil {
		t.Error(err)
	}
	for _, c := range cs {
		_, err := db.Apply(c)
		if err != nil {
			t.Error(err)
		}
	}
	return db
}

var data = `
foo(a).
bar(X) :- foo(X).
`

func TestSimpleInvalidations(t *testing.T) {
	db := dbFromString(t, data)

	db.Apply(db.ParseCommandOrPanic("bar(X)?"))

	if len(db.invalidations) != 1 {
		t.Error("Expected 1 invalidations, got", len(db.invalidations))
	}
	if len(db.results) != 2 {
		t.Error("Expected 2 results, got", len(db.results))
	}
	report := db.invalidateLiteral(Literal{Predicate: "foo", Terms: []Term{Term{}}})

	if report.countResultsCleared != 2 {
		t.Error("Expected to clear 2 results, but cleared", report.countResultsCleared)
	}
	if len(db.results) != 0 {
		t.Error("Expected 0 invalidations, got", len(db.results))
	}

	// Reset and invalidate again, but only invalidate the top level bar
	db = dbFromString(t, data)
	db.Apply(db.ParseCommandOrPanic("bar(X)?"))
	report = db.invalidateLiteral(Literal{Predicate: "bar", Terms: []Term{Term{}}})

	if report.countResultsCleared != 1 {
		t.Error("Expected to clear 1 results, but cleared", report.countResultsCleared)
	}
	if len(db.results) != 1 {
		t.Error("Expected 1 invalidations, got", len(db.results))
	}

	fmt.Println(report)
	fmt.Println("")
}
