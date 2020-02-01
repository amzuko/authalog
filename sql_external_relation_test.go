package authalog

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setupDB(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE users (
		id integer,
		name text
	);
	INSERT INTO users (id, name) VALUES
	(1, 'Loki'),
	(2, 'Quincy'),
	(3, 'Flo');
	`)
	if err != nil {
		return err
	}
	return nil
}

type mockInterner struct {
	// maps variables and string constants to ints
	interned       map[string]int64
	internedLookup map[int64]string
}

func (db *mockInterner) intern(str string) int64 {
	if _, ok := db.interned[str]; !ok {
		db.interned[str] = int64(len(db.interned))
		db.internedLookup[db.interned[str]] = str
	}
	return db.interned[str]
}

func (db *mockInterner) lookup(v int64) string {
	return db.internedLookup[v]
}

func TestExternalSQLRule(t *testing.T) {
	os.Remove("test.db")
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		t.Error(err)
	}
	err = setupDB(db)
	if err != nil {
		t.Error(err)
	}

	spec := SQLExternalRelationSpec{
		table:   "users",
		columns: []string{"id", "name"},
		types:   []interface{}{0, ""},
	}

	relation, err := CreateSQLExternalRelation(spec, db)
	if err != nil {
		t.Error(err)
	}

	mi := mockInterner{map[string]int64{}, map[int64]string{}}

	// Check full enumeration
	terms, err := relation.run(&mi, makeVars(2))
	if err != nil {
		t.Error(err)
	}

	// Spot check terms
	if len(terms) != 3 {
		t.Errorf("Expectd 3 tuples, got %v", len(terms))
	}
	if mi.lookup(terms[0][0].Value) != "1" {
		t.Errorf("Expectd '1', got %v", mi.lookup(terms[0][0].Value))
	}
	if mi.lookup(terms[0][1].Value) != "Loki" {
		t.Errorf("Expectd '1', got %v", mi.lookup(terms[0][1].Value))
	}

	// Check filtered enumeration
	qTerms := makeVars(2)
	qTerms[0] = Term{
		IsConstant: true,
		IsAtom:     true,
		Value:      mi.intern("2"),
	}
	terms, err = relation.run(&mi, qTerms)
	if err != nil {
		t.Error(err)
	}

	// Spot check terms
	if len(terms) != 1 {
		t.Errorf("Expectd 1 tuples, got %v", len(terms))
	}
	if mi.lookup(terms[0][0].Value) != "2" {
		t.Errorf("Expectd '2', got %v", mi.lookup(terms[0][0].Value))
	}
	if mi.lookup(terms[0][1].Value) != "Quincy" {
		t.Errorf("Expectd 'Quincy', got %v", mi.lookup(terms[0][1].Value))
	}

	qTerms[0] = Term{
		IsConstant: true,
		IsAtom:     true,
		Value:      mi.intern("2"),
	}
	qTerms[1] = Term{
		IsConstant: true,
		IsAtom:     true,
		Value:      mi.intern("Quincy"),
	}
	terms, err = relation.run(&mi, qTerms)
	if err != nil {
		t.Error(err)
	}

	// Spot check terms
	if len(terms) != 1 {
		t.Errorf("Expectd 1 tuples, got %v", len(terms))
	}
	if mi.lookup(terms[0][0].Value) != "2" {
		t.Errorf("Expectd '2', got %v", mi.lookup(terms[0][0].Value))
	}
	if mi.lookup(terms[0][1].Value) != "Quincy" {
		t.Errorf("Expectd 'Quincy', got %v", mi.lookup(terms[0][1].Value))
	}
}
