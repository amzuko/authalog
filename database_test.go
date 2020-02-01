package authalog

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
)

func parseApplyExecute(t *testing.T, prog string) string {
	db := NewDatabase([]ExternalRelation{})
	cmds, err := db.Parse(strings.NewReader(prog))
	if err != nil {
		t.Errorf("Error parsing: %s", err)
		t.Fail()
	}
	var results []result
	for _, c := range cmds {
		results, err = db.Apply(c)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}

	return db.ToString(results)
}

type pCase struct {
	name     string
	prog     string
	expected string
}

var programCases = []pCase{
	pCase{
		name: "foobar",
		prog: `
		foo(1).
		foo(2).
		foo(3).
		baz(1, 3).
		baz(1, 4).
		bar(A, B) :-
			foo(A),
			foo(B),
			baz(A, B).
		bar(X,Y)?
`,
		expected: `bar(1, 3).
`,
	},
	pCase{
		name: "pq-chenwarren",
		prog: `% p q test from Chen & Warren
	q(X) :- p(X).
	q(a).
	p(X) :- q(X).
	q(X)?

	`,
		expected: `q(a).
`,
	},
	pCase{
		name: "ancestor",
		prog: `ancestor(A, B) :- parent(A, B).
			ancestor(A, B) :- parent(A, C), ancestor(C, B).
			parent(john, douglas).
			parent(bob, john).
			parent(ebbon, bob).
			ancestor(A, B)?`,
		expected: `ancestor(bob, john).
ancestor(ebbon, bob).
ancestor(bob, douglas).
ancestor(ebbon, john).
ancestor(ebbon, douglas).
ancestor(john, douglas).
`,
	},
	pCase{
		name: "path",
		prog: `% path test from Chen & Warren
		edge(a, b). edge(b, c). edge(c, d). edge(d, a).
		path(X, Y) :- edge(X, Y).
		path(X, Y) :- edge(X, Z), path(Z, Y).
		path(X, Y) :- path(X, Z), edge(Z, Y).
		path(X, Y)?
		`,
		expected: `path(a, a).
path(a, b).
path(a, c).
path(a, d).
path(b, a).
path(b, b).
path(b, c).
path(b, d).
path(c, a).
path(c, b).
path(c, c).
path(c, d).
path(d, a).
path(d, b).
path(d, c).
path(d, d).
`,
	},
	pCase{
		name: "Laps",
		prog: `% Laps Test
		contains(ca, store, rams_couch, rams).
		contains(rams, fetch, rams_couch, will).
		contains(ca, fetch, Name, Watcher) :-
		    contains(ca, store, Name, Owner),
		    contains(Owner, fetch, Name, Watcher).
		trusted(ca).
		permit(User, Priv, Name) :-
		    contains(Auth, Priv, Name, User),
		    trusted(Auth).
		permit(User, Priv, Name)?
		`,
		expected: `permit(rams, store, rams_couch).
permit(will, fetch, rams_couch).
`,
	},
	pCase{
		name: "longidentifiers",
		prog: `abcdefghi(z123456789,
		z1234567890123456789,
		z123456789012345678901234567890123456789,
		z1234567890123456789012345678901234567890123456789012345678901234567890123456789).

		this_is_a_long_identifier_and_tests_the_scanners_concat_when_read_with_a_small_buffer.
		this_is_a_long_identifier_and_tests_the_scanners_concat_when_read_with_a_small_buffer?`,
		expected: `this_is_a_long_identifier_and_tests_the_scanners_concat_when_read_with_a_small_buffer.
`,
	},
	pCase{
		name: "zero-arity",
		prog: `true.
		true?
		`,
		expected: `true.
`,
	},
	pCase{
		name: "negation",
		prog: `foo(a). foo(b). bar(a).
				baz(X) :-
					foo(X),
					!bar(X).
				baz(Y)?`,
		expected: `baz(b).
`,
	},
	pCase{
		name: "negationReordering",
		prog: `foo(a). foo(b). bar(a).
				baz(X) :-
					!bar(X),
					foo(X).
				baz(Y)?`,
		expected: `baz(b).
`,
	},
	// 	pCase{
	// 		name: "retraction",
	// 		prog: `foo(a,b).
	//     foo(b,c).
	//     foo(a,b)~
	//     foo(X,Y)?`,
	// 		expected: `foo(b, c).
	// `,
	// 	},
}

func compareDatalogResult(t *testing.T, result string, expected string) {
	if len(result) != len(expected) {
		t.Errorf("Different string lengths. Got:\n'%v'\nExpected:\n'%v'\n", result, expected)
	}
	r := bufio.NewReader(strings.NewReader(result))
	for {
		b, _, _ := r.ReadLine()
		if b == nil {
			break
		}
		s := string(b)
		if !strings.Contains(expected, s) {
			t.Errorf("unexpected solution %s", s)
		}
	}
}

func TestProgramCases(t *testing.T) {
	for _, pCase := range programCases {
		t.Run(pCase.name, func(t *testing.T) {
			result := parseApplyExecute(t, pCase.prog)
			compareDatalogResult(t, result, pCase.expected)
		})
	}
}

func TestProofStruct(t *testing.T) {
	c := programCases[0]
	if c.name != "foobar" {
		t.Error("Wrong case")
	}

	db := NewDatabase([]ExternalRelation{})
	cmds, err := db.Parse(strings.NewReader(c.prog))
	if err != nil {
		t.Errorf("Error parsing: %s", err)
		t.Fail()
	}
	var results []result
	for _, c := range cmds {
		results, err = db.Apply(c)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if c.CommandType == Query {
			for _, r := range results {
				resultString := db.ProofString(r.Literal)
				if r.Literal.String() == "bar(1,3)." {
					if resultString !=
						`bar(1, 3) :- foo(1), foo(3), baz(1, 3).
foo(1).
foo(3).
baz(1, 3).` {
						t.Errorf("nexpected proofstring: %v", resultString)
					}
				}
				fmt.Println("PROOFSTRING FOR LITERAL:", r.Literal)
				fmt.Println(resultString)
			}
		}
	}
}
