package authalog

import (
	"fmt"
	"sort"
)

type binding struct {
	k int64
	v Term
}

const ENV_FIXED_LENGTH = 8

type environment struct {
	bindings  [ENV_FIXED_LENGTH]binding
	extension []binding
	count     int
}

func (e *environment) forEach(cb func(k int64, v Term)) {
	for i := 0; i < e.count && i < ENV_FIXED_LENGTH; i++ {
		cb(e.bindings[i].k, e.bindings[i].v)
	}

	for _, b := range e.extension {
		cb(b.k, b.v)
	}
}

func emptyEnvironment() environment {
	return environment{}
}

func (e *environment) reset() {
	e.count = 0
	e.extension = nil
}

func rewritten(a environment, chaser environment) environment {
	ret := environment{
		count: a.count,
	}

	for i := 0; i < a.count && i < ENV_FIXED_LENGTH; i++ {
		b := a.bindings[i]
		ret.bindings[i].k = b.k
		ret.bindings[i].v = chaser.chase(b.v)
	}

	if a.count > ENV_FIXED_LENGTH {
		ret.extension = make([]binding, a.count-ENV_FIXED_LENGTH)

		for i := 0; i < a.count-ENV_FIXED_LENGTH; i++ {
			b := a.extension[i]
			ret.extension[i].k = b.k
			ret.extension[i].v = chaser.chase(b.v)
		}
	}
	return ret
}

func (e *environment) chase(t Term) Term {
	if t.IsConstant {
		return t
	}

	for i := 0; i < e.count; i++ {
		b := e.bindings[i]
		if b.k == t.Value {
			return b.v
		}
	}
	for _, b := range e.extension {
		if b.k == t.Value {
			return b.v
		}
	}
	return t
}

func (e *environment) bind(id int64, t Term) {
	if !t.IsConstant && id == t.Value {
		panic(fmt.Sprintf("Binding variable to itself: %v", t.Value))
	}
	for i := 0; i < e.count; i++ {
		b := e.bindings[i]
		if b.k == id {
			panic(fmt.Sprintf("Cannot rebind variables: %v. old: %v new : %v", id, b.v, t))
		}
	}
	for _, b := range e.extension {
		if b.k == id {
			panic(fmt.Sprintf("Cannot rebind variables: %v. old: %v new : %v", id, b.v, t))
		}
	}

	if e.count < ENV_FIXED_LENGTH {
		e.bindings[e.count] = binding{id, t}
	} else {
		e.extension = append(e.extension, binding{id, t})
	}
	e.count++
}

func (t Term) unify(other Term, env *environment) bool {
	// TODO should move the check for aboslute equality here?
	if t.IsConstant && other.IsConstant {
		return false
	} else if other.IsConstant {
		env.bind(t.Value, other)
	} else {
		env.bind(other.Value, t)
	}
	return true
}

func unify(a Literal, b Literal, in *environment) bool {
	if a.Predicate != b.Predicate || len(a.Terms) != len(b.Terms) {
		return false
	}

	for i := range a.Terms {

		at := in.chase(a.Terms[i])
		bt := in.chase(b.Terms[i])

		if at != bt {
			success := at.unify(bt, in)
			if !success {
				return false
			}
		}
	}
	return true
}

// mutates env
func (g *goal) freshenIn(l Literal, env *environment) Literal {
	return freshenIn(l, &g.varCount, env)
}

// mutates env
func freshenIn(l Literal, count *int64, env *environment) Literal {
	result := Literal{
		Negated:   l.Negated,
		Predicate: l.Predicate,
		Terms:     make([]Term, len(l.Terms)),
	}
	for i, v := range l.Terms {
		if v.IsConstant {
			result.Terms[i] = v
		} else {
			if newId := env.chase(v); newId != v {
				v = newId
			} else {
				*count--
				t := Term{IsConstant: false, Value: *count}
				env.bind(v.Value, t)
				v = t
			}
			result.Terms[i] = v
		}
	}
	return result
}

func freshen(c Clause, counter *int64) (Clause, environment) {
	resultEnv := emptyEnvironment()
	result := Clause{}
	result.Head = freshenIn(c.Head, counter, &resultEnv)
	result.Body = make([]Literal, len(c.Body))
	for i, l := range c.Body {
		result.Body[i] = freshenIn(l, counter, &resultEnv)
	}
	return result, resultEnv
}

// Used mostly for debug printing
func (env environment) fullmap() string {
	keys := make([]int64, 0)
	env.forEach(func(k int64, v Term) {
		keys = append(keys, k)
	})
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	s := ""
	for _, k := range keys {
		t := env.chase(Term{IsConstant: false, Value: k})
		if t.IsConstant {
			s = s + fmt.Sprintf("%v -> c%v\n", k, t.Value)
		} else {
			s = s + fmt.Sprintf("%v -> v%v\n", k, t.Value)
		}
	}
	return s
}

func (env environment) rewrite(l Literal) Literal {
	var result = Literal{
		Negated:   l.Negated,
		Predicate: l.Predicate,
		Terms:     make([]Term, len(l.Terms)),
	}
	for i, t := range l.Terms {
		result.Terms[i] = env.chase(t)
	}

	return result
}

func (env environment) rewriteClause(c Clause) Clause {
	newHead := env.rewrite(c.Head)

	newBody := make([]Literal, len(c.Body))
	for i, c := range c.Body {
		newBody[i] = env.rewrite(c)
	}
	return Clause{
		newHead,
		newBody,
	}
}
