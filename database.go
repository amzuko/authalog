package authalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
)

type database struct {
	// Map id to Clause
	Clauses map[uuid.UUID]Clause
	// Map Literal id to proof
	proofs map[uuid.UUID][]proof
	// Map goal hash to results
	results map[uuid.UUID][]result
	// Used to freshen all stored clauses, so that there are no name collisions between scopes
	vars int64

	// maps variables and string constants to ints
	interned       map[string]int64
	internedLookup map[int64]string
}

func newDatabase() *database {
	d := database{
		Clauses:        map[uuid.UUID]Clause{},
		proofs:         map[uuid.UUID][]proof{},
		results:        map[uuid.UUID][]result{},
		vars:           0,
		interned:       map[string]int64{},
		internedLookup: map[int64]string{},
	}
	return &d
}

type Clause struct {
	Head Literal
	Body []Literal
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
	isFailure    bool
	env          environment
	Literal      Literal
	proof        proof
	invalidators []uuid.UUID
}

type Literal struct {
	Negated   bool
	Predicate string
	Terms     []Term
}

func (l Literal) String() string {
	ret := ""
	if l.Negated {
		ret = ret + "!"
	}
	ret = ret + l.Predicate
	if len(l.Terms) > 0 {
		ret = ret + "("
		termStrings := make([]string, len(l.Terms))
		for i, t := range l.Terms {
			if t.IsConstant {
				termStrings[i] = fmt.Sprintf("c%v", t.Value)
			} else {
				termStrings[i] = fmt.Sprintf("v%v", t.Value)
			}
		}
		ret = ret + strings.Join(termStrings, ", ")
		ret = ret + ")"
	}
	return ret
}

func (c Clause) String() string {
	var ret = c.Head.String()
	if len(c.Body) > 0 {
		ret = ret + " :-\n"
		for _, l := range c.Body {
			ret = ret + l.String() + "\n"
		}
	}
	return ret
}

type environment struct {
	bindings map[int64]Term
}

// A A 2
// C 1 C
func (e environment) chase(t Term) Term {
	if t.IsConstant {
		return t
	}
	if bound, ok := e.bindings[t.Value]; ok {
		return bound
	}
	return t
}

func (e environment) bind(id int64, t Term) {
	if !t.IsConstant && id == t.Value {
		panic(fmt.Sprintf("Binding variable to itself: %v", t.Value))
	}

	if existing, ok := e.bindings[id]; ok && existing != t {
		panic(fmt.Sprintf("Cannot rebind variables: %v. old: %v new : %v", id, existing, t))
	}
	e.bindings[id] = t
}

func (t Term) unify(other Term, env environment) bool {
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

func unify(a Literal, b Literal, in environment) (bool, environment) {
	if a.Predicate != b.Predicate || len(a.Terms) != len(b.Terms) {
		return false, environment{}
	}

	for i := range a.Terms {

		at := in.chase(a.Terms[i])
		bt := in.chase(b.Terms[i])

		if at != bt {
			success := at.unify(bt, in)
			if !success {
				return false, in
			}
		}
	}
	return true, in
}

type goal struct {
	db       *database
	varCount int64 // Used to freshen/rename variables
	l        Literal
	// Subgoals are particular literals that need to be derived. They may be derived by any
	// facts or rules in database, and so conceptual represent 'or' nodes in the graph of chains
	// and subgoals that querying constructs.
	subgoals map[uuid.UUID]*subgoal
	// Chains are conjunctions of literals that must be solved together; they are attempted in-order,
	// with additional bindings being 'chained' together to accumulate results. They are initialized
	// from rule bodies.
	chains map[uuid.UUID]*chain
}

func dependentsEqual(a dependent, b dependent) bool {
	if a.recieverID != b.recieverID {
		return false
	}

	if len(a.ClauseMapping) != len(b.ClauseMapping) {
		return false
	}

	for k, va := range a.ClauseMapping {
		if vb, ok := b.ClauseMapping[k]; !ok {
			return false
		} else {
			if va != vb {
				return false
			}
		}
	}

	return true
}

func (g *goal) addDependentToChain(chain *chain, additionaDependents []dependent) {
	for _, d := range additionaDependents {
		notNew := false
		for _, v := range chain.dependents {
			if dependentsEqual(v, d) {
				notNew = true
			}
		}
		if !notNew {
			chain.dependents = append(chain.dependents, d)
			for _, rn := range chain.results {
				newDependent := dependent{d.recieverID, map[int64]Term{}}
				for k, v := range d.ClauseMapping {
					newDependent.ClauseMapping[k] = rn.result.env.chase(v)
				}
				if rn.next == uuid.Nil {
					r := g.resultForDependentSubgoal(chain, rn.result, newDependent)
					g.mergeResultIntoSubgoal(g.subgoals[newDependent.recieverID], r)
				} else {

					nextchain, ok := g.chains[rn.next]
					if !ok {
						panic("not a valid chain")
					}
					g.addDependentToChain(nextchain, []dependent{newDependent})
				}
			}
		}
	}
}

func (g *goal) addDependent(sg *subgoal, additionalDependents []dependent) {
	for _, d := range additionalDependents {
		notNew := false
		for _, v := range sg.dependents {
			if dependentsEqual(v, d) {
				notNew = true
			}
		}
		if !notNew {
			sg.dependents = append(sg.dependents, d)
			// Rewrite the
			// We need to check not just this subgoal's results, but actually
			for _, r := range sg.results {
				// No further chained subgoals, pass the result back into the new dependent
				dresult := g.resultForDependentChain(sg, r, d)
				chain := g.chains[d.recieverID]
				g.mergeResultIntoChain(chain, dresult)
			}
		}
	}
}

func (g *goal) chain(clauseID uuid.UUID, env environment, body []Literal, additionalDependents []dependent) (uuid.UUID, bool) {
	isNew := false

	newBody := make([]Literal, len(body))
	for i, l := range body {
		newBody[i] = env.rewrite(l)
	}

	id := chainHash(clauseID, newBody, env)

	if c, ok := g.chains[id]; ok {
		// Chain already exists.
		// Need to rewrite the clausemapping of the dependents?
		rewriteEnv := emptyEnvironment()
		for i, l := range newBody {
			alwaysTrue, _ := unify(c.body[i], l, rewriteEnv)
			if !alwaysTrue {
				panic("something went very wrong")
			}
		}

		newDependents := make([]dependent, len(additionalDependents))
		for i, d := range additionalDependents {
			newd := dependent{d.recieverID, map[int64]Term{}}
			for k, v := range d.ClauseMapping {
				newd.ClauseMapping[k] = rewriteEnv.chase(v)
			}
			newDependents[i] = newd
		}

		g.addDependentToChain(c, newDependents)
	} else {
		isNew = true
		c := chain{
			clauseId:   clauseID,
			body:       newBody,
			env:        env,
			results:    map[uuid.UUID]resultNext{},
			dependents: additionalDependents,
		}
		g.chains[id] = &c
	}

	return id, isNew
}

func (g *goal) putSubgoal(l Literal, env environment, additionalDependents []dependent) (uuid.UUID, bool) {
	isNew := false
	l = env.rewrite(l)
	id := subgoalHash(l)

	// This looks for structural equality between the subgoals
	if sg, ok := g.subgoals[id]; ok {
		// We need to rewrite the new dependents variable mappings to use
		// the variable names in the existing subgoal.
		// TODO do we need to unify the entire body?
		env := emptyEnvironment()
		alwaysTrue, env := unify(sg.Literal, l, env)
		if !alwaysTrue {
			panic("Something went very wrong")
		}

		newDependents := make([]dependent, len(additionalDependents))
		for i, d := range additionalDependents {
			newDependents[i] = dependent{d.recieverID, map[int64]Term{}}
			// I think that these d.ClauseMapping is always empty, but thats not right
			// TODO can we always completely construct the cluasemapping from the env??
			// Or are there cases wherein we need to pull information from the original dependent's clausempapping? is it always empty?
			for k, v := range d.ClauseMapping {
				newDependents[i].ClauseMapping[k] = env.chase(v)
			}
		}
		g.addDependent(sg, newDependents)
	} else {
		isNew = true
		g.subgoals[id] = &subgoal{
			Literal:    l,
			results:    map[uuid.UUID]result{},
			dependents: additionalDependents,
		}
	}
	return id, isNew
}

type dependent struct {
	// reciever might be a subgoal or a chain
	recieverID uuid.UUID
	// Clausemapping maps the original subgoal's variables to terms.
	ClauseMapping map[int64]Term
}

type resultNext struct {
	result result
	// Next is the uuid of the subgoal generated for this result
	next uuid.UUID
}

type chain struct {
	// The rest of the clause's body. When a result is generated for this
	// subgoal, if this is not empty, rewrite
	clauseId   uuid.UUID
	body       []Literal
	results    map[uuid.UUID]resultNext
	env        environment
	dependents []dependent
}

type subgoal struct {
	// The literal that we are trying to proove.
	Literal Literal
	// Store the results for this subgoal. This stores results for the _positive_ form of
	// the subgoal's literal. If the literal is negated
	// TODO: consider a new structure that would allow
	results map[uuid.UUID]result
	// The dependents of this subgoal (other subgoals that depend on it)
	// These
	dependents []dependent
}

func idFromInts(a uint64, b uint64) uuid.UUID {
	abytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(abytes, a)
	bbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bbytes, b)

	var u uuid.UUID
	// ignore error.
	u.UnmarshalBinary(append(abytes, bbytes...))
	return u
}

func writeHash(hasher murmur3.Hash128, env environment) {

	keys := make([]int64, 0)
	for k := range env.bindings {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		binary.Write(hasher, binary.LittleEndian, k)
		t := env.chase(Term{IsConstant: false, Value: k})
		binary.Write(hasher, binary.LittleEndian, t)
	}
}

func (env environment) fullmap() string {
	keys := make([]int64, 0)
	for k := range env.bindings {
		keys = append(keys, k)
	}
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

func subgoalHash(literal Literal) uuid.UUID {
	hasher := murmur3.New128()

	writeStructuralTag(hasher, []Literal{literal})

	return idFromInts(hasher.Sum128())
}

func chainHash(ClauseID uuid.UUID, body []Literal, env environment) uuid.UUID {
	hasher := murmur3.New128()
	// ignore error
	hasher.Write(ClauseID.Bytes())
	writeHash(hasher, env)
	writeStructuralTag(hasher, body)

	return idFromInts(hasher.Sum128())
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

// mutates env
func (g *goal) freshenIn(l Literal, env environment) Literal {
	return freshenIn(l, &g.varCount, env)
}

// mutates env
func freshenIn(l Literal, count *int64, env environment) Literal {
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
	resultEnv := environment{bindings: map[int64]Term{}}
	result := Clause{}
	result.Head = freshenIn(c.Head, counter, resultEnv)
	result.Body = make([]Literal, len(c.Body))
	for i, l := range c.Body {
		result.Body[i] = freshenIn(l, counter, resultEnv)
	}
	return result, resultEnv
}

func (g *goal) resultForDependentChain(sg *subgoal, r result, d dependent) result {
	dependentChain := g.chains[d.recieverID]
	// This environment should map from the dependent's variables through to whatever got bound
	denv := emptyEnvironment()

	for k, v := range d.ClauseMapping {
		denv.bindings[k] = r.env.chase(v)
	}

	var newL = denv.rewrite(dependentChain.body[0])

	if !newL.allConstant() {
		// TODO: this still happens?
		panic(fmt.Sprint("Generated a non-constant result for chain", newL, dependentChain.body[0]))
	}

	return result{
		env:     denv,
		Literal: newL,
	}
}

func (g *goal) resultForDependentSubgoal(chain *chain, r result, d dependent) result {
	dependentSubgoal := g.subgoals[d.recieverID]

	// This environment should map from the dependent's variables through to whatever got bound
	denv := emptyEnvironment()
	for k, v := range d.ClauseMapping {
		denv.bind(k, r.env.chase(v))
	}
	var newL = denv.rewrite(dependentSubgoal.Literal)

	if !newL.allConstant() {
		panic(fmt.Sprint("Generated a non-constant result for subgoal", dependentSubgoal.Literal, "->", newL))
	}

	return result{
		env:     denv,
		Literal: newL,
		// Do we even need a proof when passing to chains?
		proof: proof{
			success:       true,
			Clause:        chain.clauseId,
			substitutions: r.env,
		},
	}
}

func (g *goal) mergeResultIntoChain(chain *chain, r result) {
	newEnv := emptyEnvironment()
	// Rewrite a copy of the chain's starting environment
	for k, v := range chain.env.bindings {
		newEnv.bind(k, r.env.chase(v))
	}
	// Merge in the additions from the latest result; this includes
	// variables seen for the first time in this chain's leading literal
	for k, v := range r.env.bindings {
		newEnv.bind(k, v)
	}

	// assert that len(chain.body) != 0 under any circumstances
	if len(chain.body) == 1 {
		r.env = newEnv
		chain.results[r.Literal.id()] = resultNext{r, uuid.Nil}
		// Only pass it to dependents if it matches the polarity of
		// what we're looking for.
		if (!chain.body[0].Negated && !r.isFailure) ||
			(chain.body[0].Negated && r.isFailure) {
			for _, d := range chain.dependents {
				dependentResult := g.resultForDependentSubgoal(chain, r, d)
				g.mergeResultIntoSubgoal(g.subgoals[d.recieverID], dependentResult)
			}
			return
		}
	}

	// TODO: if we held an env and did all of our unification in it, copying it to new
	// subgoals when merging results, we wouldn't need to rewrite and hold on to all of these
	// literals.

	if (!chain.body[0].Negated && !r.isFailure) ||
		(chain.body[0].Negated && r.isFailure) {
		// Create new dependents
		newDependents := make([]dependent, len(chain.dependents))
		for i, d := range chain.dependents {
			newDependent := dependent{d.recieverID, map[int64]Term{}}
			for k, v := range d.ClauseMapping {
				bound := r.env.chase(v)
				newDependent.ClauseMapping[k] = bound
			}
			newDependents[i] = newDependent
		}

		next, isNew := g.chain(chain.clauseId, newEnv, chain.body[1:], newDependents)
		chain.results[r.Literal.id()] = resultNext{result: r, next: next}
		if isNew {
			g.visitChain(next)
		}
	}
}

func (g *goal) mergeResultIntoSubgoal(sg *subgoal, r result) {
	if _, ok := sg.results[r.Literal.id()]; !ok {
		// Store the result so that we can extract results and proof later
		sg.results[r.Literal.id()] = r

		// Trigger dependents
		for _, d := range sg.dependents {
			dresult := g.resultForDependentChain(sg, r, d)
			g.mergeResultIntoChain(g.chains[d.recieverID], dresult)
		}
	}
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

func (l Literal) allConstant() bool {
	for _, v := range l.Terms {
		if !v.IsConstant {
			return false
		}
	}
	return true
}

func (g *goal) visitChain(chainId uuid.UUID) {
	chain := g.chains[chainId]

	cm := map[int64]Term{}
	for _, t := range chain.body[0].Terms {
		if !t.IsConstant {
			cm[t.Value] = t
		}
	}

	subgoalTarget := chain.body[0]
	subgoalTarget.Negated = false
	id, isNew := g.putSubgoal(subgoalTarget, emptyEnvironment(), []dependent{dependent{chainId, cm}})
	if isNew {
		g.visitSubgoal(id)
	}
	// If the leading literal is negated and we get here without accumulating any successful results,
	// then signal
	if chain.body[0].Negated {
		for _, rn := range chain.results {
			if !rn.result.isFailure {
				return
			}
		}
		g.mergeResultIntoChain(chain, result{
			isFailure: true,
		})
	}
}

func (g *goal) visitSubgoal(subgoal uuid.UUID) {
	sg := g.subgoals[subgoal]

	if sg.Literal.Negated {
		panic(fmt.Sprintf("Visiting negated subgoal: %v. All subgoals should be in positive form.", sg.Literal))
	}
	// Check whether or not the database has attempted this subgoal.
	// TODO: we should be able to store failure as well?
	if results, ok := g.db.results[sg.Literal.id()]; ok {
		for _, r := range results {
			g.mergeResultIntoSubgoal(sg, r)
		}
		return
	}

	// Check Clauses
	for cid, c := range g.db.Clauses {
		// If it's a fact
		if len(c.Body) == 0 {
			if ok, env := unify(sg.Literal, c.Head, emptyEnvironment()); ok {
				// Pass it up
				r := result{
					env:     env,
					Literal: env.rewrite(sg.Literal),
					// Literal?
					proof: proof{
						success:       true,
						Clause:        cid,
						substitutions: env,
					},
				}
				g.mergeResultIntoSubgoal(sg, r)
			}
			continue
		}

		// It's a rule

		freshEnv := emptyEnvironment()
		fresh := g.freshenIn(sg.Literal, freshEnv)

		if ok, env := unify(fresh, c.Head, emptyEnvironment()); ok {
			// Some variable bindings may be made in unify against the head -- need to add those
			// to the dependent bindings
			for k, v := range freshEnv.bindings {
				freshEnv.bindings[k] = env.chase(v)
			}

			chainId, isNew := g.chain(cid, env, c.Body, []dependent{dependent{subgoal, freshEnv.bindings}})
			if isNew {
				g.visitChain(chainId)
			}
		}
	}
}

func emptyEnvironment() environment {
	return environment{
		bindings: map[int64]Term{},
	}
}

func (db *database) ask(l Literal) []result {
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

	// TODO lock
	for id, sg := range goal.subgoals {
		if _, ok := db.results[id]; ok {
			// results already exist, continue
			continue
		} else {
			db.results[id] = make([]result, 0)
		}
		for _, rn := range sg.results {
			db.results[id] = append(db.results[id], rn)

			db.proofs[rn.Literal.id()] = append(db.proofs[rn.Literal.id()], rn.proof)
		}
	}

	var results []result
	for _, r := range goal.subgoals[id].results {
		results = append(results, r)
	}

	return results
}

func (db *database) assert(c Clause) {
	fresh, _ := freshen(c, &db.vars)
	id := fresh.id()
	db.Clauses[id] = fresh
}

// l must have been asked directly or returned from a previous ask of the database
func (db *database) ProofOf(l Literal) ([]proof, bool) {
	id := l.id()
	ps, ok := db.proofs[id]
	return ps, ok
}

func (db *database) ProofString(l Literal) string {
	result := bytes.NewBufferString("")

	prooved := map[uuid.UUID]struct{}{}
	toProove := []Literal{l}

	for len(toProove) > 0 {
		l = toProove[0]
		toProove = toProove[1:]
		id := l.id()
		if _, ok := prooved[id]; ok {
			continue
		} else {
			prooved[id] = struct{}{}
		}

		ps, ok := db.ProofOf(l)
		if !ok {
			panic(fmt.Sprint("Something went wrong, could not find proof for", l))
		}
		// Work with the first proof, and only the first proof
		p := ps[0]

		c := db.Clauses[p.Clause]
		substituted := p.substitutions.rewriteClause(c)
		db.writeClause(result, &substituted, Assert)
		if len(substituted.Body) > 0 {
			toProove = append(toProove, substituted.Body...)

		}
	}

	return result.String()
}
