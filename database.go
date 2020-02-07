package authalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
)

type Database struct {
	clauseMutex sync.RWMutex
	// Map id to Clause
	clauses map[uuid.UUID]Clause
	// Unindexed external rules
	externalRelations []ExternalRelation

	resultsMutex sync.RWMutex
	// Map Literal id to proof
	// TODO: consider not storing this for all derived facts?
	proofs map[uuid.UUID][]proof
	// Map subgoal hash to results
	// TODO: we only really need the literal for future things
	results map[uuid.UUID][]result
	// Map subgoal hash to other subgoal hashes that depend on it
	invalidations map[uuid.UUID]*invalidation

	internMutex sync.RWMutex
	// Used to freshen all stored clauses, so that there are no name collisions between scopes
	vars int64
	// maps variables and string constants to ints
	interned       map[string]int64
	internedLookup map[int64]string
}

func NewDatabase() *Database {
	d := Database{
		clauses:           map[uuid.UUID]Clause{},
		externalRelations: []ExternalRelation{},
		invalidations:     map[uuid.UUID]*invalidation{},
		proofs:            map[uuid.UUID][]proof{},
		results:           map[uuid.UUID][]result{},
		vars:              0,
		interned:          map[string]int64{},
		internedLookup:    map[int64]string{},
	}
	return &d
}

func (db *Database) AddExternalRelations(er ...ExternalRelation) {
	db.externalRelations = append(db.externalRelations, er...)
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
	isFailure bool
	env       environment
	Literal   Literal
	proof     proof
	// subgoal ids to literals
	// TODO: do we need all this? If we just store the uuid's, we can retrieve that literals from the other subgoals in the
	// goal structure.
	invalidators map[uuid.UUID]Literal
}

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

func (e *environment) has(k int64) bool {
	for i := 0; i < e.count; i++ {
		b := e.bindings[i]
		if b.k == k {
			return true
		}
	}
	for _, b := range e.extension {
		if b.k == k {
			return true
		}
	}
	return false
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

func (e *environment) getValue(key int64) (Term, bool) {
	for i := 0; i < e.count; i++ {
		b := e.bindings[i]
		if b.k == key {
			return b.v, true
		}
	}
	for _, b := range e.extension {
		if b.k == key {
			return b.v, true
		}
	}
	return Term{}, false
}

func (e *environment) bindUnsafe(id int64, t Term) {

	if e.count < ENV_FIXED_LENGTH {
		e.bindings[e.count] = binding{id, t}
	} else {
		e.extension = append(e.extension, binding{id, t})
	}
	e.count++
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

type goal struct {
	db       *Database
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

	for ak, av := range a.ClauseMapping {
		if bv, ok := b.ClauseMapping[ak]; !ok {
			return false
		} else {
			if bv != av {
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

func (g *goal) chain(clauseID uuid.UUID, env environment, body []Literal, additionalDependents []dependent, invalidators map[uuid.UUID]Literal) (uuid.UUID, bool) {
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
			alwaysTrue := unify(c.body[i], l, &rewriteEnv)
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

		// Merge invalidators
		for k, v := range invalidators {
			c.invalidators[k] = v
		}

		g.addDependentToChain(c, newDependents)
	} else {
		isNew = true
		c := chain{
			clauseId:     clauseID,
			body:         newBody,
			env:          env,
			results:      map[uuid.UUID]resultNext{},
			dependents:   additionalDependents,
			invalidators: invalidators,
		}
		// Make sure we store positive forms only in validators
		i := newBody[0]
		i.Negated = false
		c.invalidators[newBody[0].id()] = i
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
		match := emptyEnvironment()
		alwaysTrue := unify(sg.Literal, l, &match)
		if !alwaysTrue {
			panic("Something went very wrong")
		}

		newDependents := make([]dependent, len(additionalDependents))
		for i, d := range additionalDependents {
			newd := dependent{d.recieverID, map[int64]Term{}}
			for k, v := range d.ClauseMapping {
				newd.ClauseMapping[k] = match.chase(v)
			}
			newDependents[i] = newd
			// I think that these d.ClauseMapping is always empty, but thats not right
			// TODO can we always completely construct the cluasemapping from the env??
			// Or are there cases wherein we need to pull information from the original dependent's clausempapping? is it always empty?

		}
		g.addDependent(sg, newDependents)
	} else {
		isNew = true
		g.subgoals[id] = &subgoal{
			Literal:      l,
			results:      map[uuid.UUID]result{},
			dependents:   additionalDependents,
			invalidators: map[uuid.UUID]Literal{},
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

	// Invalidators accumulates subgoals that would invalidate this one
	invalidators map[uuid.UUID]Literal
}

type subgoal struct {
	// The literal that we are trying to proove.
	Literal Literal
	// Store the results for this subgoal. This stores results for the _positive_ form of
	// the subgoal's literal. If the literal is negated
	// TODO: consider a new structure that would allow
	results map[uuid.UUID]result
	// The dependents of this subgoal (chains that depend on it)
	dependents []dependent

	// Invalidators accumulates the subgoals that would invalidate this one
	invalidators map[uuid.UUID]Literal
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
	for i := 0; i < env.count && i < ENV_FIXED_LENGTH; i++ {
		keys = append(keys, env.bindings[i].k)
	}
	for _, b := range env.extension {
		keys = append(keys, b.k)
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

func (g *goal) resultForDependentChain(sg *subgoal, r result, d dependent) result {
	dependentChain := g.chains[d.recieverID]
	// This environment should map from the dependent's variables through to whatever got bound
	denv := emptyEnvironment()
	for k, v := range d.ClauseMapping {
		denv.bind(k, r.env.chase(v))
	}

	var newL = denv.rewrite(dependentChain.body[0])

	if !newL.allConstant() {
		panic(fmt.Sprint("Generated a non-constant result for chain", newL, dependentChain.body[0]))
	}

	return result{
		env:          denv,
		Literal:      newL,
		invalidators: map[uuid.UUID]Literal{sg.Literal.id(): sg.Literal},
	}
}

func (g *goal) resultForDependentSubgoal(chain *chain, r result, d dependent) result {
	dependentSubgoal := g.subgoals[d.recieverID]

	newInvalidators := map[uuid.UUID]Literal{}
	for k, v := range chain.invalidators {
		newInvalidators[k] = v
	}
	for k, v := range r.invalidators {
		newInvalidators[k] = v
	}

	failure := (r.isFailure && !chain.body[0].Negated) || (!r.isFailure && chain.body[0].Negated)

	if failure {
		return result{
			isFailure:    true,
			invalidators: newInvalidators,
		}
	}

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
		isFailure: false,
		env:       denv,
		Literal:   newL,
		// Do we even need a proof when passing to chains?
		proof: proof{
			success:       true,
			Clause:        chain.clauseId,
			substitutions: r.env,
		},
		invalidators: newInvalidators,
	}
}

func (g *goal) mergeResultIntoChain(chain *chain, r result) {
	// Rewrite a copy of the chain's starting environment
	newEnv := rewritten(chain.env, r.env)

	// Merge in the additions from the latest result; this includes
	// variables seen for the first time in this chain's leading literal
	r.env.forEach(func(k int64, v Term) {
		newEnv.bind(k, v)
	})

	// assert that len(chain.body) != 0 under any circumstances
	if len(chain.body) == 1 {
		r.env = newEnv
		chain.results[r.Literal.id()] = resultNext{r, uuid.Nil}
		for _, d := range chain.dependents {
			dependentResult := g.resultForDependentSubgoal(chain, r, d)
			g.mergeResultIntoSubgoal(g.subgoals[d.recieverID], dependentResult)
		}
		return
	}

	if (!chain.body[0].Negated && !r.isFailure) ||
		(chain.body[0].Negated && r.isFailure) {
		// Create new dependents
		newDependents := make([]dependent, len(chain.dependents))
		for i, d := range chain.dependents {
			newDependent := dependent{d.recieverID, map[int64]Term{}}
			for k, v := range d.ClauseMapping {
				newDependent.ClauseMapping[k] = r.env.chase(v)
			}
			newDependents[i] = newDependent
		}

		next, isNew := g.chain(chain.clauseId, newEnv, chain.body[1:], newDependents, chain.invalidators)
		chain.results[r.Literal.id()] = resultNext{result: r, next: next}
		if isNew {
			g.visitChain(next)
		}
	} else {
		// Failure -- fire the dependents
		for _, d := range chain.dependents {
			sg := g.subgoals[d.recieverID]
			g.mergeResultIntoSubgoal(sg, result{
				isFailure:    true,
				invalidators: r.invalidators,
			})
		}
	}
}

func (g *goal) mergeResultIntoSubgoal(sg *subgoal, r result) {
	trace("Merging", r.Literal, "into", sg.Literal)

	for k, v := range r.invalidators {
		sg.invalidators[k] = v
	}
	if r.isFailure {
		return
	}

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

	// This violates an invariant of environments that is enforced when bind() is called --
	// that we not map variables to themselves
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
	for _, rn := range chain.results {
		if !rn.result.isFailure {
			return
		}
	}
	g.mergeResultIntoChain(chain, result{
		isFailure: true,
	})
}

// Intermediate types used to group data
type fact struct {
	cid uuid.UUID
	env environment
}

type rule struct {
	cid      uuid.UUID
	c        Clause
	env      environment
	freshEnv environment
}

func (g *goal) visitSubgoal(subgoal uuid.UUID) {
	sg := g.subgoals[subgoal]
	trace("visiting", sg.Literal)
	if sg.Literal.Negated {
		panic(fmt.Sprintf("Visiting negated subgoal: %v. All subgoals should be in positive form.", sg.Literal))
	}
	// Check whether or not the database has attempted this subgoal.
	// TODO: we should be able to store failure as well?
	g.db.resultsMutex.RLock()
	results, ok := g.db.results[sg.Literal.id()]
	g.db.resultsMutex.RUnlock()

	if ok {
		trace("Found results")
		for _, r := range results {
			g.mergeResultIntoSubgoal(sg, r)
		}
		return
	}

	// Check external relations

	external := []ExternalRelation{}
	g.db.clauseMutex.RLock()
	for _, r := range g.db.externalRelations {
		match := emptyEnvironment()
		if ok := unify(sg.Literal, r.head, &match); ok {
			external = append(external, r)
		}
	}
	g.db.clauseMutex.RUnlock()

	for _, r := range external {
		trace("Matched external relation", r.head)
		g.runExternalRule(sg, r)
	}

	facts := []fact{}
	rules := []rule{}
	// Check Clauses
	g.db.clauseMutex.RLock()
	match := emptyEnvironment()
	for cid, c := range g.db.clauses {
		match.reset()
		// If it's a fact
		if len(c.Body) == 0 {
			if ok := unify(sg.Literal, c.Head, &match); ok {
				facts = append(facts, fact{cid, match})
			}
			continue
		}

		// It's a rule
		// We need to freshen the subgoal literal because there might be variable name
		// colisions between clauses, most directly when a clause recurses with itself.
		freshEnv := emptyEnvironment()
		fresh := g.freshenIn(sg.Literal, &freshEnv)

		if ok := unify(fresh, c.Head, &match); ok {
			// Some variable bindings may be made in unify against the head -- need to add those
			// to the dependent bindings
			freshEnv = rewritten(freshEnv, match)

			rules = append(rules, rule{
				cid:      cid,
				c:        c,
				env:      match,
				freshEnv: freshEnv,
			})
		}
	}
	g.db.clauseMutex.RUnlock()

	for _, f := range facts {
		// Pass it up
		r := result{
			env:     f.env,
			Literal: f.env.rewrite(sg.Literal),
			// Literal?
			proof: proof{
				success:       true,
				Clause:        f.cid,
				substitutions: f.env,
			},
		}
		g.mergeResultIntoSubgoal(sg, r)
	}

	for _, r := range rules {
		cm := map[int64]Term{}
		r.freshEnv.forEach(func(k int64, v Term) { cm[k] = v })

		chainId, isNew := g.chain(
			r.cid,
			r.env,
			r.c.Body,
			[]dependent{dependent{subgoal, cm}},
			map[uuid.UUID]Literal{})
		if isNew {
			g.visitChain(chainId)
		}
	}

}

func (db *Database) ask(l Literal) []result {
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

	db.resultsMutex.Lock()
	for id, sg := range goal.subgoals {
		trace("merging", id, sg.Literal.id(), sg.Literal)
		db.mergeResults(sg.Literal, id, sg.results)
		db.recordInvalidations(sg.Literal, id, sg.invalidators)
	}
	db.resultsMutex.Unlock()

	var results []result
	for _, r := range goal.subgoals[id].results {
		results = append(results, r)
	}

	return results
}

func (db *Database) assert(c Clause) {
	db.internMutex.Lock()
	db.clauseMutex.Lock()
	defer db.internMutex.Unlock()
	defer db.clauseMutex.Unlock()
	fresh, _ := freshen(c, &db.vars)
	id := fresh.id()
	db.clauses[id] = fresh
}
