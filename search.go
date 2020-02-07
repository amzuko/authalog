package authalog

import (
	"encoding/binary"
	"fmt"
	"sort"

	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
)

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
