package authalog

import (
	uuid "github.com/satori/go.uuid"
)

type invalidation struct {
	subgoal           Literal
	dependentSubgoals []uuid.UUID
}

type invalidationReport struct {
	countResultsCleared int
}

func (ir invalidationReport) merge(other invalidationReport) invalidationReport {
	ir.countResultsCleared += other.countResultsCleared
	return ir
}

// must be called while holding resultsMutex
func (db *Database) mergeResults(sgl Literal, id uuid.UUID, results map[uuid.UUID]result) {
	if _, ok := db.results[id]; ok {
		// results already exist, continue
		return
	} else {
		db.results[id] = make([]result, 0)
	}
	for _, r := range results {
		db.results[id] = append(db.results[id], r)

		db.proofs[r.Literal.id()] = append(db.proofs[r.Literal.id()], r.proof)
	}
}

func (db *Database) recordInvalidations(subgoal Literal, id uuid.UUID, invalidators map[uuid.UUID]Literal) {
	for i, l := range invalidators {
		if _, ok := db.invalidations[i]; !ok {
			db.invalidations[i] = &invalidation{
				subgoal:           l,
				dependentSubgoals: []uuid.UUID{},
			}
		}
		db.invalidations[i].dependentSubgoals = append(db.invalidations[i].dependentSubgoals, id)
	}
}

func (db *Database) invalidateLiteral(l Literal) invalidationReport {
	ir := invalidationReport{}

	db.resultsMutex.Lock()
	defer db.resultsMutex.Unlock()

	for id, i := range db.invalidations {
		if ok, _ := unify(i.subgoal, l, emptyEnvironment()); ok {
			ir = ir.merge(db.invalidate(id))
		}
	}

	// Clear this literal's results -- it might have no dependents
	if rs, ok := db.results[l.id()]; ok {
		ir.countResultsCleared++
		for _, r := range rs {
			delete(db.proofs, r.Literal.id())
		}
	}
	delete(db.results, l.id())

	return ir
}

func (db *Database) invalidate(subgoalID uuid.UUID) invalidationReport {
	toInvalidate := []uuid.UUID{subgoalID}
	ir := invalidationReport{}
	for {
		if len(toInvalidate) == 0 {
			break
		}
		id := toInvalidate[0]
		toInvalidate = toInvalidate[1:]

		if invalidation, ok := db.invalidations[id]; ok {
			toInvalidate = append(toInvalidate, invalidation.dependentSubgoals...)
		}
		delete(db.invalidations, id)
		if rs, ok := db.results[id]; ok {
			ir.countResultsCleared++
			for _, r := range rs {
				delete(db.proofs, r.Literal.id())
			}
		}
		delete(db.results, id)
	}
	return ir
}
