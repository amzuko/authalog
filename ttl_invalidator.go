package authalog

import (
	"sync"
	"time"
)

type ttlAlive struct {
	l     Literal
	alive int64 // Unix Nanoseconds
}

// TTL Invalidator
type TTLInvalidator struct {
	db         *Database
	requests   chan ttlAlive
	buffer     []ttlAlive
	bufferLock sync.Mutex
	timeout    int64
	cycle      int64
}

func NewTTLInvalidator(db *Database, timeout time.Duration, cycle time.Duration) *TTLInvalidator {
	ttl := TTLInvalidator{
		db: db,
		// Hard code a reasonable buffer size.
		// If we make more external relation calls that must be invalidated than the length
		// of this buffer in less than the invalidation loop's cycle time, we will start to block
		// goroutines making invalidation requests
		requests: make(chan ttlAlive, 1000),
		timeout:  int64(timeout),
		cycle:    int64(cycle),
	}
	return &ttl
}

func (ttl *TTLInvalidator) Start() {
	// TODO: isn't there a way to do all of this on a single goroutine?
	// It feels weird to need two when we're already using a channel for
	// communication with a potentially larger number of goroutines.
	dequeue := func() {
		for {
			a := <-ttl.requests
			ttl.bufferLock.Lock()
			ttl.buffer = append(ttl.buffer, a)
			ttl.bufferLock.Unlock()
		}
	}

	go dequeue()

	invalidate := func() {
		for {
			ttl.bufferLock.Lock()
			count := 0
			for count < len(ttl.buffer) {
				if time.Now().UnixNano() > ttl.buffer[count].alive+ttl.timeout {
					count++
				} else {
					break
				}
			}
			var toInvalidate []ttlAlive
			if count > 0 {
				toInvalidate = ttl.buffer[0:count]
				ttl.buffer = ttl.buffer[count:]
			}
			ttl.bufferLock.Unlock()
			for _, a := range toInvalidate {
				ttl.db.invalidateLiteral(a.l)
			}
			time.Sleep(time.Duration(ttl.cycle))
		}
	}
	go invalidate()

}

func (ttl *TTLInvalidator) InvalidatingRelation(er ExternalRelation) ExternalRelation {
	new := ExternalRelation{
		head: er.head,
	}
	new.run = func(i interner, terms []Term) ([][]Term, error) {
		r, err := er.run(i, terms)
		// TODO: do we want to store an invalidation on error?
		ttl.requests <- ttlAlive{
			l:     Literal{Predicate: new.head.Predicate, Terms: terms},
			alive: time.Now().UnixNano(),
		}
		return r, err
	}
	return new
}
