package authalog

import (
	"sync"
	"time"
)

// TTL Invalidator

type ttlAlive struct {
	l     Literal
	alive int64 // Unix Nanoseconds
}

type TTLInvalidator struct {
	db         *Database
	requests   chan ttlAlive
	buffer     []ttlAlive
	bufferLock sync.Mutex
	timeout    int64
	cycle      int64
	next       ttlAlive
}

func NewTTLInvalidator(db *Database, timeout time.Duration, cycle time.Duration) *TTLInvalidator {
	ttl := TTLInvalidator{
		db:       db,
		requests: make(chan ttlAlive, 1000),
		timeout:  int64(timeout),
		cycle:    int64(cycle),
	}
	return &ttl
}

func (ttl *TTLInvalidator) Start() {
	l := func() {
		for {
			if ttl.next.alive != 0 {
				if time.Now().UnixNano() > ttl.next.alive+ttl.timeout {
					trace("invalidating next")
					ttl.db.invalidateLiteral(ttl.next.l)
				} else {
					goto sleep
				}
			}

			for {
				ttl.next = <-ttl.requests
				trace("Got", ttl.next)
				if time.Now().UnixNano() > ttl.next.alive+ttl.timeout {
					ttl.db.invalidateLiteral(ttl.next.l)
				} else {
					goto sleep
				}
			}
		sleep:
			time.Sleep(time.Duration(ttl.cycle))
		}
	}

	go l()
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
