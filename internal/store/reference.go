package store

import "sync"

// Reference is a deliberately naive key-value store: one map, one mutex, no
// shards, no WAL, no eviction sampling, no slots, no wheel.
//
// Its only job is to be obviously correct so the real store can be checked
// against it (DESIGN.md §11, "Model-based / differential"). Every change to
// the real store's semantics has to be mirrored here, and that friction is a
// feature: if a behaviour is hard to state in twenty lines, it is probably
// not a behaviour worth having.
//
// It is NOT used in the server. It is compiled into the binary only because
// Go has no test-only package concept, and it costs a few hundred bytes.
type Reference struct {
	mu    sync.Mutex
	data  map[string]refEntry
	clock Clock
}

type refEntry struct {
	value    []byte
	expireAt uint64
}

// NewReference builds a reference store sharing the given clock.
func NewReference(clock Clock) *Reference {
	if clock == nil {
		clock = NewRealClock()
	}
	return &Reference{data: make(map[string]refEntry), clock: clock}
}

func (r *Reference) live(k string, nowMs uint64) (refEntry, bool) {
	e, ok := r.data[k]
	if !ok {
		return refEntry{}, false
	}
	if e.expireAt != 0 && e.expireAt <= nowMs {
		delete(r.data, k)
		return refEntry{}, false
	}
	return e, true
}

// Get returns a copy of the value.
func (r *Reference) Get(key []byte) ([]byte, bool) {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live(string(key), now)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), e.value...), true
}

// Set stores key=value with an absolute deadline.
func (r *Reference) Set(key, value []byte, expireAt uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[string(key)] = refEntry{value: append([]byte(nil), value...), expireAt: expireAt}
}

// Delete removes a key, reporting whether a live key was present.
func (r *Reference) Delete(key []byte) bool {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	k := string(key)
	_, ok := r.live(k, now)
	delete(r.data, k)
	return ok
}

// Exists reports whether a live key is present.
func (r *Reference) Exists(key []byte) bool {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.live(string(key), now)
	return ok
}

// Expire sets an absolute deadline on an existing live key.
func (r *Reference) Expire(key []byte, expireAt uint64) bool {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	k := string(key)
	e, ok := r.live(k, now)
	if !ok {
		return false
	}
	e.expireAt = expireAt
	r.data[k] = e
	return true
}

// TTL mirrors Store.TTL.
func (r *Reference) TTL(key []byte) (int64, bool) {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live(string(key), now)
	if !ok {
		return 0, false
	}
	if e.expireAt == 0 {
		return -1, true
	}
	ms := int64(e.expireAt) - int64(now)
	if ms < 0 {
		ms = 0
	}
	return ms, true
}

// Len returns the number of live keys, reaping expired ones on the way.
func (r *Reference) Len() int {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.data {
		r.live(k, now)
	}
	return len(r.data)
}

// Flush empties the store.
func (r *Reference) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = make(map[string]refEntry)
}

// Keys returns every live key (unordered).
func (r *Reference) Keys() [][]byte {
	now := r.clock.NowMillis()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, 0, len(r.data))
	for k := range r.data {
		if _, ok := r.live(k, now); ok {
			out = append(out, []byte(k))
		}
	}
	return out
}
