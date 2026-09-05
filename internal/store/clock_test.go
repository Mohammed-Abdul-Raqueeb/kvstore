package store

import "sync/atomic"

// fakeClock lets TTL and eviction tests advance time without sleeping, which
// is what makes those tests fast and, more importantly, deterministic.
type fakeClock struct {
	wall atomic.Uint64
	mono atomic.Uint64
}

func newFakeClock(startWallMs uint64) *fakeClock {
	c := &fakeClock{}
	c.wall.Store(startWallMs)
	return c
}

func (c *fakeClock) NowMillis() uint64  { return c.wall.Load() }
func (c *fakeClock) MonoMillis() uint64 { return c.mono.Load() }

// advance moves both clocks forward by ms.
func (c *fakeClock) advance(ms uint64) {
	c.wall.Add(ms)
	c.mono.Add(ms)
}

// advanceMonoOnly moves only the monotonic clock, used to age entries for
// LRU tests without expiring anything.
func (c *fakeClock) advanceMonoOnly(ms uint64) { c.mono.Add(ms) }
