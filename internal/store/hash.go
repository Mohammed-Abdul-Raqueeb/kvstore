package store

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
)

// Seeded 64-bit hash, used for shard routing.
//
// Why seeded (DESIGN.md §6, mistake #16): an unseeded, publicly known hash
// lets anyone — or an unlucky natural workload — choose keys that all land
// in one shard. That collapses an N-lock design back to a single lock, which
// is a denial-of-service against your own concurrency. The seed is generated
// at startup from crypto/rand, so an attacker cannot precompute collisions.
//
// This is xxHash64, implemented here rather than pulled in as a dependency:
// the project's dependency budget is "net, sync, and a CRC library"
// (DESIGN.md §0), and a 60-line hash is not worth spending it on. It is a
// non-cryptographic hash, which is the right choice — we need avalanche and
// speed, not collision resistance against an adversary who can see the seed.

const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

// NewSeed returns a cryptographically random hash seed.
func NewSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not a recoverable condition, but the store
		// must still start. Fall back to a fixed seed and let the caller's
		// log record that hash flooding protection is degraded.
		return prime1
	}
	seed := binary.LittleEndian.Uint64(b[:])
	if seed == 0 {
		seed = prime1
	}
	return seed
}

// Hash64 computes the seeded 64-bit hash of b.
func Hash64(seed uint64, b []byte) uint64 {
	n := len(b)
	var h uint64

	if n >= 32 {
		v1 := seed + prime1 + prime2
		v2 := seed + prime2
		v3 := seed
		v4 := seed - prime1
		for len(b) >= 32 {
			v1 = round(v1, binary.LittleEndian.Uint64(b[0:8]))
			v2 = round(v2, binary.LittleEndian.Uint64(b[8:16]))
			v3 = round(v3, binary.LittleEndian.Uint64(b[16:24]))
			v4 = round(v4, binary.LittleEndian.Uint64(b[24:32]))
			b = b[32:]
		}
		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = mergeRound(h, v1)
		h = mergeRound(h, v2)
		h = mergeRound(h, v3)
		h = mergeRound(h, v4)
	} else {
		h = seed + prime5
	}

	h += uint64(n)

	for len(b) >= 8 {
		k := round(0, binary.LittleEndian.Uint64(b[:8]))
		h ^= k
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b[:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}

func round(acc, input uint64) uint64 {
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= prime1
	return acc
}

func mergeRound(acc, val uint64) uint64 {
	val = round(0, val)
	acc ^= val
	acc = acc*prime1 + prime4
	return acc
}

// splitmix64 is a fast, high-quality PRNG used by the eviction sampler and
// the expiry sweeper. Each shard owns its own instance, so no shared state
// and no lock is needed for random sampling — using math/rand's global
// source here would put a mutex in the hot path of every eviction.
type splitmix64 struct{ state uint64 }

func (s *splitmix64) next() uint64 {
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// intn returns a value in [0, n). n must be positive.
func (s *splitmix64) intn(n int) int {
	if n <= 1 {
		return 0
	}
	return int(s.next() % uint64(n))
}
