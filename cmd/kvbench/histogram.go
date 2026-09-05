package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Histogram is a log-linear latency histogram in the spirit of HDR
// Histogram: constant relative error across many orders of magnitude, fixed
// memory, and no reservoir sampling to distort the tail.
//
// Implemented here rather than imported because the project's dependency
// budget is deliberately near zero (DESIGN.md §0) and the whole structure is
// eighty lines.
//
// Layout: values are bucketed by their base-2 magnitude, and each magnitude
// is subdivided into 2^subBits linear sub-buckets. With subBits=6 that is 64
// sub-buckets per octave, giving worst-case relative error of about 1.6% —
// comfortably better than the precision anyone should claim for a p99.
//
// NEVER report only the mean from this. The mean of a latency distribution
// hides exactly the behaviour you care about: a service that is 1ms at p50
// and 4 seconds at p99.9 has a lovely mean.
type Histogram struct {
	subBits  uint
	subCount int64
	counts   []uint64
	total    uint64
	sum      uint64
	min      uint64
	max      uint64
}

const (
	histSubBits = 6
	histBuckets = 64 // covers up to 2^64 ns, far beyond any real latency
)

// NewHistogram builds an empty histogram.
func NewHistogram() *Histogram {
	sub := int64(1) << histSubBits
	return &Histogram{
		subBits:  histSubBits,
		subCount: sub,
		counts:   make([]uint64, histBuckets*sub),
		min:      math.MaxUint64,
	}
}

func (h *Histogram) index(v uint64) int {
	if v < uint64(h.subCount) {
		return int(v)
	}
	// magnitude = position of the highest set bit
	mag := 63 - leadingZeros64(v)
	shift := uint(mag) - h.subBits
	sub := (v >> shift) & uint64(h.subCount-1)
	idx := int((uint64(shift)+1)*uint64(h.subCount) + sub)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	return idx
}

// value returns the representative value for a bucket index.
func (h *Histogram) value(idx int) uint64 {
	if idx < int(h.subCount) {
		return uint64(idx)
	}
	shift := uint64(idx)/uint64(h.subCount) - 1
	sub := uint64(idx) % uint64(h.subCount)
	return (uint64(h.subCount) | sub) << shift
}

// Record adds one observation, in nanoseconds.
func (h *Histogram) Record(d time.Duration) {
	v := uint64(d)
	if d < 0 {
		v = 0
	}
	h.counts[h.index(v)]++
	h.total++
	h.sum += v
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
}

// Merge folds another histogram in. Per-worker histograms are merged at the
// end so the hot path never touches shared state — a mutex around Record
// would make the benchmark measure its own contention.
func (h *Histogram) Merge(o *Histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
	h.sum += o.sum
	if o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.total }

// Quantile returns the latency at the given quantile (0..1).
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(h.total)))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			return time.Duration(h.value(i))
		}
	}
	return time.Duration(h.max)
}

// Mean returns the arithmetic mean. Present for completeness; report it
// alongside quantiles, never instead of them.
func (h *Histogram) Mean() time.Duration {
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.sum / h.total)
}

// Min returns the smallest observation.
func (h *Histogram) Min() time.Duration {
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.min)
}

// Max returns the largest observation.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max) }

// Summary renders the distribution as one line.
func (h *Histogram) Summary() string {
	if h.total == 0 {
		return "no samples"
	}
	return fmt.Sprintf("p50=%s p90=%s p99=%s p99.9=%s max=%s (mean=%s, n=%d)",
		fmtDur(h.Quantile(0.50)), fmtDur(h.Quantile(0.90)),
		fmtDur(h.Quantile(0.99)), fmtDur(h.Quantile(0.999)),
		fmtDur(h.Max()), fmtDur(h.Mean()), h.total)
}

// Percentiles returns the standard set as a map, for CSV output.
func (h *Histogram) Percentiles() map[string]time.Duration {
	return map[string]time.Duration{
		"min":   h.Min(),
		"p50":   h.Quantile(0.50),
		"p90":   h.Quantile(0.90),
		"p99":   h.Quantile(0.99),
		"p99.9": h.Quantile(0.999),
		"max":   h.Max(),
		"mean":  h.Mean(),
	}
}

// Bars renders an ASCII distribution, useful for spotting bimodality that
// percentiles alone hide.
func (h *Histogram) Bars(width int) string {
	if h.total == 0 {
		return ""
	}
	type bucket struct {
		v uint64
		c uint64
	}
	var bs []bucket
	var peak uint64
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		bs = append(bs, bucket{h.value(i), c})
		if c > peak {
			peak = c
		}
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].v < bs[j].v })
	// Collapse to at most 20 rows so the output stays readable.
	step := 1
	if len(bs) > 20 {
		step = len(bs) / 20
	}
	var sb strings.Builder
	for i := 0; i < len(bs); i += step {
		var c uint64
		for j := i; j < i+step && j < len(bs); j++ {
			c += bs[j].c
		}
		n := int(float64(c) / float64(peak) * float64(width))
		fmt.Fprintf(&sb, "  %10s |%-*s %d\n", fmtDur(time.Duration(bs[i].v)), width, strings.Repeat("#", n), c)
	}
	return sb.String()
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func leadingZeros64(x uint64) uint {
	n := uint(0)
	if x == 0 {
		return 64
	}
	for x&(1<<63) == 0 {
		x <<= 1
		n++
	}
	return n
}
