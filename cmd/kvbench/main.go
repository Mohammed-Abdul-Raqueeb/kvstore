package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/protocol"
)

var Version = "dev"

// kvbench is the load generator (DESIGN.md §12).
//
// The single most important thing here is OPEN-LOOP mode, and the reason is
// coordinated omission.
//
// A closed-loop benchmark has N connections, each sending its next request
// only after the previous response arrives. When the server stalls for 500ms,
// every client politely stops sending. That stall is recorded as ONE slow
// request instead of the hundreds of requests that would have been issued
// during it — so the stall is almost entirely absent from the histogram and
// the p99 comes out beautiful and completely fictional.
//
// Open-loop issues requests on a schedule set in advance, at a fixed rate,
// regardless of whether earlier ones came back. Latency is measured from the
// time a request was DUE, not from when it was sent. A 500ms stall then
// shows up in every request that was due during it, which is what a user
// behind a queue actually experiences.
//
// Report both. Closed-loop tells you maximum throughput; open-loop tells you
// what latency looks like at a given load.

type benchConfig struct {
	addr         string
	mode         string
	workload     string
	distribution string
	conns        int
	pipeline     int
	valueSize    int
	keyspace     int
	duration     time.Duration
	warmup       time.Duration
	rate         int
	zipfS        float64
	csvPath      string
	histogram    bool
	runs         int
	label        string
	preload      bool
}

func main() {
	var c benchConfig
	fs := flag.NewFlagSet("kvbench", flag.ExitOnError)
	fs.StringVar(&c.addr, "addr", "127.0.0.1:7379", "server address")
	fs.StringVar(&c.mode, "mode", "closed", "load mode: closed|open")
	fs.StringVar(&c.workload, "workload", "50/50", "get/set mix: get|set|95/5|50/50")
	fs.StringVar(&c.distribution, "dist", "uniform", "key distribution: uniform|zipfian")
	fs.IntVar(&c.conns, "conns", 8, "concurrent connections")
	fs.IntVar(&c.pipeline, "pipeline", 1, "requests in flight per connection")
	fs.IntVar(&c.valueSize, "value-size", 64, "value size in bytes")
	fs.IntVar(&c.keyspace, "keyspace", 100000, "number of distinct keys")
	fs.DurationVar(&c.duration, "duration", 10*time.Second, "measurement duration")
	fs.DurationVar(&c.warmup, "warmup", 2*time.Second, "warmup duration (not measured)")
	fs.IntVar(&c.rate, "rate", 10000, "target requests/sec for --mode=open")
	fs.Float64Var(&c.zipfS, "zipf-s", 0.99, "Zipfian skew parameter")
	fs.StringVar(&c.csvPath, "csv", "", "append results to this CSV file")
	fs.BoolVar(&c.histogram, "hist", false, "print an ASCII latency distribution")
	fs.IntVar(&c.runs, "runs", 1, "repeat the measurement this many times and report the median")
	fs.StringVar(&c.label, "label", "", "label recorded in the CSV row")
	fs.BoolVar(&c.preload, "preload", true, "populate the keyspace before measuring")
	showVersion := fs.Bool("version", false, "print version and exit")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `kvbench %s — load generator for kvstore

Examples:
  # Maximum throughput, 64 connections, 95%% reads
  kvbench --mode closed --conns 64 --workload 95/5

  # Latency at a fixed 50k req/s, immune to coordinated omission
  kvbench --mode open --rate 50000 --conns 64

  # Skewed keys, which is where sharding stops helping
  kvbench --dist zipfian --zipf-s 0.99 --conns 64

  # Sweep a dimension into a CSV for plotting
  for n in 1 8 64 256; do kvbench --conns $n --csv results.csv --label "conns=$n"; done

Flags:
`, Version)
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println(Version)
		return
	}

	if err := runBench(c); err != nil {
		fmt.Fprintf(os.Stderr, "kvbench: %v\n", err)
		os.Exit(1)
	}
}

type result struct {
	hist       *Histogram
	ops        uint64
	errors     uint64
	notFound   uint64
	elapsed    time.Duration
	throughput float64
	// omitted counts requests that were issued late in open-loop mode: the
	// generator could not keep up with its own schedule. A non-zero value
	// means the CLIENT is the bottleneck and the numbers understate the
	// server's problems.
	omitted uint64
}

func runBench(c benchConfig) error {
	getRatio, err := parseWorkload(c.workload)
	if err != nil {
		return err
	}

	fmt.Printf("kvbench %s\n", Version)
	fmt.Printf("  target      %s\n", c.addr)
	fmt.Printf("  mode        %s\n", c.mode)
	fmt.Printf("  workload    %s (%.0f%% GET)\n", c.workload, getRatio*100)
	fmt.Printf("  keys        %d, %s\n", c.keyspace, c.distribution)
	fmt.Printf("  value size  %d B\n", c.valueSize)
	fmt.Printf("  conns       %d (pipeline depth %d)\n", c.conns, c.pipeline)
	if c.mode == "open" {
		fmt.Printf("  rate        %d req/s\n", c.rate)
	}
	fmt.Printf("  duration    %s (after %s warmup), %d run(s)\n", c.duration, c.warmup, c.runs)
	fmt.Printf("  client host %s/%s, %d CPUs\n\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())

	// Measure the loopback ceiling first. Without it there is no way to know
	// whether a number is the server's limit or the kernel's.
	if ceiling, err := measurePingCeiling(c.addr); err == nil {
		fmt.Printf("loopback round-trip floor (PING): %s\n\n", ceiling.Summary())
	}

	if c.preload {
		fmt.Printf("preloading %d keys... ", c.keyspace)
		start := time.Now()
		if err := preload(c); err != nil {
			return fmt.Errorf("preload: %w", err)
		}
		fmt.Printf("done in %s\n\n", time.Since(start).Round(time.Millisecond))
	}

	var results []result
	for run := 0; run < c.runs; run++ {
		if c.warmup > 0 {
			warm := c
			warm.duration = c.warmup
			if _, err := measure(warm, getRatio, true); err != nil {
				return err
			}
		}
		r, err := measure(c, getRatio, false)
		if err != nil {
			return err
		}
		results = append(results, r)
		fmt.Printf("run %d/%d  %.0f ops/s  %s\n", run+1, c.runs, r.throughput, r.hist.Summary())
	}

	med := medianResult(results)
	fmt.Printf("\n=== result (median of %d run(s)) ===\n", c.runs)
	fmt.Printf("throughput   %.0f ops/sec\n", med.throughput)
	fmt.Printf("operations   %d (%d errors, %d not-found)\n", med.ops, med.errors, med.notFound)
	fmt.Printf("latency      %s\n", med.hist.Summary())
	if med.omitted > 0 {
		fmt.Printf("\nWARNING: %d requests were issued behind schedule. The load generator\n", med.omitted)
		fmt.Printf("could not sustain %d req/s, so these latencies understate the truth.\n", c.rate)
		fmt.Printf("Lower --rate or add client machines before believing this run.\n")
	}
	if c.runs > 1 {
		lo, hi := spread(results)
		fmt.Printf("spread       %.0f .. %.0f ops/sec across runs\n", lo, hi)
	}
	if c.histogram {
		fmt.Printf("\nlatency distribution:\n%s", med.hist.Bars(50))
	}
	if c.mode == "closed" {
		fmt.Printf("\nNote: closed-loop numbers suffer from coordinated omission. The p99 above\n")
		fmt.Printf("is the p99 of requests this client chose to send, which excludes the ones\n")
		fmt.Printf("it did not send because the server was stalled. Use --mode open for\n")
		fmt.Printf("latency claims.\n")
	}

	if c.csvPath != "" {
		if err := appendCSV(c, med); err != nil {
			return fmt.Errorf("write csv: %w", err)
		}
		fmt.Printf("\nappended a row to %s\n", c.csvPath)
	}
	return nil
}

func parseWorkload(w string) (float64, error) {
	switch strings.ToLower(w) {
	case "get", "read":
		return 1.0, nil
	case "set", "write":
		return 0.0, nil
	}
	parts := strings.Split(w, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("bad workload %q (want get, set, or a mix like 95/5)", w)
	}
	g, err1 := strconv.Atoi(parts[0])
	s, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || g+s == 0 {
		return 0, fmt.Errorf("bad workload %q", w)
	}
	return float64(g) / float64(g+s), nil
}

func measurePingCeiling(addr string) (*Histogram, error) {
	c, err := client.Dial(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	h := NewHistogram()
	for i := 0; i < 2000; i++ {
		start := time.Now()
		if err := c.Ping(); err != nil {
			return nil, err
		}
		h.Record(time.Since(start))
	}
	return h, nil
}

func preload(c benchConfig) error {
	conns := c.conns
	if conns > 16 {
		conns = 16
	}
	value := make([]byte, c.valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, conns)
	per := c.keyspace / conns

	for w := 0; w < conns; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl, err := client.Dial(c.addr)
			if err != nil {
				errCh <- err
				return
			}
			defer cl.Close()
			start := w * per
			end := start + per
			if w == conns-1 {
				end = c.keyspace
			}
			// Pipeline the preload; doing it one round trip at a time makes
			// a million-key preload take minutes for no reason.
			const batch = 500
			for i := start; i < end; i += batch {
				p := cl.Pipeline()
				for j := i; j < i+batch && j < end; j++ {
					p.Add(protocol.OpSet, protocol.Command{Key: keyFor(j), Value: value})
				}
				if _, err := p.Run(); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func keyFor(i int) []byte {
	return []byte(fmt.Sprintf("bench:key:%09d", i))
}

// keyGen produces key indices under the configured distribution.
type keyGen struct {
	rng      *rand.Rand
	keyspace int
	zipf     *rand.Zipf
}

func newKeyGen(c benchConfig, seed int64) *keyGen {
	rng := rand.New(rand.NewSource(seed))
	g := &keyGen{rng: rng, keyspace: c.keyspace}
	if c.distribution == "zipfian" {
		// A Zipfian distribution concentrates most traffic on a few keys,
		// which is what real workloads look like and what makes sharding
		// stop helping: every hot key lives in exactly one shard.
		g.zipf = rand.NewZipf(rng, c.zipfS+1, 1, uint64(c.keyspace-1))
	}
	return g
}

func (g *keyGen) next() int {
	if g.zipf != nil {
		return int(g.zipf.Uint64())
	}
	return g.rng.Intn(g.keyspace)
}

func measure(c benchConfig, getRatio float64, warmup bool) (result, error) {
	value := make([]byte, c.valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	hists := make([]*Histogram, c.conns)
	var ops, errs, notFound, omitted atomic.Uint64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	for w := 0; w < c.conns; w++ {
		h := NewHistogram()
		hists[w] = h
		wg.Add(1)
		go func(w int, h *Histogram) {
			defer wg.Done()
			cl, err := client.DialWithOptions(client.Options{Addr: c.addr, Timeout: 60 * time.Second})
			if err != nil {
				errs.Add(1)
				return
			}
			defer cl.Close()
			g := newKeyGen(c, int64(w)*7919+time.Now().UnixNano())

			<-startBarrier
			if c.mode == "open" {
				openLoop(c, cl, g, h, value, getRatio, stop, &ops, &errs, &notFound, &omitted)
			} else {
				closedLoop(c, cl, g, h, value, getRatio, stop, &ops, &errs, &notFound)
			}
		}(w, h)
	}

	close(startBarrier)
	start := time.Now()
	time.Sleep(c.duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	total := NewHistogram()
	for _, h := range hists {
		if h != nil {
			total.Merge(h)
		}
	}
	n := ops.Load()
	return result{
		hist:       total,
		ops:        n,
		errors:     errs.Load(),
		notFound:   notFound.Load(),
		elapsed:    elapsed,
		throughput: float64(n) / elapsed.Seconds(),
		omitted:    omitted.Load(),
	}, nil
}

// closedLoop sends the next request as soon as the previous one returns.
func closedLoop(c benchConfig, cl *client.Client, g *keyGen, h *Histogram, value []byte,
	getRatio float64, stop <-chan struct{}, ops, errs, notFound *atomic.Uint64) {

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-stop:
			return
		default:
		}

		if c.pipeline > 1 {
			p := cl.Pipeline()
			for i := 0; i < c.pipeline; i++ {
				if rng.Float64() < getRatio {
					p.Add(protocol.OpGet, protocol.Command{Key: keyFor(g.next())})
				} else {
					p.Add(protocol.OpSet, protocol.Command{Key: keyFor(g.next()), Value: value})
				}
			}
			start := time.Now()
			res, err := p.Run()
			lat := time.Since(start)
			if err != nil {
				errs.Add(1)
				return
			}
			// Attribute the batch latency to each request in it. This is an
			// approximation, and it is why pipelined latency numbers should
			// be read as throughput evidence rather than latency evidence.
			per := lat / time.Duration(len(res))
			for _, r := range res {
				h.Record(per)
				if r.Status == protocol.StatusNotFound {
					notFound.Add(1)
				} else if r.Status != protocol.StatusOK {
					errs.Add(1)
				}
			}
			ops.Add(uint64(len(res)))
			continue
		}

		start := time.Now()
		var err error
		if rng.Float64() < getRatio {
			_, err = cl.Get(keyFor(g.next()))
			if err == client.ErrNotFound {
				notFound.Add(1)
				err = nil
			}
		} else {
			err = cl.Set(keyFor(g.next()), value, 0)
		}
		h.Record(time.Since(start))
		if err != nil {
			errs.Add(1)
			return
		}
		ops.Add(1)
	}
}

// openLoop issues requests on a fixed schedule, measuring from the time each
// request was DUE rather than the time it was sent.
//
// That single detail is what defeats coordinated omission. If the server
// stalls for 200ms, requests 1..N whose scheduled time passed during the
// stall each record their full waiting time, exactly as a queued user would
// experience it.
func openLoop(c benchConfig, cl *client.Client, g *keyGen, h *Histogram, value []byte,
	getRatio float64, stop <-chan struct{}, ops, errs, notFound, omitted *atomic.Uint64) {

	perConnRate := float64(c.rate) / float64(c.conns)
	if perConnRate <= 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / perConnRate)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// The schedule is fixed in advance from a start instant; it does NOT
	// advance from "now" after each request. Deriving the next deadline from
	// the current time is precisely the bug that reintroduces coordinated
	// omission.
	begin := time.Now()
	var seq int64

	for {
		select {
		case <-stop:
			return
		default:
		}

		due := begin.Add(time.Duration(seq) * interval)
		seq++
		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-stop:
				timer.Stop()
				return
			}
		} else if wait < -interval {
			// We are behind schedule: the generator itself is the
			// bottleneck. Count it so the report can say so out loud
			// instead of quietly reporting optimistic numbers.
			omitted.Add(1)
		}

		var err error
		if rng.Float64() < getRatio {
			_, err = cl.Get(keyFor(g.next()))
			if err == client.ErrNotFound {
				notFound.Add(1)
				err = nil
			}
		} else {
			err = cl.Set(keyFor(g.next()), value, 0)
		}
		// Latency is measured from `due`, not from just before the call.
		h.Record(time.Since(due))
		if err != nil {
			errs.Add(1)
			return
		}
		ops.Add(1)
	}
}

func medianResult(rs []result) result {
	if len(rs) == 1 {
		return rs[0]
	}
	sorted := make([]result, len(rs))
	copy(sorted, rs)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].throughput < sorted[j-1].throughput; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return sorted[len(sorted)/2]
}

func spread(rs []result) (float64, float64) {
	lo, hi := math.MaxFloat64, 0.0
	for _, r := range rs {
		if r.throughput < lo {
			lo = r.throughput
		}
		if r.throughput > hi {
			hi = r.throughput
		}
	}
	return lo, hi
}

func appendCSV(c benchConfig, r result) error {
	_, statErr := os.Stat(c.csvPath)
	newFile := os.IsNotExist(statErr)

	f, err := os.OpenFile(c.csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if newFile {
		// Hardware and environment go in the file, not in someone's memory
		// of how the run was done.
		w.Write([]string{
			"label", "timestamp", "mode", "workload", "distribution", "conns", "pipeline",
			"value_size", "keyspace", "target_rate", "ops", "errors", "throughput_ops_s",
			"min_us", "p50_us", "p90_us", "p99_us", "p999_us", "max_us", "mean_us",
			"omitted", "client_os", "client_arch", "client_cpus",
		})
	}
	p := r.hist.Percentiles()
	us := func(d time.Duration) string {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1000.0, 'f', 2, 64)
	}
	w.Write([]string{
		c.label, time.Now().Format(time.RFC3339), c.mode, c.workload, c.distribution,
		strconv.Itoa(c.conns), strconv.Itoa(c.pipeline), strconv.Itoa(c.valueSize),
		strconv.Itoa(c.keyspace), strconv.Itoa(c.rate),
		strconv.FormatUint(r.ops, 10), strconv.FormatUint(r.errors, 10),
		strconv.FormatFloat(r.throughput, 'f', 1, 64),
		us(p["min"]), us(p["p50"]), us(p["p90"]), us(p["p99"]), us(p["p99.9"]), us(p["max"]), us(p["mean"]),
		strconv.FormatUint(r.omitted, 10),
		runtime.GOOS, runtime.GOARCH, strconv.Itoa(runtime.NumCPU()),
	})
	return w.Error()
}
