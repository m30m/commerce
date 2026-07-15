// burstgen — an adversarial, burst-oriented load generator.
//
// Unlike a steady RPS generator, burstgen overloads a service at *low average
// throughput* by firing tightly-synchronised bursts of concurrent requests and
// then idling. Against a fan-out service backed by a small connection pool, a
// burst whose width exceeds the pool depth saturates the pool, pushes downstream
// latency past its timeout, and — with no retries — cascades into 5xx errors far
// out of proportion to the average request rate. Between bursts the system is
// allowed to recover, so each burst's damage is attributable.
//
// It is intentionally standalone: its own Go module, standard library only, and
// no coupling to the rest of the repo. Build it with `go build` and run the
// binary directly against a target URL.
//
// Example:
//
//	go build -o burstgen .
//	./burstgen -target http://localhost:8000 -aggr 0.6
//	./burstgen -target http://localhost:8000 -ramp        # auto-find the knee
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// -- Configuration -----------------------------------------------------------

type config struct {
	target        string
	path          string
	aggr          float64
	burst         int
	gap           time.Duration
	users         int
	duration      time.Duration
	timeout       time.Duration
	ramp          bool
	rampThreshold float64
	seed          int64
}

// Aggressiveness maps the single 0..1 knob onto burst width and inter-burst gap.
const (
	minBurst = 4
	maxBurst = 250
	maxGap   = 2 * time.Second
	minGap   = 100 * time.Millisecond
)

// lerp linearly interpolates a..b by t (clamped to [0,1]).
func lerp(a, b, t float64) float64 {
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	return a + (b-a)*t
}

// deriveBurst returns the burst width implied by the aggressiveness knob.
func deriveBurst(aggr float64) int {
	return int(math.Round(lerp(minBurst, maxBurst, aggr)))
}

// deriveGap returns the inter-burst gap implied by the aggressiveness knob.
// Higher aggressiveness means a shorter gap.
func deriveGap(aggr float64) time.Duration {
	return time.Duration(lerp(float64(maxGap), float64(minGap), aggr))
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.target, "target", "", "Base URL of the target, e.g. http://localhost:8000 (required)")
	flag.StringVar(&c.path, "path", "/home/%d", "Request path template; %d is filled with a sampled id (use a literal path with no %d to hammer one endpoint)")
	flag.Float64Var(&c.aggr, "aggr", 0.5, "Aggressiveness knob 0.0-1.0; derives burst width and inter-burst gap")
	flag.IntVar(&c.burst, "burst", 0, "Concurrent requests per burst; 0 = derive from -aggr")
	flag.DurationVar(&c.gap, "gap", 0, "Delay between burst starts, e.g. 500ms; 0 = derive from -aggr")
	flag.IntVar(&c.users, "users", 500, "Id key space; each request samples a uniformly-random id in [1,users]")
	flag.DurationVar(&c.duration, "duration", 0, "Total run time; 0 = run until interrupted")
	flag.DurationVar(&c.timeout, "timeout", 10*time.Second, "Per-request client timeout")
	flag.BoolVar(&c.ramp, "ramp", false, "Auto-escalate burst width each cycle until the 5xx rate exceeds -ramp-threshold, then hold")
	flag.Float64Var(&c.rampThreshold, "ramp-threshold", 0.25, "5xx fraction that ends the ramp")
	flag.Int64Var(&c.seed, "seed", 0, "RNG seed for id sampling; 0 = time-based")
	flag.Parse()
	return c
}

// -- Per-burst statistics ----------------------------------------------------

// class buckets a completed request by outcome.
type class int

const (
	classOK  class = iota // 2xx / 3xx
	class4xx              // client error
	class5xx              // server error — the cascade signal
	classErr             // transport failure (timeout, connection refused, reset)
)

// result is one request's outcome.
type result struct {
	cls     class
	latency time.Duration
}

// burstStats aggregates the results of a single burst.
type burstStats struct {
	ok, c4, c5, errs int
	latencies        []time.Duration
	wall             time.Duration
}

func (b *burstStats) add(res result) {
	switch res.cls {
	case classOK:
		b.ok++
	case class4xx:
		b.c4++
	case class5xx:
		b.c5++
	case classErr:
		b.errs++
	}
	b.latencies = append(b.latencies, res.latency)
}

func (b *burstStats) total() int { return b.ok + b.c4 + b.c5 + b.errs }

// failRate is the fraction of requests that failed as 5xx or transport errors —
// the effects a pool/timeout cascade produces.
func (b *burstStats) failRate() float64 {
	t := b.total()
	if t == 0 {
		return 0
	}
	return float64(b.c5+b.errs) / float64(t)
}

// percentile returns the p-th (0..100) latency; latencies need not be sorted.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	s := make([]time.Duration, len(latencies))
	copy(s, latencies)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(math.Ceil(p/100*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

// -- Request issuing ---------------------------------------------------------

// classify maps an HTTP response / transport error to an outcome bucket.
func classify(status int, err error) class {
	if err != nil {
		return classErr
	}
	switch {
	case status >= 500:
		return class5xx
	case status >= 400:
		return class4xx
	default:
		return classOK
	}
}

// issue performs one request and returns its outcome. The body is fully drained
// and closed so the underlying connection can be reused within the burst.
func issue(ctx context.Context, client *http.Client, url string) result {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return result{cls: classErr, latency: time.Since(start)}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{cls: classErr, latency: time.Since(start)}
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return result{cls: classify(resp.StatusCode, nil), latency: time.Since(start)}
}

// runBurst fires `width` requests as simultaneously as the runtime allows: every
// worker goroutine is pre-spawned and blocked on a shared gate channel, which is
// then closed to release them all at once. It blocks until every request has
// completed (or timed out) and returns the aggregated outcome.
func runBurst(ctx context.Context, client *http.Client, cfg config, rng *lockedRand, width int) burstStats {
	gate := make(chan struct{})
	results := make([]result, width)
	var wg sync.WaitGroup
	wg.Add(width)
	for i := 0; i < width; i++ {
		url := cfg.target + formatPath(cfg.path, rng.id(cfg.users))
		go func(slot int, url string) {
			defer wg.Done()
			<-gate // block until released
			results[slot] = issue(ctx, client, url)
		}(i, url)
	}

	start := time.Now()
	close(gate) // release the whole burst at once
	wg.Wait()

	stats := burstStats{wall: time.Since(start)}
	for _, res := range results {
		stats.add(res)
	}
	return stats
}

// formatPath fills a "%d" path template with an id; a literal path (no verb) is
// returned unchanged so a single endpoint can be hammered.
func formatPath(tmpl string, id int) string {
	if !containsVerb(tmpl) {
		return tmpl
	}
	return fmt.Sprintf(tmpl, id)
}

func containsVerb(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '%' {
			if s[i+1] == '%' { // escaped percent, skip both
				i++
				continue
			}
			return true
		}
	}
	return false
}

// lockedRand is a tiny concurrency-safe wrapper; id sampling happens on the main
// goroutine here, but the lock keeps it safe and the seed reproducible.
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) id(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Intn(n) + 1 // ids are 1-based
}

// -- Main --------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	if cfg.target == "" {
		fmt.Fprintln(os.Stderr, "error: -target is required (e.g. -target http://localhost:8000)")
		flag.Usage()
		os.Exit(2)
	}
	if cfg.users < 1 {
		fmt.Fprintln(os.Stderr, "error: -users must be >= 1")
		os.Exit(2)
	}

	// Resolve the burst width and gap, letting explicit flags override the knob.
	// In ramp mode (with no explicit -burst) start at the minimum and climb, so
	// the run actually sweeps up to the tipping point instead of starting past it.
	width := cfg.burst
	if width <= 0 {
		if cfg.ramp {
			width = minBurst
		} else {
			width = deriveBurst(cfg.aggr)
		}
	}
	gap := cfg.gap
	if gap <= 0 {
		gap = deriveGap(cfg.aggr)
	}

	seed := cfg.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := &lockedRand{r: rand.New(rand.NewSource(seed))}

	// One client whose connection pool is wide enough that a burst genuinely
	// opens many simultaneous connections rather than serialising on keep-alive.
	maxConns := maxBurst + 16
	transport := &http.Transport{
		MaxConnsPerHost:     maxConns,
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Timeout: cfg.timeout, Transport: transport}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("burstgen -> %s%s  |  start burst=%d gap=%s  users=%d  ramp=%v\n",
		cfg.target, cfg.path, width, gap, cfg.users, cfg.ramp)
	if cfg.ramp {
		fmt.Printf("ramp: growing burst until 5xx+err rate > %.0f%%\n", cfg.rampThreshold*100)
	}

	run(ctx, client, cfg, rng, width, gap)
}

// run drives the burst loop until the duration elapses or an interrupt arrives,
// then prints a final summary.
func run(ctx context.Context, client *http.Client, cfg config, rng *lockedRand, width int, gap time.Duration) {
	var (
		started      = time.Now()
		deadline     time.Time
		grandTotal   int
		grand5xx     int
		grandErrs    int
		bursts       int
		rampFrozen   bool
		tippingPoint int
	)
	if cfg.duration > 0 {
		deadline = started.Add(cfg.duration)
	}

	for {
		if ctx.Err() != nil {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}

		stats := runBurst(ctx, client, cfg, rng, width)
		bursts++
		grandTotal += stats.total()
		grand5xx += stats.c5
		grandErrs += stats.errs

		elapsed := time.Since(started).Seconds()
		achievedRPS := 0.0
		if elapsed > 0 {
			achievedRPS = float64(grandTotal) / elapsed
		}
		fmt.Printf("burst#%-4d width=%-4d wall=%-8s 2xx=%-4d 4xx=%-4d 5xx=%-4d err=%-4d fail=%5.1f%%  p50=%-8s p95=%-8s p99=%-8s  achieved_rps=%.1f\n",
			bursts, width, stats.wall.Round(time.Millisecond),
			stats.ok, stats.c4, stats.c5, stats.errs, stats.failRate()*100,
			percentile(stats.latencies, 50).Round(time.Millisecond),
			percentile(stats.latencies, 95).Round(time.Millisecond),
			percentile(stats.latencies, 99).Round(time.Millisecond),
			achievedRPS,
		)

		// Ramp: escalate the burst width until the system starts failing, then
		// freeze at the width that first crossed the threshold.
		if cfg.ramp && !rampFrozen {
			if stats.failRate() >= cfg.rampThreshold {
				rampFrozen = true
				tippingPoint = width
				fmt.Printf(">> tipping point: burst width %d drove fail rate to %.1f%% — holding here\n",
					width, stats.failRate()*100)
			} else {
				next := width + width/4 + 1 // +25%, at least +1
				if next > maxBurst {
					next = maxBurst
				}
				width = next
			}
		}

		// Idle between bursts so the pool can recover and the next burst's damage
		// is attributable. Interruptible.
		select {
		case <-ctx.Done():
		case <-time.After(gap):
		}
	}

	elapsed := time.Since(started).Seconds()
	overallFail := 0.0
	if grandTotal > 0 {
		overallFail = float64(grand5xx+grandErrs) / float64(grandTotal)
	}
	achievedRPS := 0.0
	if elapsed > 0 {
		achievedRPS = float64(grandTotal) / elapsed
	}
	fmt.Println("---")
	fmt.Printf("summary: bursts=%d requests=%d 5xx=%d err=%d overall_fail=%.1f%% achieved_rps=%.1f elapsed=%.1fs\n",
		bursts, grandTotal, grand5xx, grandErrs, overallFail*100, achievedRPS, elapsed)
	if cfg.ramp && rampFrozen {
		fmt.Printf("summary: tipping point burst width = %d\n", tippingPoint)
	}
}
