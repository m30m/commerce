// Open-loop, constant-arrival-rate load generator.
//
// Design notes:
//
//   - OPEN LOOP. Requests are launched on a fixed wall-clock schedule (one every
//     1/RPS seconds) and fired as detached goroutines. Arrivals never wait for
//     prior responses, so when the system slows down we keep sending and the
//     latency tail is measured accurately (avoids coordinated omission).
//
//   - ZIPFIAN KEYS. Product and user ids are drawn from a bounded Zipf
//     distribution, so hot-key / cache-eviction effects show up.
//
//   - READ-HEAVY MIX. ~90% reads / ~10% writes.
//
// Everything is tunable via environment variables (see the constants below).
// Latency percentiles are reported periodically from the client's perspective.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// -- Configuration (all overridable via environment variables) ---------------

var (
	gatewayURL     = getenvStr("GATEWAY_URL", "http://gateway:8000")
	rps            = getenvFloat("RPS", 50)
	durationS      = getenvFloat("DURATION_S", 0) // 0 = run forever
	readFraction   = getenvFloat("READ_FRACTION", 0.9)
	numProducts    = getenvInt("NUM_PRODUCTS", 1000)
	numUsers       = getenvInt("NUM_USERS", 500)
	zipfS          = getenvFloat("ZIPF_S", 1.1) // skew; >1 = more concentrated
	warmupS        = getenvFloat("WARMUP_S", 10)
	reportInterval = getenvFloat("REPORT_INTERVAL_S", 10)
	requestTimeout = getenvFloat("REQUEST_TIMEOUT_S", 10)
	// Safety cap on concurrent in-flight requests. Kept large so we stay
	// open-loop; only a genuinely collapsing system hits it, and overflow is
	// recorded as a drop rather than being allowed to blow up generator memory.
	maxInflight = int64(getenvInt("MAX_INFLIGHT", 5000))
)

// -- Structured JSON logging (matches the services' log schema) --------------

var logMu sync.Mutex

// logJSON emits one JSON line on stdout with the stable base fields the log
// collector expects (time, level, logger, service, message) plus any
// caller-supplied structured fields.
func logJSON(level, message string, fields map[string]any) {
	payload := make(map[string]any, len(fields)+5)
	for k, v := range fields {
		payload[k] = v
	}
	payload["time"] = time.Now().UTC().Format("2006-01-02T15:04:05.000000-07:00")
	payload["level"] = level
	payload["logger"] = "loadgen"
	payload["service"] = "loadgen"
	payload["message"] = message

	line, err := json.Marshal(payload)
	if err != nil {
		line = []byte(fmt.Sprintf(`{"level":%q,"service":"loadgen","message":%q}`, level, message))
	}
	logMu.Lock()
	os.Stdout.Write(append(line, '\n'))
	logMu.Unlock()
}

// -- Zipf sampling -----------------------------------------------------------

// zipfSampler is a bounded Zipf sampler over ids 1..n via a precomputed CDF.
type zipfSampler struct {
	cdf []float64
}

func newZipfSampler(n int, s float64) *zipfSampler {
	weights := make([]float64, n)
	var total float64
	for i := 0; i < n; i++ {
		w := 1.0 / math.Pow(float64(i+1), s)
		weights[i] = w
		total += w
	}
	cdf := make([]float64, n)
	var acc float64
	for i, w := range weights {
		acc += w / total
		cdf[i] = acc
	}
	return &zipfSampler{cdf: cdf}
}

func (z *zipfSampler) sample(r *rand.Rand) int {
	// bisect_left over the CDF, returning a 1-based id.
	return sort.SearchFloat64s(z.cdf, r.Float64()) + 1
}

// -- Stats -------------------------------------------------------------------

// stats keeps running counters plus a bounded ring buffer of recent latencies
// (seconds) used to compute percentiles from the client's perspective.
type stats struct {
	mu        sync.Mutex
	latencies []float64 // ring buffer of at most `window` samples
	window    int
	head      int
	full      bool

	total  atomic.Int64
	errors atomic.Int64
	drops  atomic.Int64
}

func newStats(window int) *stats {
	return &stats{latencies: make([]float64, 0, window), window: window}
}

func (s *stats) record(elapsed float64, ok bool) {
	s.total.Add(1)
	if !ok {
		s.errors.Add(1)
		return
	}
	s.mu.Lock()
	if len(s.latencies) < s.window {
		s.latencies = append(s.latencies, elapsed)
	} else {
		s.latencies[s.head] = elapsed
		s.head = (s.head + 1) % s.window
		s.full = true
	}
	s.mu.Unlock()
}

type snapshot struct {
	total, errors, drops int64
	p50, p95, p99        float64 // milliseconds
}

func (s *stats) snapshot() snapshot {
	s.mu.Lock()
	lat := make([]float64, len(s.latencies))
	copy(lat, s.latencies)
	s.mu.Unlock()
	sort.Float64s(lat)

	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		idx := int(p / 100.0 * float64(len(lat)))
		if idx > len(lat)-1 {
			idx = len(lat) - 1
		}
		return lat[idx]
	}
	return snapshot{
		total:  s.total.Load(),
		errors: s.errors.Load(),
		drops:  s.drops.Load(),
		p50:    round(pct(50)*1000, 1),
		p95:    round(pct(95)*1000, 1),
		p99:    round(pct(99)*1000, 1),
	}
}

// -- Shared runtime state ----------------------------------------------------

var (
	products *zipfSampler
	users    *zipfSampler
	st       *stats
	inflight atomic.Int64
	// Worst schedule lag (nanoseconds behind the fixed arrival schedule) seen
	// since the last report. Surfaced by the reporter so "the generator can't
	// keep up" is visible instead of silent.
	maxLagNs atomic.Int64
)

func recordLag(ns int64) {
	for {
		cur := maxLagNs.Load()
		if ns <= cur || maxLagNs.CompareAndSwap(cur, ns) {
			return
		}
	}
}

// -- Request selection -------------------------------------------------------

type request struct {
	method string
	url    string
	body   []byte // nil for reads
}

// nextRequest picks the next request to issue based on the workload mix.
func nextRequest(r *rand.Rand) request {
	if r.Float64() < readFraction {
		// Reads: mostly the home aggregate, some direct product lookups.
		if r.Float64() < 0.6 {
			return request{"GET", fmt.Sprintf("%s/home/%d", gatewayURL, users.sample(r)), nil}
		}
		return request{"GET", fmt.Sprintf("%s/products/%d", gatewayURL, products.sample(r)), nil}
	}
	// Writes: add a hot product to a user's cart.
	uid := users.sample(r)
	body, _ := json.Marshal(map[string]int{
		"product_id": products.sample(r),
		"qty":        r.Intn(3) + 1,
	})
	return request{"POST", fmt.Sprintf("%s/carts/%d/items", gatewayURL, uid), body}
}

// issue fires a single request and records its client-side latency. ok is true
// unless the request failed to complete or the server returned a 5xx.
func issue(client *http.Client, req request) {
	inflight.Add(1)
	start := time.Now()
	ok := false
	defer func() {
		st.record(time.Since(start).Seconds(), ok)
		inflight.Add(-1)
	}()

	var bodyReader io.Reader
	if req.body != nil {
		bodyReader = bytes.NewReader(req.body)
	}
	httpReq, err := http.NewRequest(req.method, req.url, bodyReader)
	if err != nil {
		return
	}
	if req.body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return
	}
	// Drain and close so the connection can be reused (keep-alive).
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ok = resp.StatusCode < 500
}

// -- Reporter ----------------------------------------------------------------

func reporter(ctx context.Context, started time.Time) {
	ticker := time.NewTicker(time.Duration(reportInterval * float64(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		elapsed := time.Since(started).Seconds()
		snap := st.snapshot()
		phase := "steady"
		if elapsed < warmupS {
			phase = "warmup"
		}
		lag := float64(maxLagNs.Swap(0)) / float64(time.Second)
		// Effective RPS actually achieved this window (vs the requested RPS).
		achieved := 0.0
		if elapsed > 0 {
			achieved = round(float64(snap.total)/elapsed, 1)
		}
		logJSON("INFO", "load report", map[string]any{
			"elapsed_s":    round(elapsed, 1),
			"phase":        phase,
			"rps":          rps,
			"achieved_rps": achieved,
			"behind_s":     round(lag, 3),
			"inflight":     inflight.Load(),
			"total":        snap.total,
			"errors":       snap.errors,
			"drops":        snap.drops,
			"p50_ms":       snap.p50,
			"p95_ms":       snap.p95,
			"p99_ms":       snap.p99,
		})
		// Loud, throttled warning when we can't sustain the requested rate.
		if lag > 1.0 || snap.drops > 0 {
			logJSON("WARNING", "load generator falling behind requested RPS", map[string]any{
				"behind_s":      round(lag, 3),
				"drops":         snap.drops,
				"inflight":      inflight.Load(),
				"requested_rps": rps,
				"achieved_rps":  achieved,
			})
		}
	}
}

// -- Main --------------------------------------------------------------------

func main() {
	products = newZipfSampler(numProducts, zipfS)
	users = newZipfSampler(numUsers, zipfS)
	st = newStats(20000)

	var durForLog any
	if durationS != 0 {
		durForLog = durationS
	}
	logJSON("INFO", "load starting", map[string]any{
		"target":        gatewayURL,
		"rps":           rps,
		"read_fraction": readFraction,
		"zipf_s":        zipfS,
		"duration_s":    durForLog,
	})

	// One HTTP client with a connection pool sized like the Python httpx limits:
	// up to MAX_INFLIGHT concurrent connections, 200 kept alive idle.
	transport := &http.Transport{
		MaxConnsPerHost:     int(maxInflight),
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Timeout:   time.Duration(requestTimeout * float64(time.Second)),
		Transport: transport,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	started := time.Now()
	go reporter(ctx, started)

	interval := time.Duration(float64(time.Second) / rps)
	var duration time.Duration
	if durationS > 0 {
		duration = time.Duration(durationS * float64(time.Second))
	}

	nextAt := time.Duration(0) // offset from `started`
	for {
		if ctx.Err() != nil {
			break
		}
		if duration > 0 && time.Since(started) >= duration {
			break
		}
		if inflight.Load() < maxInflight {
			req := nextRequest(rng)
			go issue(client, req)
		} else {
			st.drops.Add(1)
		}
		// Advance the schedule by a fixed step regardless of completion time.
		nextAt += interval
		sleep := nextAt - time.Since(started)
		if sleep > 0 {
			timer := time.NewTimer(sleep)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		} else {
			// Behind schedule (the server or our own CPU can't keep up). Stay
			// open-loop and keep firing, but track how far behind we are so the
			// reporter can surface it instead of the generator silently
			// struggling.
			recordLag(int64(-sleep))
		}
	}

	logJSON("INFO", "load finished", func() map[string]any {
		snap := st.snapshot()
		return map[string]any{
			"total":  snap.total,
			"errors": snap.errors,
			"drops":  snap.drops,
			"p50_ms": snap.p50,
			"p95_ms": snap.p95,
			"p99_ms": snap.p99,
		}
	}())
}

// -- Small helpers -----------------------------------------------------------

func round(x float64, decimals int) float64 {
	p := math.Pow(10, float64(decimals))
	return math.Round(x*p) / p
}

func getenvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err == nil {
			return f
		}
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}
