// whalegen — a cohort-shaped load generator: a small "reseller" cohort quietly
// accumulates very large carts inside otherwise ordinary consumer traffic.
//
// The story is a B2B/reseller segment on a consumer commerce site. It is tiny —
// ~2% of the user base — but its members treat the cart as a standing order and
// let it grow to the cart service's ceiling. Nothing about the traffic *shape*
// is abusive: the request rate is flat, the mix is normal, every response is a
// 200. Only a handful of user ids are expensive, and they are expensive on every
// single request they make.
//
// The chain:
//
//	A whale's GET /carts/{uid} returns a full CART_PAGE_LIMIT page of cart_items
//	instead of a consumer's handful. Cart then enriches that page with one
//	batched call to product (`/products/batch?ids=` with ~1000 ids), which mgets
//	a cache key per id and, on a miss, re-reads and re-caches each row one await
//	at a time. The enriched cart is then serialised, embedded in the gateway's
//	/home aggregate, and serialised again. Every step scales with cart size, so a
//	whale's /home costs several times a consumer's /home — and the whales pay it
//	on every request.
//
//	Cart size is the whole mechanism, and CART_PAGE_LIMIT is what bounds it: the
//	marginal cost of a cart item is only ~19µs warm, so it takes ~1000 of them to
//	rise clear of the ~5ms fixed cost of a request. At the old 100-item ceiling
//	this scenario did not reproduce at all — whales came in at ~1.2x a consumer
//	and never surfaced above ambient noise. See the README's measurements.
//
// What that looks like from the outside is the point of the scenario: whales are
// ~2% of the user id space and are sampled like everyone else, so they are ~2%
// of requests. The gateway's p50 and p95 never move — they are consumer traffic.
// Only the top ~2% of the latency distribution is whale traffic, so p99 lifts
// clean off p50 and stays there. The RED metrics carry no user dimension at all
// (`http_request_duration_seconds` is labelled by method and route template
// only), so the dashboards can show *that* p99 diverged but contain nothing that
// can explain *why*. The user id survives in exactly one place: the raw `path`
// field of the structured access log (`/home/417`). Attributing the p99 means
// aggregating `duration_ms` by uid out of Loki and noticing that the slow
// requests are not spread across the fleet — they are the same few ids, over and
// over. See the README for the LogQL and the decoys this run also lights up.
//
// Standard library only; its own module. Build with `go build` and point it at a
// target you own.
//
// Example:
//
//	go build -o whalegen .
//	./whalegen -target http://localhost:8000
//	./whalegen -target http://localhost:8000 -rps 100 -whale-ratio 0.02 -duration 10m
//	./whalegen -target http://localhost:8000 -drain     # reset the cohort's carts
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
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
	users       int
	products    int
	whaleRatio  float64
	cartSize    int
	whaleQty    int
	fillDelay   time.Duration
	fillRPS     float64
	detectRatio float64
	drain       bool
	maxInflight int
	duration    time.Duration
	timeout     time.Duration
	report      time.Duration
	seed        int64
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.target, "target", "", "Base URL of the target, e.g. http://localhost:8000 (required)")
	flag.Float64Var(&c.rps, "rps", 50, "Steady baseline request rate (requests/second)")
	flag.Float64Var(&c.writeRatio, "write-ratio", 0.10, "Fraction of baseline requests that are writes (rest are reads)")
	flag.IntVar(&c.users, "users", 500, "User id key space; ids are sampled uniformly in [1,users]")
	flag.IntVar(&c.products, "products", 1000, "Product id key space; sampled in [1,products]")
	flag.Float64Var(&c.whaleRatio, "whale-ratio", 0.02, "Fraction of users in the reseller cohort; these are the only expensive ids")
	flag.IntVar(&c.cartSize, "cart-size", 1000, "Distinct products to accumulate in each whale's cart; capped in practice by the cart service's CART_PAGE_LIMIT (default 1000)")
	flag.IntVar(&c.whaleQty, "whale-qty", 12, "Max qty per whale line item; resellers buy in bulk (affects payload, not row count)")
	flag.DurationVar(&c.fillDelay, "fill-delay", 10*time.Second, "Delay before the cohort starts accumulating, so a clean baseline is on the dashboards first")
	flag.Float64Var(&c.fillRPS, "fill-rps", 200, "Rate of cohort cart-adds (requests/second); a full cohort is ~10k adds, so this sets how long loading takes (~50s at 200/s). Lower = a more gradual p99 ramp")
	flag.Float64Var(&c.detectRatio, "detect-ratio", 5.0, "Report divergence once whale p99 reaches this multiple of the consumer p50 (the baseline a normal user sees)")
	flag.BoolVar(&c.drain, "drain", false, "Teardown mode: empty the cohort's carts and exit (no load is generated)")
	flag.IntVar(&c.maxInflight, "max-inflight", 256, "Cap on concurrent in-flight requests; ticks are dropped when exceeded")
	flag.DurationVar(&c.duration, "duration", 0, "Total run time; 0 = run until interrupted")
	flag.DurationVar(&c.timeout, "timeout", 15*time.Second, "Per-request client timeout")
	flag.DurationVar(&c.report, "report", 10*time.Second, "Interval between cohort-split status lines")
	flag.Int64Var(&c.seed, "seed", 0, "RNG seed; 0 = time-based. A fixed seed reproduces the same cohort")
	flag.Parse()
	return c
}

// -- Cohort ------------------------------------------------------------------

// cohort is the set of whale user ids, plus O(1) membership for the hot path.
type cohort struct {
	ids []int
	set map[int]bool
}

func (c *cohort) isWhale(uid int) bool { return c.set[uid] }

// pickCohort samples the reseller ids uniformly without replacement. It is
// seeded, so a given -seed always yields the same cohort: the run is
// reproducible, and the printed id list is the answer key for grading.
func pickCohort(cfg config, rng *lockedRand) *cohort {
	n := int(math.Round(float64(cfg.users) * cfg.whaleRatio))
	if n < 1 {
		n = 1
	}
	if n > cfg.users {
		n = cfg.users
	}
	perm := rng.perm(cfg.users)
	c := &cohort{set: make(map[int]bool, n)}
	for _, i := range perm[:n] {
		uid := i + 1 // ids are 1-based
		c.ids = append(c.ids, uid)
		c.set[uid] = true
	}
	sort.Ints(c.ids)
	return c
}

// -- Latency sampling --------------------------------------------------------

// latencyCap bounds a cohort's cumulative sample set. Past the cap the sample is
// maintained by reservoir sampling, so the end-of-run percentiles stay
// representative of the whole run without growing without bound.
const latencyCap = 100_000

// samples collects one cohort's read latencies. `window` is drained by each
// status line (percentiles over the last interval); `total` is the bounded
// reservoir behind the final summary.
type samples struct {
	mu     sync.Mutex
	window []time.Duration
	total  []time.Duration
	seen   int64
}

func (s *samples) add(d time.Duration, rng *lockedRand) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.window = append(s.window, d)
	s.seen++
	if len(s.total) < latencyCap {
		s.total = append(s.total, d)
		return
	}
	// Reservoir: keep each observed latency with probability latencyCap/seen.
	if j := rng.intn(int(s.seen)); j < latencyCap {
		s.total[j] = d
	}
}

// takeWindow returns the latencies since the last call and resets the window.
func (s *samples) takeWindow() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.window
	s.window = nil
	return w
}

func (s *samples) snapshotTotal() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.total))
	copy(out, s.total)
	return out
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

// -- Shared counters ---------------------------------------------------------

// stats holds live counters shared across goroutines. Counter fields are
// accessed atomically; the sample sets carry their own lock.
type stats struct {
	reads   atomic.Int64
	writes  atomic.Int64
	ok      atomic.Int64 // 2xx/3xx
	c4xx    atomic.Int64
	c5xx    atomic.Int64
	errs    atomic.Int64 // transport failures
	dropped atomic.Int64 // ticks skipped because max-inflight was reached

	fillAdds   atomic.Int64 // cohort cart-adds accepted
	fillTotal  atomic.Int64 // cohort cart-adds planned
	fillDone   atomic.Bool
	whalesFull atomic.Int64 // whales confirmed at target cart size

	// Read latency, split by cohort. This split is the ground truth the
	// scenario asks the operator to rediscover from logs alone.
	consumer samples
	whale    samples
	blended  samples // every read, as a uid-less dashboard would see it

	diverged atomic.Bool
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
// It returns the status code, the elapsed time and any transport error.
func doDrain(ctx context.Context, client *http.Client, s *stats, method, url string, body []byte) (int, time.Duration, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		s.record(0, err)
		return 0, time.Since(start), err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		s.record(0, err)
		return 0, time.Since(start), err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	s.record(resp.StatusCode, nil)
	return resp.StatusCode, elapsed, nil
}

// addToCart adds product `pid` to user `uid`'s cart via the edge.
func addToCart(ctx context.Context, client *http.Client, s *stats, target string, uid, pid, qty int) (int, error) {
	body, _ := json.Marshal(map[string]int{"product_id": pid, "qty": qty})
	status, _, err := doDrain(ctx, client, s, http.MethodPost, fmt.Sprintf("%s/carts/%d/items", target, uid), body)
	return status, err
}

// homeResponse is the slice of /home we inspect: the cart's line items. The edge
// exposes no direct cart read, so /home is how we observe a cart from outside.
type homeResponse struct {
	Cart struct {
		Items []struct {
			ProductID int `json:"product_id"`
		} `json:"items"`
	} `json:"cart"`
}

// cartProducts returns the product ids currently visible in uid's cart. This is
// what the cart service actually pages back (CART_PAGE_LIMIT), not the raw row count.
func cartProducts(ctx context.Context, client *http.Client, target string, uid int) ([]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/home/%d", target, uid), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var hr homeResponse
	if err := json.Unmarshal(body, &hr); err != nil {
		return nil, fmt.Errorf("decoding home response: %w", err)
	}
	out := make([]int, 0, len(hr.Cart.Items))
	for _, it := range hr.Cart.Items {
		out = append(out, it.ProductID)
	}
	return out, nil
}

// -- Baseline load -----------------------------------------------------------

// runBaseline dispatches requests at cfg.rps, split read/write by cfg.writeRatio.
// User ids are sampled uniformly across the whole key space — the cohort gets no
// special treatment here, which is the point: whales are ~whale-ratio of the
// traffic purely because they are ~whale-ratio of the users. Each tick launches
// one request bounded by a semaphore; when the semaphore is full the tick is
// dropped rather than blocking the pacer, so a slowing target shows up as
// dropped ticks instead of unbounded goroutine growth.
func runBaseline(ctx context.Context, client *http.Client, cfg config, s *stats, co *cohort, rng *lockedRand, sem chan struct{}) {
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
						pid := rng.intn(cfg.products) + 1
						addToCart(ctx, client, s, cfg.target, uid, pid, 1)
						return
					}
					s.reads.Add(1)
					_, elapsed, err := doDrain(ctx, client, s, http.MethodGet,
						fmt.Sprintf("%s/home/%d", cfg.target, uid), nil)
					if err != nil {
						return
					}
					// Attribute the latency to its cohort. The target has no
					// idea this split exists; only we do, because we chose the
					// ids.
					s.blended.add(elapsed, rng)
					if co.isWhale(uid) {
						s.whale.add(elapsed, rng)
					} else {
						s.consumer.add(elapsed, rng)
					}
				}()
			default:
				s.dropped.Add(1)
			}
		}
	}
}

// -- Cohort accumulation -----------------------------------------------------

// cartAdd is one planned reseller cart line.
type cartAdd struct{ uid, pid, qty int }

// planFill reads each whale's current cart through the edge and returns the
// cart-adds needed to bring every whale up to cfg.cartSize distinct products.
//
// Two details matter. Products are only ever added once per cart: the batch
// fetch dedupes ids, so a repeated product would add a cart_items row without
// adding any fetch work — it would grow the cart on paper but not in cost. And
// the plan is round-robin across whales rather than whale-by-whale, so the whole
// cohort's carts grow together and the p99 ramps up smoothly instead of one
// whale at a time going over the cliff.
func planFill(ctx context.Context, client *http.Client, cfg config, co *cohort, rng *lockedRand) []cartAdd {
	perWhale := make([][]cartAdd, 0, len(co.ids))
	longest := 0
	for _, uid := range co.ids {
		have, err := cartProducts(ctx, client, cfg.target, uid)
		if err != nil {
			logf("whales: could not read cart for uid=%d (%v) — assuming empty", uid, err)
		}
		inCart := make(map[int]bool, len(have))
		for _, pid := range have {
			inCart[pid] = true
		}
		need := cfg.cartSize - len(inCart)
		if need <= 0 {
			continue
		}
		adds := make([]cartAdd, 0, need)
		for _, i := range rng.perm(cfg.products) {
			if need == 0 {
				break
			}
			pid := i + 1 // ids are 1-based
			if inCart[pid] {
				continue
			}
			adds = append(adds, cartAdd{uid: uid, pid: pid, qty: rng.intn(cfg.whaleQty) + 1})
			inCart[pid] = true
			need--
		}
		perWhale = append(perWhale, adds)
		if len(adds) > longest {
			longest = len(adds)
		}
	}

	var plan []cartAdd
	for i := 0; i < longest; i++ {
		for _, adds := range perWhale {
			if i < len(adds) {
				plan = append(plan, adds[i])
			}
		}
	}
	return plan
}

// runFill waits for the baseline to settle, works out what each whale is missing,
// then issues the cart-adds at cfg.fillRPS. It stops once every whale is at the
// target size — the cohort is a standing order, not a runaway writer, and the
// scenario is about how those carts are *read* from then on.
func runFill(ctx context.Context, client *http.Client, cfg config, s *stats, co *cohort, rng *lockedRand) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(cfg.fillDelay):
	}

	plan := planFill(ctx, client, cfg, co, rng)
	s.fillTotal.Store(int64(len(plan)))
	if len(plan) == 0 {
		logf("whales: cohort already at %d items/cart — nothing to accumulate", cfg.cartSize)
		s.fillDone.Store(true)
		s.whalesFull.Store(int64(len(co.ids)))
		return
	}
	logf("whales: accumulating %d cart lines across %d resellers at %.0f/s (target %d distinct products each)",
		len(plan), len(co.ids), cfg.fillRPS, cfg.cartSize)

	interval := time.Duration(float64(time.Second) / cfg.fillRPS)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	started := time.Now()
	for _, add := range plan {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if status, err := addToCart(ctx, client, s, cfg.target, add.uid, add.pid, add.qty); err == nil && status < 300 {
			s.fillAdds.Add(1)
		}
	}
	s.fillDone.Store(true)
	logf("whales: cohort loaded — %d/%d cart lines accepted in %s; every reseller now pages a full %d-item cart",
		s.fillAdds.Load(), len(plan), time.Since(started).Round(time.Second), cfg.cartSize)

	// Confirm from outside what the cart service actually pages back, rather
	// than trusting the adds: this is the number the scenario turns on.
	full := 0
	for _, uid := range co.ids {
		if items, err := cartProducts(ctx, client, cfg.target, uid); err == nil && len(items) >= cfg.cartSize {
			full++
		}
	}
	s.whalesFull.Store(int64(full))
	logf("whales: %d/%d resellers confirmed at >=%d items in the cart page", full, len(co.ids), cfg.cartSize)
}

// -- Drain (teardown) --------------------------------------------------------

// runDrain empties the cohort's carts via DELETE /carts/{uid}/items/{pid} and
// returns. The cart page is capped at CART_PAGE_LIMIT, so a cart holding more than a
// page needs several passes: each pass deletes every row for the products on the
// current page, then re-reads what surfaced behind them.
func runDrain(ctx context.Context, client *http.Client, cfg config, s *stats, co *cohort) {
	logf("drain: emptying carts for %d resellers: %v", len(co.ids), co.ids)
	const maxPasses = 20
	totalRemoved := 0
	for _, uid := range co.ids {
		removed := 0
		for pass := 0; pass < maxPasses; pass++ {
			items, err := cartProducts(ctx, client, cfg.target, uid)
			if err != nil {
				logf("drain: uid=%d: reading cart failed: %v", uid, err)
				break
			}
			if len(items) == 0 {
				break
			}
			for _, pid := range items {
				if ctx.Err() != nil {
					return
				}
				status, _, err := doDrain(ctx, client, s, http.MethodDelete,
					fmt.Sprintf("%s/carts/%d/items/%d", cfg.target, uid, pid), nil)
				if err == nil && status < 300 {
					removed++
				}
			}
		}
		totalRemoved += removed
		logf("drain: uid=%d: removed %d cart lines", uid, removed)
	}
	logf("drain: done — %d cart lines removed across the cohort", totalRemoved)
}

// -- Reporting ---------------------------------------------------------------

func runReporter(ctx context.Context, cfg config, s *stats, co *cohort, started time.Time) {
	ticker := time.NewTicker(cfg.report)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printWindow(cfg, s, co, started)
		}
	}
}

// printWindow prints the status block for the last report interval: the blended
// view a uid-less dashboard would show, then the same requests split by cohort.
func printWindow(cfg config, s *stats, co *cohort, started time.Time) {
	blended := s.blended.takeWindow()
	consumer := s.consumer.takeWindow()
	whale := s.whale.takeWindow()

	fill := fmt.Sprintf("carts %d/%d filled", s.fillAdds.Load(), s.fillTotal.Load())
	if s.fillDone.Load() {
		fill = fmt.Sprintf("cohort loaded (%d/%d resellers at >=%d items)",
			s.whalesFull.Load(), len(co.ids), cfg.cartSize)
	}

	logf("[t=%s] reads=%d writes=%d | 2xx=%d 4xx=%d 5xx=%d err=%d dropped=%d | %s",
		time.Since(started).Round(time.Second), s.reads.Load(), s.writes.Load(),
		s.ok.Load(), s.c4xx.Load(), s.c5xx.Load(), s.errs.Load(), s.dropped.Load(), fill)
	fmt.Printf("           blended  %s%s\n", fmtPct(blended), divergenceNote(blended))
	fmt.Printf("           consumer %s\n", fmtPct(consumer))
	fmt.Printf("           whale    %s%s\n", fmtPct(whale), ratioNote(consumer, whale))

	// Report the divergence once, the moment it is real, the way poisongen
	// reports the poison landing in recommendations. The comparison is the one
	// the scenario is written around: a whale's slow request against the
	// baseline a consumer experiences (their median), not against the consumer
	// tail — the consumer tail is noise the cohort has nothing to do with.
	if !s.diverged.Load() && len(whale) >= 5 && len(consumer) >= 20 {
		base, wp99 := percentile(consumer, 50), percentile(whale, 99)
		if base > 0 && float64(wp99)/float64(base) >= cfg.detectRatio {
			if s.diverged.CompareAndSwap(false, true) {
				logf("DETECTED: reseller p99 is %.1fx the consumer baseline (%s vs p50 %s) while blended p50 sits flat at %s — "+
					"p99 has left p50 behind, and no metric label says which users are responsible",
					float64(wp99)/float64(base), wp99.Round(time.Millisecond), base.Round(time.Millisecond),
					percentile(blended, 50).Round(time.Millisecond))
			}
		}
	}
}

// divergenceNote annotates the blended line with its own p99/p50 spread — the
// shape of the anomaly as the gateway's RED dashboard renders it, with no way to
// attribute it.
func divergenceNote(blended []time.Duration) string {
	if len(blended) == 0 {
		return ""
	}
	p50, p99 := percentile(blended, 50), percentile(blended, 99)
	if p50 <= 0 {
		return ""
	}
	return fmt.Sprintf("  p99/p50 = %.1fx  <- the dashboard's whole story", float64(p99)/float64(p50))
}

// fmtPct renders a percentile summary for one sample set.
func fmtPct(l []time.Duration) string {
	if len(l) == 0 {
		return "(no samples)"
	}
	return fmt.Sprintf("p50=%-8s p95=%-8s p99=%-8s (n=%d)",
		percentile(l, 50).Round(time.Millisecond),
		percentile(l, 95).Round(time.Millisecond),
		percentile(l, 99).Round(time.Millisecond),
		len(l))
}

// ratioNote annotates the whale line with the two multiples that matter, which
// are very different numbers and answer different questions:
//
//   - vs the consumer p50: how much slower a reseller's slow request is than the
//     baseline a normal user experiences. This is the scenario's headline.
//   - vs the consumer p99: whether the cohort is separated from the consumer
//     tail at all. Below ~1 the cohort is hiding inside ordinary noise and the
//     blended p99 is not actually theirs to own.
func ratioNote(consumer, whale []time.Duration) string {
	if len(consumer) == 0 || len(whale) == 0 {
		return ""
	}
	cp50, cp99, wp99 := percentile(consumer, 50), percentile(consumer, 99), percentile(whale, 99)
	if cp50 <= 0 || cp99 <= 0 {
		return ""
	}
	return fmt.Sprintf("  p99 = %.1fx consumer p50 (baseline), %.1fx consumer p99",
		float64(wp99)/float64(cp50), float64(wp99)/float64(cp99))
}

// printSummary prints the whole-run picture, including the cohort answer key.
func printSummary(cfg config, s *stats, co *cohort, started time.Time) {
	blended := s.blended.snapshotTotal()
	consumer := s.consumer.snapshotTotal()
	whale := s.whale.snapshotTotal()

	fmt.Println("---")
	fmt.Printf("summary: elapsed=%s reads=%d writes=%d 2xx=%d 4xx=%d 5xx=%d err=%d dropped=%d\n",
		time.Since(started).Round(time.Second), s.reads.Load(), s.writes.Load(),
		s.ok.Load(), s.c4xx.Load(), s.c5xx.Load(), s.errs.Load(), s.dropped.Load())
	fmt.Printf("summary: blended  %s\n", fmtPct(blended))
	fmt.Printf("summary: consumer %s\n", fmtPct(consumer))
	fmt.Printf("summary: whale    %s%s\n", fmtPct(whale), ratioNote(consumer, whale))
	fmt.Printf("summary: reseller cohort (%d of %d users, %.1f%%, cart target %d) = %v\n",
		len(co.ids), cfg.users, float64(len(co.ids))/float64(cfg.users)*100, cfg.cartSize, co.ids)
	fmt.Println("summary: ^ that id list is the answer key — the dashboards cannot produce it, only the access logs can.")
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

func (l *lockedRand) perm(n int) []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Perm(n)
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
	if cfg.rps <= 0 || cfg.fillRPS <= 0 {
		fmt.Fprintln(os.Stderr, "error: -rps and -fill-rps must be > 0")
		os.Exit(2)
	}
	if cfg.whaleRatio <= 0 || cfg.whaleRatio > 1 {
		fmt.Fprintln(os.Stderr, "error: -whale-ratio must be in (0,1]")
		os.Exit(2)
	}
	if cfg.cartSize < 1 || cfg.cartSize > cfg.products {
		fmt.Fprintln(os.Stderr, "error: -cart-size must be >= 1 and <= -products")
		os.Exit(2)
	}
	if cfg.whaleQty < 1 {
		fmt.Fprintln(os.Stderr, "error: -whale-qty must be >= 1")
		os.Exit(2)
	}
	if cfg.report <= 0 {
		fmt.Fprintln(os.Stderr, "error: -report must be > 0")
		os.Exit(2)
	}
	cfg.target = strings.TrimRight(cfg.target, "/")

	seed := cfg.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := &lockedRand{r: rand.New(rand.NewSource(seed))}

	// A pool wide enough that concurrent baseline + fill requests genuinely open
	// parallel connections rather than serialising on keep-alive.
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

	co := pickCohort(cfg, rng)
	s := &stats{}

	if cfg.drain {
		runDrain(ctx, client, cfg, s, co)
		return
	}

	logf("whalegen -> %s  |  rps=%.0f write_ratio=%.0f%% users=%d seed=%d",
		cfg.target, cfg.rps, cfg.writeRatio*100, cfg.users, seed)
	logf("cohort: %d resellers (%.1f%% of users) accumulating %d-product carts after %s at %.0f adds/s",
		len(co.ids), float64(len(co.ids))/float64(cfg.users)*100, cfg.cartSize, cfg.fillDelay, cfg.fillRPS)
	logf("cohort: uids = %v", co.ids)

	sem := make(chan struct{}, cfg.maxInflight)
	started := time.Now()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); runBaseline(ctx, client, cfg, s, co, rng, sem) }()
	go func() { defer wg.Done(); runFill(ctx, client, cfg, s, co, rng) }()
	go func() { defer wg.Done(); runReporter(ctx, cfg, s, co, started) }()
	wg.Wait()

	printSummary(cfg, s, co, started)
	logf("whalegen: stopped")
}
