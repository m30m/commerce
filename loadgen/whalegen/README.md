# whalegen

A **cohort-shaped** load generator. A small "reseller" cohort (~2% of users)
quietly accumulates 1000-product carts inside otherwise ordinary consumer traffic.

Nothing about the traffic *shape* is abusive: the rate is flat, the mix is
normal, every response is a 200. Only a handful of user ids are expensive — and
they are expensive on every request they make.

Unlike [`burstgen`](../burstgen) (saturate the pool, cascade 5xx) and
[`poisongen`](../poisongen) (amplify one pathological row into every response),
whalegen breaks nothing. It is built to test **attribution**: can you find *which
users* are behind a latency tail, when no metric label carries a user dimension?

> Requires `CART_PAGE_LIMIT=1000` on the cart service (the default). At the old
> 100-item ceiling this scenario **does not reproduce** — see
> [Why cart size is the whole mechanism](#why-cart-size-is-the-whole-mechanism).
> All numbers below are measured, not aspirational.

## The chain

```
whale GET /carts/{uid}   ->  1000 rows (vs ~6 for a consumer)
                         ->  /products/batch with ~1000 ids
                         ->  1000-line cart payload (84.7 KiB vs 2.9 KiB /home)
                         ->  embedded in /home, serialised again
```

Whales are ~2% of the id space and are sampled like everyone else, so they are
~2% of requests. The intent is that p50/p95 never move (that is consumer traffic)
while the top ~2% of the distribution — p99 — is entirely whale traffic. The RED
metrics carry no user dimension (`http_request_duration_seconds` is labelled by
method and route template only), so the dashboards can show *that* p99 diverged
but contain nothing that explains *why*.

The uid survives in exactly one place: the raw `path` field of the structured
access log (`/home/417`). That field is what makes the scenario solvable at all —
before it existed the access log recorded only the route template (`/home/{uid}`)
and the uid appeared nowhere in the system. See the logging section of the
[root README](../../README.md#logging).

## Build & run

```sh
cd whalegen
go build -o whalegen .

# default: 50 rps, 2% of 500 users accumulate 1000-product carts after 10s
./whalegen -target http://localhost:8000

# reproducible cohort, bounded run
./whalegen -target http://localhost:8000 -seed 42 -duration 10m

# put the cohort's carts back to normal afterwards
./whalegen -target http://localhost:8000 -seed 42 -drain
```

`-drain` uses `DELETE /carts/{uid}/items/{product_id}` to empty the cohort's
carts and exit. Pass the **same `-seed`** you ran with, or it will drain a
different cohort. (Postgres in `k8s/` uses `emptyDir`, so restarting the pod also
resets everything.)

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-target` | (required) | Base URL, e.g. `http://localhost:8000`. |
| `-rps` | `50` | Steady baseline request rate (requests/second). |
| `-write-ratio` | `0.10` | Fraction of baseline requests that are writes (rest are reads). |
| `-users` | `500` | User id key space; ids sampled uniformly in `[1,users]`. Must exist — `cart_items` has an FK onto `users`. |
| `-products` | `1000` | Product id key space; sampled in `[1,products]`. Must exist — FK onto `products`. |
| `-whale-ratio` | `0.02` | Fraction of users in the reseller cohort. |
| `-cart-size` | `1000` | Distinct products per whale cart. Capped in practice by the cart service's `CART_PAGE_LIMIT`; must be `<= -products`. |
| `-whale-qty` | `12` | Max qty per whale line item; resellers buy in bulk (affects payload, not row count). |
| `-fill-delay` | `10s` | Delay before the cohort starts accumulating, so a clean baseline lands on the dashboards first. |
| `-fill-rps` | `200` | Rate of cohort cart-adds. A full cohort is ~10k adds (~50s at 200/s); lower = a more gradual p99 ramp. |
| `-detect-ratio` | `5.0` | Report divergence once whale p99 reaches this multiple of the consumer p50. |
| `-drain` | `false` | Teardown mode: empty the cohort's carts and exit. |
| `-max-inflight` | `256` | Cap on concurrent in-flight requests; ticks drop when exceeded. |
| `-duration` | `0` | Total run time; `0` = until Ctrl-C. |
| `-timeout` | `15s` | Per-request client timeout. |
| `-report` | `10s` | Interval between cohort-split status lines. |
| `-seed` | `0` | RNG seed; `0` = time-based. A fixed seed reproduces the same cohort. |

## Reading the output

The cohort is printed up front — it is the answer key:

```
cohort: 10 resellers (2.0% of users) accumulating 1000-product carts after 10s at 200 adds/s
cohort: uids = [6 18 69 95 100 167 292 313 323 460]
```

Then a status block per interval. The generator knows which ids are whales, so it
can print the split the target cannot:

```
[t=40s] reads=1800 writes=199 | 2xx=11942 4xx=0 5xx=0 err=0 dropped=0 | cohort loaded (10/10 resellers at >=1000 items)
        blended  p50=6ms  p95=13ms  p99=37ms  (n=1799)  p99/p50 = 6.6x  <- the dashboard's whole story
        consumer p50=6ms  p95=11ms  p99=34ms  (n=1752)
        whale    p50=15ms p95=33ms  p99=45ms  (n=47)  p99 = 8.1x consumer p50 (baseline), 1.3x consumer p99
DETECTED: reseller p99 is 8.1x the consumer baseline (45ms vs p50 6ms) while blended p50 sits flat at 6ms —
          p99 has left p50 behind, and no metric label says which users are responsible
```

Read it as three claims. `blended` is the dashboard's entire story: p99 has left
p50 by 6.6× and p50 is flat — an anomaly with no attribution attached. `consumer`
confirms the flat half is real. `whale` is the part no dashboard can produce.

The two whale ratios answer different questions:

- **vs consumer p50** — how much slower a reseller's slow request is than the
  baseline a normal user gets. The scenario's headline number, and what
  `-detect-ratio` fires on.
- **vs consumer p99** — whether the cohort is separated from the consumer tail at
  all. **Below ~1 the cohort is hiding inside ordinary noise and the blended p99
  is not theirs to own** — which is exactly what happened at the old 100-item
  ceiling. At 1000 it sits at 1.3–1.8×: the cohort is above the tail, but not so
  far above that the *individual* slowest requests are all whales (see below).

## Why cart size is the whole mechanism

Measured against the four services on loopback (single uvicorn worker each,
Postgres + Redis local, `CACHE_TTL=60`, 50 rps, 2% cohort):

| `/home/{uid}` | Consumer | Whale @ 100 items | Whale @ 1000 items |
|---|---|---|---|
| median | 6.7 ms | 9.4 ms (**1.2×**) | 23.9 ms (**3.6×**) |
| p99 | 28.8 ms | 24.9 ms | 65.6 ms |
| p99 vs consumer **p50** | — | 3.7× | **9.8×** |
| payload | 2.9 KiB | 10.6 KiB | 84.7 KiB |

**At 100 items the scenario does not reproduce.** The marginal cost of a cart
item is only **~19 µs** warm (a 100-id `/products/batch` is 3.4 ms vs 1.6 ms for
6 ids), so 100 items adds ~1.8 ms to a ~5 ms request — a 100-item cart is only
~25% more expensive than a 6-item one. That never rises above ambient noise, and
ranking uids by latency finds nothing:

```
rank  uid    median_ms  whale?          <- at CART_PAGE_LIMIT=100
1     470    13.2
2     69     11.3       ** WHALE **
3     100    10.9       ** WHALE **
4     495    10.7                       <- consumers interleaved with whales
```

**At 1000 items it separates cleanly.** Same analysis, same cohort:

```
rank  uid    median_ms  whale?          <- at CART_PAGE_LIMIT=1000
1     323    27.4       ** WHALE **
...   (ranks 1-10 are all ten whales, 16.8-27.4 ms)
10    6      16.8       ** WHALE **
11    238    9.6                        <- clean gap; no overlap
12    206    9.2
```

Whale #10 sits at 16.8 ms against 9.6 ms for the fastest consumer — the cohort is
the top 10 with nothing interleaved. That is the property the scenario needs, and
it is why `CART_PAGE_LIMIT` is the load-bearing setting: per-item cost is small
enough that it takes ~1000 items to clear the fixed ~5 ms cost of a request.

Two things notably do **not** drive it:

- **The database.** A whale's cart query sorts its rows in **0.09 ms**
  (`Sort` → `Limit`, `Buffers: shared hit=17`). It is not the DB.
- **Load.** At 250 rps the whole stack saturates and p50 goes to 811 ms *for
  everyone* — that is [`burstgen`](../burstgen)'s scenario, and it erases the
  cohort signal entirely (whale ratio falls to 0.8×).

### The tail is not fully the cohort's

Whale p99 (65.6 ms) is only **1.8×** the consumer p99 (28.8 ms), because the
consumer tail carries its own noise. Of the slowest 1% of individual requests,
only 24% are whales. So the cohort owns the p99 *in aggregate by uid* — which is
what the LogQL below asks — but the single slowest requests are still a mix. Rank
uids by their **median**, not by the extremes.

### The cache-miss path amplifies it further

The one term that scales even harder than linearly with cart size is the
cache-miss path in `product.get_products_batch`, which re-caches each row in a
**sequential `await` loop** — one Redis round trip per product:

| `/products/batch` | 6 ids | 100 ids |
|---|---|---|
| warm | 1.6 ms | 3.4 ms |
| cold (miss) | 3.0 ms | **16.5 ms** |

At 1000 ids cold that loop is ~1000 round trips. It only fires when the whale's
keys are cold: per-user inter-arrival is `users / rps` (independent of
`-whale-ratio`), which at 500 users and 50 rps is 10 s — inside a 60 s
`CACHE_TTL`, so whale sets normally stay warm and the numbers above are the warm
path. Do not lower `CACHE_TTL` to force it: that fattens the *consumer* tail too
(consumer p50 went 8 ms → 22 ms at `CACHE_TTL=5`) and destroys the "p50 flat"
half of the signature.

### Caveats

- **A 1000-product cart is the entire seed catalog.** `db/init.sql` seeds exactly
  1000 products, so `-cart-size 1000` means every whale holds every product. It
  measures fine but is not realistic — seed a larger catalog if that matters, and
  note `-cart-size` cannot exceed `-products`.
- **The cohort adds ~10k rows to `cart_items`** (3,000 → 13,548, a 4.5×
  table). That inflates the recommendation decoy below; it stayed a decoy here
  (rec still 3–6 ms, consumer p50 flat at 7 ms) but it is worth re-checking if you
  raise `-whale-ratio` or `-cart-size`.
- **The batch URL carries ~1000 ids (~4.9 KiB of query string).** Fine for the
  internal cart→product hop, but it is within range of default proxy header
  limits (nginx `large_client_header_buffers` is 8k) if that path ever moves
  behind an ingress.

The generator reports the cohort split every interval so you can confirm the
scenario is landing on *your* stack rather than assuming it.

## Decoys

Both arise on their own from the mechanism — nothing synthetic is injected:

- **"recommendation is slow"** — it moves a little, for *everyone*. The cohort
  adds ~1000 rows to `cart_items` (a ~33% increase over the ~3000-row seed), and
  `recommendation` aggregates the whole table
  (`SELECT product_id, COUNT(*) ... GROUP BY product_id`) on every request. So a
  real, fleet-wide regression appears, correlated in time with the incident, and
  it is not the cause.
- **"GC / memory"** — whale responses are ~11× larger (8.3 KiB vs 762 B carts),
  so `process_resident_memory_bytes` and the GC counters on `cart` / `gateway`
  drift up as the cohort loads. Also real, also an effect rather than a cause.

## Grading

Finding "p99 is high" is not the answer. The answer is **the cohort and the
mechanism**: a specific ~2% set of user ids whose carts have reached the cart
service's `CART_PAGE_LIMIT` page ceiling, making every one of their `/home` requests cost
more. The uid list printed at startup and in the summary is the key.

The intended path — aggregate `duration_ms` by uid from Loki, because no metric
can:

```logql
# rank users by typical /home latency; the cohort should surface together
topk(20,
  avg by (path) (
    avg_over_time(
      {service="gateway"} | json | path =~ "/home/[0-9]+" | unwrap duration_ms [5m]
    )
  )
)

# same question at the cart service, where the cost actually is
topk(20,
  max by (path) (
    max_over_time(
      {service="cart"} | json | path =~ "/carts/[0-9]+" | unwrap duration_ms [5m]
    )
  )
)
```

Then confirm the mechanism rather than stopping at the correlation: fetch one
suspect id's cart and count the line items (1000, against a handful for anyone
else).

### Give the logs time to accumulate

This is the one operational trap. The stack ships with `ACCESS_LOG_SAMPLE_N=20`
(`.env` and `k8s/01-config.yaml`) — only 1 in 20 successful requests is logged,
to keep `json.dumps` and a blocking stdout write off the event loop. The sampling
is unbiased, so per-uid medians stay correct, but they get 20× thinner. Log lines
per uid work out as:

```
lines/uid/min = 60 * rps / users / ACCESS_LOG_SAMPLE_N
              = 60 * 50 / 500 / 20  =  0.3      -> ~30 min for ~10 samples/uid
```

So at stock settings plan a **30+ minute run**, or set `ACCESS_LOG_SAMPLE_N` to
1–5 for the scenario and note that you have moved a knob. A short run at
`N=20` gives one or two samples per uid, which is not enough to rank uids by
median — the cohort is there, but the logs cannot show it yet. (The measurements
in this README were taken at `N=1`, every request logged.)

> Point this only at infrastructure you own or are authorised to test.
