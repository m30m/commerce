# burstgen

A standalone, adversarial **burst** load generator. Instead of a steady request
rate, it fires tightly-synchronised bursts of concurrent requests and then idles.
Against a fan-out service backed by a small connection pool (like `eyebench`'s
gateway → product/cart/recommendation, capped at `DB_POOL_MAX=20` with
`DOWNSTREAM_RETRIES=0`), a burst wider than the pool depth saturates the pool,
pushes downstream latency past its 2s timeout, and cascades into `502`s — driving
5xx spikes at a *low* average RPS rather than through sheer volume. Between bursts
the system is allowed to recover, so each burst's damage is attributable.

It has no dependencies beyond the Go standard library and is intentionally
decoupled from the rest of the repo (its own module; not wired into `k8s/`).

## Build & run

```sh
cd burstgen
go build -o burstgen .

# probe gently
./burstgen -target http://localhost:8000 -aggr 0.1 -duration 30s

# turn it up
./burstgen -target http://localhost:8000 -aggr 0.7

# auto-find the burst width where the system tips over
./burstgen -target http://localhost:8000 -ramp
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-target` | (required) | Base URL, e.g. `http://localhost:8000`. |
| `-path` | `/home/%d` | Path template; `%d` is filled with a sampled id. Use a literal path (no `%d`) to hammer one endpoint. |
| `-aggr` | `0.5` | Aggressiveness knob 0.0–1.0; derives burst width and inter-burst gap. |
| `-burst` | `0` | Explicit requests per burst; `0` = derive from `-aggr`. |
| `-gap` | `0` | Explicit delay between bursts (e.g. `500ms`); `0` = derive from `-aggr`. |
| `-users` | `500` | Id key space; each request samples a uniformly-random id in `[1,users]`. |
| `-duration` | `0` | Total run time; `0` = until Ctrl-C. |
| `-timeout` | `10s` | Per-request client timeout. |
| `-ramp` | `false` | Grow burst width each cycle until the 5xx+err rate exceeds `-ramp-threshold`, then hold. |
| `-ramp-threshold` | `0.25` | Fail fraction that ends the ramp. |
| `-seed` | `0` | RNG seed for id sampling; `0` = time-based. |

The `-aggr` knob interpolates burst width `4 → 250` and inter-burst gap
`2s → 100ms`. Explicit `-burst` / `-gap` override it.

## Reading the output

One line per burst: outcome counts (`2xx`/`4xx`/`5xx`/`err`), the burst's fail
rate, latency percentiles, and the cumulative achieved RPS. The signal to watch
is **5xx (or err) climbing while `achieved_rps` stays low** — that's the cascade.
Cross-check against the stack's Grafana: `db_pool_wait_seconds` for `product`
spiking and the gateway 5xx rate climbing in lockstep with the bursts.

> Point this only at infrastructure you own or are authorised to test.
