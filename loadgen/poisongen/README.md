# poisongen

A steady-load generator with an adversarial **poison product** side channel.

Two things run at once against the target:

1. **A steady baseline** — a fixed request rate split **90/10 read/write**. Reads
   hit the aggregating edge endpoint (`GET /home/{uid}`); writes either add a
   random real product to a random user's cart (`POST /carts/{uid}/items`) or
   create an ordinary product (`POST /products`). This is normal, representative
   traffic.

2. **A poison thread** — it creates one pathological product via `POST /products`
   whose `description` is ~1 MiB of text, **logs the id it was assigned**, then
   adds that product to more and more user carts. The recommendation service ranks
   products by cart popularity discounted by price, so a cheap, heavily-carted
   product climbs into the top-ranked set and starts being returned as a
   recommendation. Because the recommendation payload embeds each item's full
   `description`, every `/home/{uid}` response then carries the 1 MiB blob —
   amplifying a trickle of writes into large, memory-hungry read responses across
   the fleet. The thread keeps carting the product forever, so its rank (and the
   amplification) only grows.

A detector polls `/home` and reports the first time the poison appears in
recommendations, along with how large the home response has become.

Because product creation is part of the *normal* baseline (`-create-ratio`), the
poison's own creation is one create among many rather than a lone anomaly.

> Requires the `POST /products` endpoint on the product service (and its gateway
> pass-through). Point this only at infrastructure you own or are authorised to test.

## Build & run

```sh
cd poisongen
go build -o poisongen .

# steady 50 rps, poison after 5s, carted at 20/s
./poisongen -target http://localhost:8000

# heavier baseline, bigger blob, bounded run
./poisongen -target http://localhost:8000 -rps 200 -desc-bytes 2097152 -duration 5m
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-target` | (required) | Base URL, e.g. `http://localhost:8000`. |
| `-rps` | `50` | Steady baseline request rate (requests/second). |
| `-write-ratio` | `0.10` | Fraction of baseline requests that are writes (rest are reads). |
| `-create-ratio` | `0.30` | Fraction of baseline **writes** that create a normal product (rest are cart adds). |
| `-users` | `500` | User id key space; ids sampled uniformly in `[1,users]`. |
| `-products` | `1000` | Real-product id key space for baseline cart adds. |
| `-desc-bytes` | `1048576` | Size of the poison product's description in bytes (1 MiB). |
| `-poison-name` | `!!! FREE GIFT — LIMITED OFFER` | Name for the poison product. |
| `-poison-price` | `0.99` | Poison price; a low price ranks it higher in recommendations. |
| `-poison-category` | `electronics` | Poison category. |
| `-poison-delay` | `5s` | Delay before creating the poison, so the baseline settles first. |
| `-poison-rps` | `20` | Rate at which the poison is added to carts. |
| `-max-inflight` | `256` | Cap on concurrent in-flight requests; ticks drop when exceeded. |
| `-duration` | `0` | Total run time; `0` = until Ctrl-C. |
| `-timeout` | `15s` | Per-request client timeout (generous — poisoned responses are large). |
| `-seed` | `0` | RNG seed; `0` = time-based. |

## Reading the output

The poison creation is logged explicitly, with its assigned id:

```
poison: created poison product id=1004  description=1.0MiB  price=0.99  category="electronics"
DETECTED: poison id=1004 is now returned as a recommendation — /home/468 response is 1.0MiB
```

Then a status line every 5s:

```
[t=10s] reads=361 writes=39 | 2xx=797 4xx=0 5xx=0 err=0 dropped=0 | poison id=1004 cart_adds=398 in_recs=yes home_size=1.0MiB
```

The signal to watch is `home_size` jumping to ~1 MiB once `in_recs=yes`, and 5xx /
err / dropped climbing as the amplified read responses pressure the stack. Cross-
check against Grafana: per-pod RSS on `recommendation` / `gateway`, response-size
and latency on `/home`, and the gateway 5xx rate.
