# Results

Measured numbers only. Each entry records the date, git commit, dataset (sample/full), and hardware.

Hardware for all runs unless noted: Apple M4 (10 cores), 16 GB RAM, Spark `local[*]`, driver memory 4g (full) / 2g (sample).

## Ingest and clean (Phase 1)
Measured 2026-10-09, Phase 1 commit. Wall time is `make data` including JVM startup. Stage times come from `data/*/_reports/*.json`.

| | Full | Sample (5% of visitors) |
|---|---:|---:|
| Raw events | 2,756,101 | 138,381 |
| Silver events | 2,755,641 | 138,352 |
| Exact duplicates dropped | 460 | 29 |
| Rejected rows | 0 | 0 |
| Raw item-property rows | 20,275,902 | 2,960,817 |
| `item_properties_scd` rows (after collapsing unchanged snapshots) | 12,933,522 | 1,642,862 |
| Items in `catalog_latest` | 417,053 | 47,303 |
| Event items with no catalog entry | 49,815 | 7,571 |
| Items whose category isn't in the tree (warn) | 132 | — |
| raw → bronze stage | 24.98 s | 7.03 s |
| silver events stage | 10.72 s | 5.28 s |
| silver catalog stage | 26.95 s | 7.13 s |
| `make data` wall time | 68.45 s | — |

Data facts that drive later phases (full data): 1,407,580 visitors and 235,061 items with events. 71.2% of visitors have exactly one event (median 1, p90 3, p99 13). 23,352 items change category over time. 137,179 events come before the first property snapshot (ADR-005).

## Candidate generation (Phase 3)
_Not yet measured._

## Vector retrieval (Phase 4)
_Not yet measured._

## Ranking (Phase 5)
_Not yet measured._

## Offline evaluation (Phase 6)
_Not yet measured._

## Serving load test (Phase 7)
_Not yet measured._

## Pipeline performance (Phase 9)
_Not yet measured._
