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

## Features and split (Phase 2)
Measured 2026-10-09, Phase 2 commit. `make gold ENV=base`: 88.63 s stage time, 90.85 s wall (4 cutoffs).

| Table (full data, all cutoffs) | Rows |
|---|---:|
| `events_enriched` | 2,755,641 |
| `user_features` | 4,316,835 |
| `user_category_affinity` | 1,338,315 |
| `item_features` | 1,838,940 |
| `labels` | 843,105 |

Label windows (14 days each):

| split | cutoff | label pairs | label visitors | visitors with any history | pairs seen before T | purchased pairs |
|---|---|---:|---:|---:|---:|---:|
| train | 2015-07-24 | 248,024 | 172,049 | 8.4% | 1.8% | 2,408 |
| train | 2015-08-07 | 198,318 | 142,829 | 9.4% | 2.1% | 1,972 |
| val | 2015-08-21 | 196,586 | 141,559 | 10.1% | 2.2% | 1,969 |
| test | 2015-09-04 | 200,177 | 142,838 | 10.4% | 2.1% | 1,842 |

**About 90% of the visitors we'd evaluate on have no history before the cutoff.** Personalized candidate generators can only help the ~10% warm segment. The cold majority gets popularity- and context-based recommendations. Phase 3 reports warm and cold separately.

Event category coverage (point-in-time): 90.73% with first-version backfill, 76.16% without (ADR-005).

Sample (5% of visitors): 25.77 s stage, 28.68 s wall. 41,662 label rows, 9.0–9.7% warm visitors per cutoff.

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
