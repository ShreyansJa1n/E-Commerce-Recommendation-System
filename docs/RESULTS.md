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
Measured 2026-10-09, Phase 3 commit, full data. **Validation cutoff only** (2015-08-21, labels 08-21..09-04). Test is untouched until Phase 5/6. Each source keeps its top 100 per target visitor. Truth = the visitor's label-window items. Metrics are averaged over visitors with at least one relevant item. Coverage = share of segment visitors with at least one candidate from that source. Definitions are in `recsys.eval.metrics`.

`make candidates ENV=base`: 751.66 s stage time, 755.16 s wall, all 4 cutoffs. ALS (rank 128) trains once per cutoff and dominates the runtime. Free disk never dropped below 7 GB (see the disk note under the ALS sweep).

### Warm visitors (any history before T), any interaction (relevance ≥ 1), 14,237 visitors
| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `recent_items` | 100.0% | 0.1924 | 0.1935 | 0.1941 | 0.1943 | 0.1869 | 0.2284 |
| `popular_category` | 91.4% | 0.0815 | 0.1098 | 0.1556 | 0.1876 | 0.0515 | 0.1029 |
| `als` | 42.4% | 0.0527 | 0.0663 | 0.0868 | 0.1022 | 0.0367 | 0.0733 |
| `cooccurrence` | 61.7% | 0.0195 | 0.0245 | 0.0306 | 0.0322 | 0.0136 | 0.0348 |
| `popular_global` | 100.0% | 0.0079 | 0.0128 | 0.0205 | 0.0316 | 0.0049 | 0.0130 |
| union of all sources | 100.0% | 0.2333 | 0.2495 | 0.2737 | 0.2953 | — | — |

### Warm visitors, add-to-cart or purchase (relevance ≥ 2), 482 visitors
| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `recent_items` | 100.0% | 0.2466 | 0.2535 | 0.2571 | 0.2572 | 0.2243 | 0.2905 |
| `popular_category` | 91.4% | 0.1479 | 0.1968 | 0.2373 | 0.2636 | 0.1031 | 0.1805 |
| `als` | 42.4% | 0.1233 | 0.1528 | 0.1773 | 0.2025 | 0.0949 | 0.1618 |
| `cooccurrence` | 61.7% | 0.0176 | 0.0274 | 0.0438 | 0.0551 | 0.0147 | 0.0373 |
| `popular_global` | 100.0% | 0.0259 | 0.0531 | 0.0729 | 0.0956 | 0.0155 | 0.0415 |
| union of all sources | 100.0% | 0.2982 | 0.3372 | 0.3690 | 0.4003 | — | — |

### All label visitors, any interaction, 141,559 visitors (about 90% cold start)
| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `recent_items` | 10.1% | 0.0194 | 0.0195 | 0.0195 | 0.0195 | 0.0188 | 0.0230 |
| `popular_category` | 9.2% | 0.0082 | 0.0110 | 0.0156 | 0.0189 | 0.0052 | 0.0103 |
| `als` | 4.3% | 0.0053 | 0.0067 | 0.0087 | 0.0103 | 0.0037 | 0.0074 |
| `cooccurrence` | 6.2% | 0.0020 | 0.0025 | 0.0031 | 0.0032 | 0.0014 | 0.0035 |
| `popular_global` | 100.0% | 0.0103 | 0.0149 | 0.0234 | 0.0333 | 0.0073 | 0.0127 |
| union of all sources | 100.0% | 0.0330 | 0.0387 | 0.0489 | 0.0598 | — | — |

### All label visitors, add-to-cart or purchase, 3,881 visitors
| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `recent_items` | 10.1% | 0.0306 | 0.0315 | 0.0319 | 0.0319 | 0.0279 | 0.0361 |
| `popular_category` | 9.2% | 0.0184 | 0.0244 | 0.0295 | 0.0327 | 0.0128 | 0.0224 |
| `als` | 4.3% | 0.0153 | 0.0190 | 0.0220 | 0.0252 | 0.0118 | 0.0201 |
| `cooccurrence` | 6.2% | 0.0022 | 0.0034 | 0.0054 | 0.0068 | 0.0018 | 0.0046 |
| `popular_global` | 100.0% | 0.0188 | 0.0388 | 0.0515 | 0.0747 | 0.0097 | 0.0229 |
| union of all sources | 100.0% | 0.0526 | 0.0741 | 0.0883 | 0.1125 | — | — |

Takeaways:
- **Recent items are the strongest single source for returning visitors** (R@10 = 0.192). It saturates almost immediately because it only re-surfaces the visitor's own items.
- **`popular_category` is the best discovery source** (R@100 = 0.188, warm). ALS is second (R@100 = 0.102), and it is much stronger on carts/purchases (R@100 = 0.203, warm, relevance ≥ 2).
- **Session co-occurrence is weak here** (R@100 = 0.032, warm). It excludes the visitor's seed items, which are exactly the repeat interactions, and most item pairs are too sparse to pass `min_pair_sessions = 2` within 60 days. Phase 4's item2vec will be compared against it.
- **The union reaches R@100 = 0.295 for warm visitors vs 0.194 for the best single source.** This is the recall ceiling for the Phase 5 ranker.
- **Cold visitors only get `popular_global`**, so overall recall@100 is 0.060 (union). Most of the remaining opportunity is in the cold segment.

### ALS sweep (validation, warm visitors, relevance ≥ 1)
Round 1 (`make als-sweep ENV=base`, 205.98 s): rank × reg × alpha grid. Its best (64 / 0.01 / 20) was on the grid edge, so round 2 (`make als-sweep ENV=als_sweep_round2`) extended rank and alpha. 798,118 training interactions after filters (≥ 2 items per visitor, ≥ 2 visitors per item). 14,237 target visitors.

| round | rank | reg | alpha | train+recommend s | R@10 | R@100 | NDCG@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 32 | 0.01 | 5 | 22.53 | 0.0217 | 0.0502 | 0.0156 |
| 1 | 32 | 0.01 | 20 | 16.08 | 0.0260 | 0.0619 | 0.0187 |
| 1 | 32 | 0.1 | 5 | 16.3 | 0.0215 | 0.0504 | 0.0156 |
| 1 | 32 | 0.1 | 20 | 15.4 | 0.0261 | 0.0618 | 0.0188 |
| 1 | 64 | 0.01 | 5 | 32.18 | 0.0297 | 0.0650 | 0.0217 |
| 1 | 64 | 0.01 | 20 | 32.32 | 0.0362 | 0.0800 | 0.0258 |
| 1 | 64 | 0.1 | 5 | 35.25 | 0.0297 | 0.0647 | 0.0217 |
| 1 | 64 | 0.1 | 20 | 34.13 | 0.0358 | 0.0795 | 0.0256 |
| 2 | 64 | 0.01 | 20 | 37.2 | 0.0362 | 0.0800 | 0.0258 |
| 2 | 64 | 0.01 | 40 | 32.2 | 0.0393 | 0.0864 | 0.0275 |
| 2 | 128 | 0.01 | 20 | 127.06 | 0.0482 | 0.0948 | 0.0338 |
| 2 | 128 | 0.01 | 40 | 157.13 | 0.0527 | 0.1022 | 0.0367 |

Chosen: **rank 128, reg 0.01, alpha 40** (best R@100). It's still on the grid edge: each rank doubling roughly quadruples training time, and the first round-2 attempt filled the disk at rank 128 (fixed by checkpointing ALS every 5 iterations and running Spark's cleaner every minute). Larger ranks are left for Phase 9.

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
