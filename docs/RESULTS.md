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

## Embeddings and vector search (Phase 4)
Measured 2026-10-09, Phase 4 commit, full data. Qdrant v1.19.2 runs in Docker (OrbStack) on the same M4. qdrant-client 1.19.1 talks to it over HTTP on localhost.

### item2vec training (`make embeddings ENV=base`, 109 s wall for 4 cutoffs)
gensim 4.4.0 skip-gram with negative sampling: dim 64, window 5, negative 10, min_count 3, 10 epochs, single-threaded and deterministic. Trained per cutoff on sessions before T that have ≥ 2 distinct items. Neighbor same-category@10 = share of an item's 10 nearest neighbors (exact) that share its category at T, over 2,000 sampled items.

| cutoff | sessions | tokens | vocab (items) | train s | neighbor same-category@10 |
|---|---:|---:|---:|---:|---:|
| 2015-07-24 | 164,658 | 624,396 | 41,588 | 13.49 | 46.1% |
| 2015-08-07 | 195,104 | 733,787 | 46,783 | 16.54 | 49.1% |
| 2015-08-21 | 217,407 | 813,720 | 50,056 | 20.75 | 50.8% |
| 2015-09-04 | 240,229 | 893,655 | 53,328 | 21.74 | 50.7% |

Comparison at the validation cutoff, on 2,000 items that have both kinds of neighbors: item2vec neighbors share the category 62.0% of the time, co-occurrence neighbors 79.5%. Co-occurrence neighbors are sparser, though: 8,181 neighbors with a category vs 19,832 for item2vec for the same items, and 26,788 items have co-occurrence neighbors vs 50,056 with an embedding.

### Qdrant: approximate vs exact search (`make vectors-load` + `make vectors-bench`, ENV=base)
Collection `items_20150821`: 50,056 points, cosine, HNSW m=16, ef_construct=100, full_scan_threshold=10 KB. Upload took 2.35 s, plus 2.01 s until fully indexed. Queries use k = 10, the query item or the user's seed items are excluded, and hnsw_ef = 128. Latency is client-side wall time for one query over HTTP to localhost. `batch_qps` uses `query_batch_points` with 64 per request. `exact_numpy_qps` is in-process brute force on the same queries, which is the offline pipeline's index.

| scenario | queries | recall@10 vs exact | p50 ms | p95 ms | p99 ms | sequential QPS | batch QPS | exact NumPy QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| similar_items | 1000 | 0.9964 | 1.724 | 4.774 | 5.869 | 450.0 | 3713.1 | 2215.7 |
| similar_items_available_only | 1000 | 0.9838 | 1.369 | 2.442 | 3.22 | 656.6 | 3533.5 | 5629.0 |
| user_vector | 1000 | 0.9986 | 1.554 | 3.687 | 5.203 | 518.8 | 3693.3 | 2383.3 |

`hnsw_ef` trade-off (ef below k = 10 behaves like ef = 10):

| hnsw_ef | similar-items recall@10 | p50 ms | p99 ms | user-vector recall@10 | p50 ms | p99 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 0.9364 | 1.683 | 5.511 | 0.9533 | 1.325 | 3.045 |
| 8 | 0.9364 | 1.783 | 6.315 | 0.9533 | 1.7 | 5.73 |
| 16 | 0.9614 | 1.46 | 7.976 | 0.9729 | 1.601 | 4.616 |
| 32 | 0.9815 | 1.408 | 5.241 | 0.9885 | 2.181 | 6.146 |
| 64 | 0.9918 | 1.595 | 4.664 | 0.9955 | 2.305 | 6.769 |
| 128 | 0.9964 | 1.507 | 6.548 | 0.9986 | 2.853 | 6.91 |

**First run was invalid, kept here for the record.** With Qdrant's default `full_scan_threshold` (10,000 KB), the collection's 5 segments (about 2.5 MB each) were all below the threshold, so the planner brute-forced every query. The run reported recall = 1.0 at every ef (p50 1.884 ms similar-items, 1.634 ms user-vector), which measures an exact scan, not HNSW. The threshold is now configurable (`vector_search.hnsw_full_scan_threshold_kb`). `tests/test_qdrant.py::test_low_ef_hnsw_is_approximate` fails if a tiny ef ever returns perfect recall again. At this collection size, latency is dominated by the HTTP round trip, so ef barely moves p50.

### item2vec as a candidate source (validation, `make candidates ENV=base SOURCES=item2vec`, 245.88 s wall)
User vector = decayed weighted mean of the visitor's top 20 recent items' vectors. Seeds are excluded, and ranking uses exact search. Rows per cutoff: 1,065,400, 1,005,600, 1,077,700, 1,133,100.

Warm visitors, relevance ≥ 1:

| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 |
|---|---:|---:|---:|---:|---:|---:|
| `recent_items` | 100.0% | 0.1924 | 0.1935 | 0.1941 | 0.1943 | 0.1869 |
| `popular_category` | 91.4% | 0.0815 | 0.1098 | 0.1556 | 0.1876 | 0.0515 |
| `als` | 42.4% | 0.0527 | 0.0663 | 0.0868 | 0.1022 | 0.0367 |
| `item2vec` | 75.7% | 0.0135 | 0.0190 | 0.0323 | 0.0434 | 0.0085 |
| `cooccurrence` | 61.7% | 0.0195 | 0.0245 | 0.0306 | 0.0322 | 0.0136 |
| `popular_global` | 100.0% | 0.0079 | 0.0128 | 0.0205 | 0.0316 | 0.0049 |
| union of all sources | 100.0% | 0.2392 | 0.2565 | 0.2838 | 0.3067 | — |

Warm visitors, relevance ≥ 2:

| source | coverage | R@10 | R@20 | R@50 | R@100 | NDCG@10 |
|---|---:|---:|---:|---:|---:|---:|
| `recent_items` | 100.0% | 0.2466 | 0.2535 | 0.2571 | 0.2572 | 0.2243 |
| `popular_category` | 91.4% | 0.1479 | 0.1968 | 0.2373 | 0.2636 | 0.1031 |
| `als` | 42.4% | 0.1233 | 0.1528 | 0.1773 | 0.2025 | 0.0949 |
| `item2vec` | 75.7% | 0.0120 | 0.0160 | 0.0235 | 0.0332 | 0.0076 |
| `cooccurrence` | 61.7% | 0.0176 | 0.0274 | 0.0438 | 0.0551 | 0.0147 |
| `popular_global` | 100.0% | 0.0259 | 0.0531 | 0.0729 | 0.0956 | 0.0155 |
| union of all sources | 100.0% | 0.3030 | 0.3440 | 0.3767 | 0.4122 | — |

### Ablation: union recall lost when one source is removed (validation, relevance ≥ 1)
| removed source | warm R@20 drop | warm R@100 drop | all R@100 drop |
|---|---:|---:|---:|
| `recent_items` | 0.0910 | 0.0501 | 0.0050 |
| `popular_category` | 0.0132 | 0.0236 | 0.0024 |
| `popular_global` | 0.0071 | 0.0160 | 0.0317 |
| `item2vec` | 0.0070 | 0.0114 | 0.0011 |
| `als` | 0.0046 | 0.0084 | 0.0008 |
| `cooccurrence` | 0.0064 | 0.0029 | 0.0003 |

Warm union recall@100 is 0.3067 with item2vec and 0.2953 without it.

Takeaways:
- On its own, item2vec is a mid-strength source for warm visitors (R@100 0.043): better than session co-occurrence (0.032), well below ALS (0.102).
- Its *marginal* contribution is the third largest (warm R@100 −0.0114 when removed), more than ALS (−0.0084). It finds items the other sources miss, and it covers 75.7% of warm visitors vs 42.4% for ALS.
- For carts and purchases it's weak (R@100 0.033). ALS and category popularity carry that segment.

## Ranking (Phase 5)
Measured 2026-10-09, Phase 5 commit, full data. `make ranking ENV=base`: 755.82 s stage (training data build + fit + scoring val/test), 760.34 s wall. Free disk never dropped below 9 GB. The evaluation part re-runs with `make ranking-eval ENV=base` (374.2 s).

**Protocol.**
- Fit on the two train cutoffs. Early-stop on validation queries.
- Test (cutoff 2015-09-04) was scored and evaluated once, with hyperparameters as configured. Nothing was tuned on it: no LightGBM sweep was run, and the parameters are the initial config.
- Training queries are label-window visitors with ≥ 1 positive candidate (queries without positives give LambdaRank no gradient). Each query keeps all positives plus ≤ 100 hash-sampled negatives.
- Validation and test are scored over **every** candidate of **every** label-window visitor. Nothing at evaluation time is filtered by labels.

**Model.** LightGBM 4.7.0 LambdaRank with 65 features (per-source rank/score, user features at T, item features at T, the user's own history with the item, user × category affinity).
- Training: 2,053,735 rows, 20,383 queries, 26,890 positive rows. Validation: 1,033,994 rows, 10,263 queries.
- Best iteration 16 (early stopping, patience 100, lr 0.05). Fit took 15.11 s.
- Validation NDCG@10 on the sampled queries: 0.3578 after 1 tree, 0.4389 at best.

### Headline: test NDCG@10, any interaction (relevance ≥ 1), with validation for comparison
| segment (test visitors) | ranker | blend | recent_items | popular_global | ALS | val ranker | val blend |
|---|---:|---:|---:|---:|---:|---:|---:|
| all (142,838) | **0.0229** | 0.0229 | 0.0175 | 0.0051 | 0.0036 | 0.0275 | 0.0264 |
| warm (14,839) | **0.1805** | 0.1765 | 0.1688 | 0.0046 | 0.0344 | 0.1986 | 0.1946 |
| cold (127,999) | **0.0046** | 0.0051 | 0.0000 | 0.0051 | 0.0000 | 0.0083 | 0.0076 |

### Test, all label visitors, relevance ≥ 1 (142,838 visitors)
| model | NDCG@10 | R@10 | R@20 | R@100 | MAP@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|
| **LightGBM ranker** | 0.0229 | 0.0290 | 0.0360 | 0.0577 | 0.0199 | 0.0346 |
| blend (recent → category → global) | 0.0229 | 0.0276 | 0.0316 | 0.0560 | 0.0203 | 0.0329 |
| `recent_items` | 0.0175 | 0.0180 | 0.0182 | 0.0182 | 0.0165 | 0.0215 |
| `popular_category` | 0.0051 | 0.0079 | 0.0105 | 0.0178 | 0.0038 | 0.0100 |
| `als` | 0.0036 | 0.0050 | 0.0062 | 0.0095 | 0.0028 | 0.0068 |
| `item2vec` | 0.0009 | 0.0014 | 0.0020 | 0.0041 | 0.0006 | 0.0023 |
| `cooccurrence` | 0.0013 | 0.0019 | 0.0024 | 0.0030 | 0.0009 | 0.0034 |
| `popular_global` | 0.0051 | 0.0086 | 0.0119 | 0.0353 | 0.0036 | 0.0109 |

### Test, warm visitors, relevance ≥ 1 (14,839 visitors)
| model | NDCG@10 | R@10 | R@20 | R@100 | MAP@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|
| **LightGBM ranker** | 0.1805 | 0.1980 | 0.2110 | 0.2543 | 0.1666 | 0.2321 |
| blend (recent → category → global) | 0.1765 | 0.1900 | 0.2008 | 0.2381 | 0.1638 | 0.2245 |
| `recent_items` | 0.1688 | 0.1737 | 0.1749 | 0.1756 | 0.1590 | 0.2068 |
| `popular_category` | 0.0487 | 0.0756 | 0.1008 | 0.1713 | 0.0369 | 0.0958 |
| `als` | 0.0344 | 0.0477 | 0.0600 | 0.0915 | 0.0272 | 0.0652 |
| `item2vec` | 0.0083 | 0.0132 | 0.0193 | 0.0395 | 0.0055 | 0.0221 |
| `cooccurrence` | 0.0129 | 0.0183 | 0.0227 | 0.0287 | 0.0090 | 0.0329 |
| `popular_global` | 0.0046 | 0.0076 | 0.0116 | 0.0392 | 0.0032 | 0.0122 |

### Test, cold visitors, relevance ≥ 1 (127,999 visitors)
| model | NDCG@10 | R@10 | R@20 | R@100 | MAP@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|
| **LightGBM ranker** | 0.0046 | 0.0094 | 0.0158 | 0.0349 | 0.0029 | 0.0117 |
| blend (recent → category → global) | 0.0051 | 0.0087 | 0.0119 | 0.0349 | 0.0037 | 0.0107 |
| `recent_items` | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| `popular_category` | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| `als` | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| `item2vec` | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| `cooccurrence` | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| `popular_global` | 0.0051 | 0.0087 | 0.0119 | 0.0349 | 0.0037 | 0.0107 |

### Test, warm visitors, add-to-cart or purchase (relevance ≥ 2, 524 visitors)
| model | NDCG@10 | R@10 | R@20 | R@100 | MAP@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|
| **LightGBM ranker** | 0.1640 | 0.1941 | 0.2148 | 0.2921 | 0.1418 | 0.2462 |
| blend (recent → category → global) | 0.1593 | 0.1831 | 0.2042 | 0.2587 | 0.1392 | 0.2366 |
| `recent_items` | 0.1513 | 0.1630 | 0.1724 | 0.1764 | 0.1346 | 0.2176 |
| `popular_category` | 0.0603 | 0.0970 | 0.1280 | 0.2155 | 0.0433 | 0.1260 |
| `als` | 0.0596 | 0.0858 | 0.1071 | 0.1656 | 0.0446 | 0.1221 |
| `item2vec` | 0.0110 | 0.0164 | 0.0221 | 0.0494 | 0.0080 | 0.0267 |
| `cooccurrence` | 0.0193 | 0.0338 | 0.0390 | 0.0633 | 0.0120 | 0.0553 |
| `popular_global` | 0.0082 | 0.0150 | 0.0228 | 0.0593 | 0.0040 | 0.0344 |

### Validation, warm visitors, relevance ≥ 1 (14,237 visitors)
| model | NDCG@10 | R@10 | R@20 | R@100 | MAP@10 | HR@10 |
|---|---:|---:|---:|---:|---:|---:|
| **LightGBM ranker** | 0.1986 | 0.2176 | 0.2339 | 0.2800 | 0.1834 | 0.2560 |
| blend (recent → category → global) | 0.1946 | 0.2095 | 0.2218 | 0.2611 | 0.1807 | 0.2477 |
| `recent_items` | 0.1869 | 0.1924 | 0.1935 | 0.1943 | 0.1762 | 0.2284 |
| `popular_category` | 0.0515 | 0.0815 | 0.1098 | 0.1876 | 0.0386 | 0.1029 |
| `als` | 0.0367 | 0.0527 | 0.0663 | 0.1022 | 0.0280 | 0.0733 |
| `item2vec` | 0.0085 | 0.0135 | 0.0190 | 0.0434 | 0.0056 | 0.0236 |
| `cooccurrence` | 0.0136 | 0.0195 | 0.0245 | 0.0322 | 0.0096 | 0.0348 |
| `popular_global` | 0.0049 | 0.0079 | 0.0128 | 0.0316 | 0.0036 | 0.0130 |

### Feature importance (gain, best iteration)
| # | feature | gain share | splits |
|---:|---|---:|---:|
| 1 | `src_recent_items_rank` | 39.0% | 49 |
| 2 | `src_recent_items_score` | 11.5% | 8 |
| 3 | `u_n_categories_30d` | 7.6% | 13 |
| 4 | `src_popular_global_rank` | 4.7% | 66 |
| 5 | `src_popular_global_score` | 4.6% | 26 |
| 6 | `src_n_sources` | 3.9% | 15 |
| 7 | `src_cooccurrence_rank` | 2.9% | 42 |
| 8 | `u_n_distinct_items_7d` | 2.2% | 6 |
| 9 | `i_days_since_first_event` | 2.1% | 92 |
| 10 | `i_purchase_rate_30d` | 1.8% | 22 |
| 11 | `i_n_visitors_7d` | 1.7% | 44 |
| 12 | `i_popularity_rank_7d` | 1.7% | 35 |
| 13 | `i_n_views_7d` | 1.5% | 36 |
| 14 | `u_n_transactions_7d` | 1.3% | 1 |
| 15 | `src_popular_category_rank` | 1.3% | 18 |

**Outcome against the done condition:** on test NDCG@10, the ranker beats popularity (0.0229 vs 0.0051, all visitors) and ALS (0.0036 all / 0.0344 warm) by a wide margin. It also beats the best single source (`recent_items`, 0.0175 all / 0.1688 warm).

**Where it doesn't win, and why:**
- **Against the hand-written blend, the gain is small.** Warm test NDCG@10 is +0.0040 (0.1805 vs 0.1765) and R@100 is +0.016. Across all visitors it's a tie (0.0229 vs 0.0229), and slightly behind on relevance ≥ 2 (0.0318 vs 0.0320). Whether these differences are significant is Phase 6's bootstrap.
- **Cold visitors (90%): the ranker loses on test** (0.0046 vs 0.0051 for popularity order, the blend's cold ranking) even though it wins on validation (0.0083 vs 0.0076). For cold visitors it can only reorder the global-popularity list using item features, and that reordering didn't transfer from August to September.
- **Early stopping at iteration 16** with recent-items rank/score taking about half the gain: the model mostly learns "re-surface the user's own recent items, then use popularity signals", which the blend already encodes.

**Next iterations (not done here, chosen on validation only):** a LightGBM sweep (leaves, min_data_in_leaf, lr) on validation; a cold-visitor policy (popularity order vs ranker) chosen on validation once Phase 6 gives confidence intervals; features that the blend lacks for cold visitors (category trends, item recency); more training cutoffs.

## Offline evaluation (Phase 6)
Measured 2026-10-09, Phase 6 commit, full data, **test** split. `make eval ENV=base`: 118.88 s stage time, 122.61 s wall. It regenerates [EVAL_REPORT.md](EVAL_REPORT.md) and `docs/figures/*.png`. Bootstrap: 2,000 resamples, 95% percentile intervals, seed 2026. Full write-up: [EXPERIMENT.md](EXPERIMENT.md).

| ranker vs | segment | paired NDCG@10 lift (95% CI) | sig. | simulated 50/50 A/B lift (95% CI) | sig. |
|---|---|---|---|---|---|
| blend | all | -0.0% [-0.6%, +0.7%] | no | +2.4% [-3.5%, +8.4%] | no |
| blend | warm | +2.3% [+1.8%, +2.7%] | yes | +5.6% [-1.0%, +12.7%] | no |
| blend | cold | -9.1% [-11.5%, -6.6%] | yes | -6.9% [-17.8%, +4.4%] | no |
| popular_global | all | +353.1% [+329.7%, +377.6%] | yes | +356.7% [+319.3%, +400.1%] | yes |
| popular_global | warm | +3820.7% [+3189.0%, +4609.3%] | yes | +3157.5% [+2512.9%, +4068.6%] | yes |
| popular_global | cold | -9.1% [-11.5%, -6.6%] | yes | -6.9% [-17.8%, +4.4%] | no |
| als | all | +540.8% [+497.8%, +587.3%] | yes | +590.2% [+517.2%, +677.1%] | yes |
| als | warm | +424.5% [+389.5%, +461.2%] | yes | +470.6% [+411.9%, +541.5%] | yes |
| als | cold | n/a [n/a, n/a] | yes | n/a [n/a, n/a] | yes |
| recent_items | all | +30.6% [+28.9%, +32.5%] | yes | +33.4% [+24.7%, +42.3%] | yes |
| recent_items | warm | +6.9% [+6.3%, +7.6%] | yes | +10.3% [+3.2%, +17.8%] | yes |
| recent_items | cold | n/a [n/a, n/a] | yes | n/a [n/a, n/a] | yes |

- **A/B power:** for returning visitors, the simulated A/B difference has SE ≈ 0.0059 NDCG@10 (~7,429 per arm). The minimum detectable effect at 80% power / α = 0.05 is ≈ 9% relative, so the measured +2.3% needs ≈ 17× the sample.
- **Beyond accuracy (top-10):** ranker coverage 6.72% (31,179 items), diversity 0.919, novelty 10.45 bits. For the blend: 6.08%, 0.862, 10.73. item2vec is the most novel source (16.56 bits) and has the widest coverage (7.29%).

![NDCG@10 by policy](figures/ndcg_by_policy.png)
![Ranker vs baselines](figures/lift_forest.png)
![Recall@K](figures/recall_at_k.png)

## Serving load test (Phase 7)
Measured 2026-10-09, Phase 7 commit. The API runs in Docker (`docker compose up`): `recsys-api` image, uvicorn with 4 workers, Redis 8.10.2, Qdrant 1.19.2. Locust 2.46.7 runs on the **same** M4 laptop. 60 s per run, traffic mix 3:1 recommendations : similar, and recommendations split 50/50 between known returning visitors and unknown ids (popularity fallback). IDs come from `make loadtest-ids` (5,000 live users, 5,000 live items).

Snapshot published by `make serve-load ENV=base` in 13.05 s (24.10 s wall): version `2015-09-04-6b5198912f88-20261009T200112`, 14,839 returning visitors with ranker lists (top 50), plus the 50-item popularity fallback and Qdrant collection `items_20150904` (53,328 items, test cutoff).

**Steady rate (latency run):** 20 users, 50–100 ms think time (`RECSYS_LOADTEST_WAIT=0.1 make loadtest LOAD_USERS=20 RUN_NAME=steady`). The client stayed below Locust's CPU warning.

| endpoint | requests | failures | req/s | p50 ms | p95 ms | p99 ms | p99.9 ms | max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| /recommendations/{id} [personalized] | 5,517 | 0 | 93.4 | 4 | 8 | 11 | 25 | 41 |
| /recommendations/{id} [fallback] | 5,404 | 0 | 91.5 | 4 | 8 | 11 | 28 | 40 |
| /similar/{id} | 3,631 | 0 | 61.4 | 8 | 13 | 17 | 33 | 40 |
| Aggregated | 14,552 | 0 | 246.3 | 5 | 11 | 15 | 29 | 41 |

**Saturation (throughput run):** 50 users, ~0 think time (`make loadtest`). Locust warned that **its own CPU went above 90%**, so this is a lower bound on server capacity and an upper bound on latency (client queueing included).

| endpoint | requests | failures | req/s | p50 ms | p95 ms | p99 ms | p99.9 ms | max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| /recommendations/{id} [personalized] | 62,102 | 0 | 1,068.6 | 8 | 18 | 26 | 56 | 105 |
| /recommendations/{id} [fallback] | 61,809 | 0 | 1,063.6 | 9 | 19 | 27 | 59 | 106 |
| /similar/{id} | 41,081 | 0 | 706.9 | 15 | 36 | 52 | 82 | 169 |
| Aggregated | 164,992 | 0 | 2,839.1 | 10 | 26 | 41 | 68 | 169 |

Latency is client-side and includes HTTP over the Docker port forward. Personalized and fallback requests cost the same (one or two Redis GETs). `/similar` adds a Qdrant point lookup plus an HNSW query.

## Pipeline performance (Phase 9)
_Not yet measured._
